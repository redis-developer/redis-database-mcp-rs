//! Curated bitmap, geospatial, and HyperLogLog operations.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, ValueEncoding, command, decode_input, destructive_annotations,
    encode_bytes, output_schema, read_annotations, write_annotations,
};
use crate::{AccessMode, RedisValue, RedisVersion};

const MAX_ITEMS: usize = 1_000;
const MAX_REDIS_BIT_OFFSET: u64 = u32::MAX as u64;
const MAX_BITMAP_WRITE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BITMAP_WRITE_BIT_OFFSET: u64 = MAX_BITMAP_WRITE_BYTES * 8 - 1;

fn validate_items(items: &[impl Sized], name: &str) -> tower_mcp::Result<()> {
    if items.is_empty() || items.len() > MAX_ITEMS {
        Err(tower_mcp::Error::tool(format!(
            "{name} must contain between 1 and {MAX_ITEMS} items"
        )))
    } else {
        Ok(())
    }
}

fn require_ok(value: RedisValue, context: &str) -> tower_mcp::Result<()> {
    match value {
        RedisValue::Okay => Ok(()),
        RedisValue::SimpleString(value) if value.eq_ignore_ascii_case("OK") => Ok(()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected reply: {other:?}"
        ))),
    }
}

fn redis_array(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<RedisValue>> {
    match value {
        RedisValue::Array(values) | RedisValue::Set(values) => Ok(values),
        RedisValue::Attribute { data, .. } => redis_array(*data, context),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected reply: {other:?}"
        ))),
    }
}

fn redis_decimal(value: RedisValue, context: &str) -> tower_mcp::Result<String> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => String::from_utf8(value)
            .map_err(|_| {
                tower_mcp::Error::tool(format!("{context} returned non-UTF-8 numeric data"))
            }),
        RedisValue::SimpleString(value) => Ok(value),
        RedisValue::Integer(value) => Ok(value.to_string()),
        RedisValue::Double(value) if value.is_finite() => Ok(value.to_string()),
        RedisValue::Attribute { data, .. } => redis_decimal(*data, context),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected numeric reply: {other:?}"
        ))),
    }
}

fn redis_nonnegative(value: RedisValue, context: &str) -> tower_mcp::Result<u64> {
    match value {
        RedisValue::Integer(value) if value >= 0 => Ok(value as u64),
        RedisValue::Attribute { data, .. } => redis_nonnegative(*data, context),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected non-negative integer reply: {other:?}"
        ))),
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedInput {
    /// Binary-safe value.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    encoding: InputEncoding,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum BinaryInput {
    /// UTF-8 shorthand.
    Utf8(String),
    /// Explicitly encoded value.
    Encoded(EncodedInput),
}

impl BinaryInput {
    fn decode(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        match self {
            Self::Utf8(value) => Ok(value.as_bytes().to_vec()),
            Self::Encoded(value) => decode_input(&value.value, value.encoding, name),
        }
    }

    fn output(&self) -> EncodedOutput {
        match self {
            Self::Utf8(value) => EncodedOutput {
                value: value.clone(),
                encoding: ValueEncoding::Utf8,
            },
            Self::Encoded(value) => EncodedOutput {
                value: value.value.clone(),
                encoding: match value.encoding {
                    InputEncoding::Utf8 => ValueEncoding::Utf8,
                    InputEncoding::Base64 => ValueEncoding::Base64,
                },
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedOutput {
    value: String,
    encoding: ValueEncoding,
}

impl From<Vec<u8>> for EncodedOutput {
    fn from(value: Vec<u8>) -> Self {
        let (value, encoding) = encode_bytes(value);
        Self { value, encoding }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
enum ExactDecimalInput {
    /// JSON number shorthand. Use a string when every decimal digit must be preserved.
    Number(f64),
    /// Exact finite Redis decimal string.
    Exact(String),
}

impl ExactDecimalInput {
    fn finite_token(&self, name: &str) -> tower_mcp::Result<String> {
        let token = match self {
            Self::Number(value) => value.to_string(),
            Self::Exact(value) => value.clone(),
        };
        let parsed = token.parse::<f64>().map_err(|_| {
            tower_mcp::Error::tool(format!("{name} must be a valid finite Redis decimal"))
        })?;
        if !parsed.is_finite() {
            return Err(tower_mcp::Error::tool(format!(
                "{name} must be a finite Redis decimal"
            )));
        }
        Ok(token)
    }

    fn bounded_token(&self, name: &str, min: f64, max: f64) -> tower_mcp::Result<String> {
        let token = self.finite_token(name)?;
        let parsed = token
            .parse::<f64>()
            .expect("finite_token validated the decimal");
        if parsed < min || parsed > max {
            Err(tower_mcp::Error::tool(format!(
                "{name} must be between {min} and {max}"
            )))
        } else {
            Ok(token)
        }
    }

    fn positive_token(&self, name: &str) -> tower_mcp::Result<String> {
        let token = self.finite_token(name)?;
        let parsed = token
            .parse::<f64>()
            .expect("finite_token validated the decimal");
        if parsed <= 0.0 {
            Err(tower_mcp::Error::tool(format!(
                "{name} must be greater than zero"
            )))
        } else {
            Ok(token)
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum ExactIntegerInput {
    /// JSON integer shorthand. Use a string outside a client's exact JSON integer range.
    Number(i64),
    /// Exact signed 64-bit integer string.
    Exact(String),
}

impl ExactIntegerInput {
    fn token(&self, name: &str) -> tower_mcp::Result<String> {
        match self {
            Self::Number(value) => Ok(value.to_string()),
            Self::Exact(value) => value.parse::<i64>().map(|_| value.clone()).map_err(|_| {
                tower_mcp::Error::tool(format!("{name} must be a signed 64-bit integer"))
            }),
        }
    }
}

// Bitmap operations -------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum BitRangeUnit {
    Byte,
    Bit,
}

impl Default for BitRangeUnit {
    fn default() -> Self {
        Self::Byte
    }
}

impl BitRangeUnit {
    fn redis_token(&self) -> &'static str {
        match self {
            Self::Byte => "BYTE",
            Self::Bit => "BIT",
        }
    }
}

fn require_bit_unit_version(state: &ToolState, unit: &BitRangeUnit) -> tower_mcp::Result<()> {
    if matches!(unit, BitRangeUnit::Bit)
        && state
            .redis_version()
            .is_some_and(|version| version < RedisVersion::new(7, 0, 0))
    {
        Err(tower_mcp::Error::tool(
            "BIT range units require Redis 7.0 or newer",
        ))
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetbitInput {
    /// Redis string key containing the bitmap.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Zero-based bit offset. Redis supports offsets through 2^32-1.
    #[schemars(range(max = 4_294_967_295_u64))]
    offset: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetbitOutput {
    key: String,
    key_encoding: InputEncoding,
    offset: u64,
    bit: u8,
    set: bool,
}

fn getbit_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_getbit")
        .title("Read Redis Bitmap Bit")
        .description("Read one bit at an explicit zero-based offset. Missing strings read as zero.")
        .output_schema(output_schema::<GetbitOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GetbitInput>| async move {
                if input.offset > MAX_REDIS_BIT_OFFSET {
                    return Err(tower_mcp::Error::tool(format!(
                        "offset must not exceed {MAX_REDIS_BIT_OFFSET}"
                    )));
                }
                let mut command = command("redis_getbit", AccessMode::ReadOnly, "GETBIT");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.offset.to_string());
                let bit = redis_nonnegative(state.raw(command, "GETBIT failed").await?, "GETBIT")?;
                if bit > 1 {
                    return Err(tower_mcp::Error::tool(format!(
                        "GETBIT returned invalid bit value {bit}"
                    )));
                }
                state.output(&GetbitOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    offset: input.offset,
                    bit: bit as u8,
                    set: bit == 1,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetbitInput {
    /// Redis string key containing the bitmap.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Zero-based bit offset. Writes are limited to a 16 MiB string extent per call.
    #[schemars(range(max = 134_217_727_u64))]
    offset: u64,
    /// New bit value.
    value: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetbitOutput {
    key: String,
    key_encoding: InputEncoding,
    offset: u64,
    value: u8,
    previous: u8,
    previous_set: bool,
    maximum_string_extent_bytes: u64,
}

fn setbit_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_setbit")
        .title("Set Redis Bitmap Bit")
        .description(
            "Set one bit and return its previous value. The offset is capped so one call cannot extend a string beyond 16 MiB.",
        )
        .output_schema(output_schema::<SetbitOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetbitInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_setbit")?;
                if input.offset > MAX_BITMAP_WRITE_BIT_OFFSET {
                    return Err(tower_mcp::Error::tool(format!(
                        "offset must not exceed {MAX_BITMAP_WRITE_BIT_OFFSET}; writes are capped at a {MAX_BITMAP_WRITE_BYTES}-byte string extent"
                    )));
                }
                let mut command = command("redis_setbit", AccessMode::ReadWrite, "SETBIT");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.offset.to_string())
                    .arg(if input.value { "1" } else { "0" });
                let previous =
                    redis_nonnegative(state.raw(command, "SETBIT failed").await?, "SETBIT")?;
                if previous > 1 {
                    return Err(tower_mcp::Error::tool(format!(
                        "SETBIT returned invalid previous bit value {previous}"
                    )));
                }
                state.output(&SetbitOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    offset: input.offset,
                    value: u8::from(input.value),
                    previous: previous as u8,
                    previous_set: previous == 1,
                    maximum_string_extent_bytes: MAX_BITMAP_WRITE_BYTES,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitRange {
    /// Inclusive start, interpreted in `unit`; negative values count from the end.
    start: i64,
    /// Inclusive end, interpreted in `unit`; negative values count from the end.
    end: i64,
    /// Byte ranges work on Redis 2.6+; bit ranges require Redis 7+.
    #[serde(default)]
    unit: BitRangeUnit,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitcountInput {
    /// Redis string key containing the bitmap.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Optional inclusive byte or bit range.
    #[serde(default)]
    range: Option<BitRange>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitcountOutput {
    key: String,
    key_encoding: InputEncoding,
    range: Option<BitRange>,
    set_bits: u64,
}

fn bitcount_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bitcount")
        .title("Count Redis Bitmap Bits")
        .description(
            "Count set bits in a complete Redis string or an explicit inclusive byte/bit range.",
        )
        .output_schema(output_schema::<BitcountOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BitcountInput>| async move {
                let mut command = command("redis_bitcount", AccessMode::ReadOnly, "BITCOUNT");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(range) = &input.range {
                    require_bit_unit_version(&state, &range.unit)?;
                    command
                        .arg(range.start.to_string())
                        .arg(range.end.to_string());
                    if matches!(range.unit, BitRangeUnit::Bit) {
                        command.arg(range.unit.redis_token());
                    }
                }
                let set_bits =
                    redis_nonnegative(state.raw(command, "BITCOUNT failed").await?, "BITCOUNT")?;
                state.output(&BitcountOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    range: input.range,
                    set_bits,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitposRange {
    /// Inclusive start, interpreted in `unit`; negative values count from the end.
    start: i64,
    /// Optional inclusive end. Required when `unit` is `bit`.
    #[serde(default)]
    end: Option<i64>,
    /// Byte ranges work on Redis 2.8.7+; bit ranges require Redis 7+.
    #[serde(default)]
    unit: BitRangeUnit,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitposInput {
    /// Redis string key containing the bitmap.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Bit value to locate.
    bit: bool,
    /// Optional bounded search range.
    #[serde(default)]
    range: Option<BitposRange>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitposOutput {
    key: String,
    key_encoding: InputEncoding,
    bit: u8,
    position: Option<u64>,
}

fn bitpos_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bitpos")
        .title("Locate Redis Bitmap Bit")
        .description("Find the first set or clear bit, optionally within an explicit inclusive byte/bit range.")
        .output_schema(output_schema::<BitposOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BitposInput>| async move {
                let mut command = command("redis_bitpos", AccessMode::ReadOnly, "BITPOS");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(if input.bit { "1" } else { "0" });
                if let Some(range) = &input.range {
                    require_bit_unit_version(&state, &range.unit)?;
                    if matches!(range.unit, BitRangeUnit::Bit) && range.end.is_none() {
                        return Err(tower_mcp::Error::tool(
                            "range.end is required when range.unit is bit",
                        ));
                    }
                    command.arg(range.start.to_string());
                    if let Some(end) = range.end {
                        command.arg(end.to_string());
                        if matches!(range.unit, BitRangeUnit::Bit) {
                            command.arg(range.unit.redis_token());
                        }
                    }
                }
                let response = state.raw(command, "BITPOS failed").await?;
                let position = match response {
                    RedisValue::Integer(-1) => None,
                    RedisValue::Integer(value) if value >= 0 => Some(value as u64),
                    other => {
                        return Err(tower_mcp::Error::tool(format!(
                            "BITPOS returned an unexpected reply: {other:?}"
                        )));
                    }
                };
                state.output(&BitposOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    bit: u8::from(input.bit),
                    position,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitfieldEncoding {
    /// Signed (`i`) when true, unsigned (`u`) when false.
    signed: bool,
    /// Signed widths are 1..=64; unsigned widths are 1..=63.
    #[schemars(range(min = 1, max = 64))]
    width: u8,
}

impl BitfieldEncoding {
    fn token(&self, name: &str) -> tower_mcp::Result<String> {
        if self.width == 0 || self.width > 64 || (!self.signed && self.width > 63) {
            return Err(tower_mcp::Error::tool(format!(
                "{name} must use signed width 1..=64 or unsigned width 1..=63"
            )));
        }
        Ok(format!(
            "{}{}",
            if self.signed { 'i' } else { 'u' },
            self.width
        ))
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BitfieldOffset {
    /// Absolute zero-based bit offset.
    Absolute {
        #[schemars(range(max = 4_294_967_295_u64))]
        value: u64,
    },
    /// Offset equal to `index * encoding.width`, represented with Redis `#` syntax.
    Index {
        #[schemars(range(max = 4_294_967_295_u64))]
        value: u64,
    },
}

impl BitfieldOffset {
    fn token(&self, width: u8, name: &str, write: bool) -> tower_mcp::Result<String> {
        let (token, last_bit) = match self {
            Self::Absolute { value } => (
                value.to_string(),
                value.checked_add(u64::from(width).saturating_sub(1)),
            ),
            Self::Index { value } => {
                let start = value.checked_mul(u64::from(width));
                (
                    format!("#{value}"),
                    start.and_then(|start| start.checked_add(u64::from(width).saturating_sub(1))),
                )
            }
        };
        let last_bit = last_bit.ok_or_else(|| {
            tower_mcp::Error::tool(format!("{name} overflows the Redis bit offset range"))
        })?;
        let maximum = if write {
            MAX_BITMAP_WRITE_BIT_OFFSET
        } else {
            MAX_REDIS_BIT_OFFSET
        };
        if last_bit > maximum {
            let guidance = if write {
                format!("; writes are capped at a {MAX_BITMAP_WRITE_BYTES}-byte string extent")
            } else {
                String::new()
            };
            Err(tower_mcp::Error::tool(format!(
                "{name} addresses bit {last_bit}, above the maximum {maximum}{guidance}"
            )))
        } else {
            Ok(token)
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitfieldGetOperation {
    encoding: BitfieldEncoding,
    offset: BitfieldOffset,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitfieldReadInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Between 1 and 1000 typed GET operations.
    #[schemars(length(min = 1, max = 1000))]
    operations: Vec<BitfieldGetOperation>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitfieldResult {
    index: usize,
    /// Exact signed/unsigned result, or null when overflow mode `fail` rejected a write.
    value: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitfieldOutput {
    key: String,
    key_encoding: InputEncoding,
    count: usize,
    results: Vec<BitfieldResult>,
}

fn decode_bitfield_results(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Vec<BitfieldResult>> {
    redis_array(value, context)?
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let value = match value {
                RedisValue::Nil => None,
                value => Some(redis_decimal(value, context)?),
            };
            Ok(BitfieldResult { index, value })
        })
        .collect()
}

fn bitfield_ro_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bitfield_ro")
        .title("Read Redis Bit Fields")
        .description(
            "Read 1 to 1000 explicitly typed signed or unsigned integer fields. Exact results are decimal strings. Requires Redis 6.0 or newer.",
        )
        .output_schema(output_schema::<BitfieldOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BitfieldReadInput>| async move {
                validate_items(&input.operations, "operations")?;
                state.validate_requested_entries(input.operations.len(), "operations")?;
                let mut command = command("redis_bitfield_ro", AccessMode::ReadOnly, "BITFIELD_RO");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for (index, operation) in input.operations.iter().enumerate() {
                    let encoding = operation
                        .encoding
                        .token(&format!("operations[{index}].encoding"))?;
                    let offset = operation.offset.token(
                        operation.encoding.width,
                        &format!("operations[{index}].offset"),
                        false,
                    )?;
                    command.arg("GET").arg(encoding).arg(offset);
                }
                let results = decode_bitfield_results(
                    state.raw(command, "BITFIELD_RO failed").await?,
                    "BITFIELD_RO",
                )?;
                if results.len() != input.operations.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "BITFIELD_RO returned {} results for {} operations",
                        results.len(),
                        input.operations.len()
                    )));
                }
                state.output_collection(
                    &BitfieldOutput {
                        key: input.key,
                        key_encoding: input.key_encoding,
                        count: results.len(),
                        results,
                    },
                    input.operations.len(),
                    "Retry BITFIELD_RO with fewer operations.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum BitfieldOverflow {
    Wrap,
    Saturate,
    Fail,
}

impl Default for BitfieldOverflow {
    fn default() -> Self {
        Self::Wrap
    }
}

impl BitfieldOverflow {
    fn redis_token(&self) -> &'static str {
        match self {
            Self::Wrap => "WRAP",
            Self::Saturate => "SAT",
            Self::Fail => "FAIL",
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum BitfieldOperation {
    Get {
        encoding: BitfieldEncoding,
        offset: BitfieldOffset,
    },
    Set {
        encoding: BitfieldEncoding,
        offset: BitfieldOffset,
        value: ExactIntegerInput,
    },
    Increment {
        encoding: BitfieldEncoding,
        offset: BitfieldOffset,
        increment: ExactIntegerInput,
        /// Overflow behavior is emitted explicitly before this increment.
        #[serde(default)]
        overflow: BitfieldOverflow,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitfieldInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Between 1 and 1000 typed GET, SET, or INCREMENT operations in wire order.
    #[schemars(length(min = 1, max = 1000))]
    operations: Vec<BitfieldOperation>,
}

fn bitfield_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bitfield")
        .title("Operate on Redis Bit Fields")
        .description(
            "Run 1 to 1000 typed GET, SET, or INCREMENT operations. Writes cannot extend the string beyond 16 MiB, overflow is explicit, and exact results are decimal strings.",
        )
        .output_schema(output_schema::<BitfieldOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BitfieldInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_bitfield")?;
                validate_items(&input.operations, "operations")?;
                state.validate_requested_entries(input.operations.len(), "operations")?;
                let mut command = command("redis_bitfield", AccessMode::ReadWrite, "BITFIELD");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for (index, operation) in input.operations.iter().enumerate() {
                    match operation {
                        BitfieldOperation::Get { encoding, offset } => {
                            command
                                .arg("GET")
                                .arg(encoding.token(&format!("operations[{index}].encoding"))?)
                                .arg(offset.token(
                                    encoding.width,
                                    &format!("operations[{index}].offset"),
                                    false,
                                )?);
                        }
                        BitfieldOperation::Set {
                            encoding,
                            offset,
                            value,
                        } => {
                            command
                                .arg("SET")
                                .arg(encoding.token(&format!("operations[{index}].encoding"))?)
                                .arg(offset.token(
                                    encoding.width,
                                    &format!("operations[{index}].offset"),
                                    true,
                                )?)
                                .arg(value.token(&format!("operations[{index}].value"))?);
                        }
                        BitfieldOperation::Increment {
                            encoding,
                            offset,
                            increment,
                            overflow,
                        } => {
                            command
                                .arg("OVERFLOW")
                                .arg(overflow.redis_token())
                                .arg("INCRBY")
                                .arg(encoding.token(&format!("operations[{index}].encoding"))?)
                                .arg(offset.token(
                                    encoding.width,
                                    &format!("operations[{index}].offset"),
                                    true,
                                )?)
                                .arg(increment.token(&format!("operations[{index}].increment"))?);
                        }
                    }
                }
                let results = decode_bitfield_results(
                    state.raw(command, "BITFIELD failed").await?,
                    "BITFIELD",
                )?;
                if results.len() != input.operations.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "BITFIELD returned {} results for {} operations",
                        results.len(),
                        input.operations.len()
                    )));
                }
                state.output_collection(
                    &BitfieldOutput {
                        key: input.key,
                        key_encoding: input.key_encoding,
                        count: results.len(),
                        results,
                    },
                    input.operations.len(),
                    "Retry BITFIELD with fewer operations.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum BitOperation {
    And,
    Or,
    Xor,
    Not,
}

impl BitOperation {
    fn redis_token(&self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
            Self::Xor => "XOR",
            Self::Not => "NOT",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitopInput {
    /// Destination key overwritten by the result.
    destination: BinaryInput,
    operation: BitOperation,
    /// Source keys. NOT requires exactly one; other operations accept 1 to 1000.
    #[schemars(length(min = 1, max = 1000))]
    sources: Vec<BinaryInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BitopOutput {
    destination: EncodedOutput,
    operation: BitOperation,
    source_count: usize,
    result_length_bytes: u64,
    destination_overwritten: bool,
    cluster_requires_same_slot: bool,
}

fn bitop_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bitop")
        .title("Store Redis Bitmap Operation")
        .description(
            "Permanently overwrite a destination with AND, OR, XOR, or NOT across binary-safe source keys. Every key must share a Redis Cluster slot. The reply is result size, not an effect bound. Requires full access.",
        )
        .output_schema(output_schema::<BitopOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BitopInput>| async move {
                state.require(AccessMode::Full, "redis_bitop")?;
                validate_items(&input.sources, "sources")?;
                state.validate_requested_entries(input.sources.len(), "sources")?;
                if matches!(input.operation, BitOperation::Not) && input.sources.len() != 1 {
                    return Err(tower_mcp::Error::tool(
                        "BITOP NOT requires exactly one source key",
                    ));
                }
                let mut command = command("redis_bitop", AccessMode::Full, "BITOP");
                command
                    .arg(input.operation.redis_token())
                    .arg(input.destination.decode("destination")?);
                for (index, source) in input.sources.iter().enumerate() {
                    command.arg(source.decode(&format!("sources[{index}]"))?);
                }
                let result_length_bytes =
                    redis_nonnegative(state.raw(command, "BITOP failed").await?, "BITOP")?;
                state.output(&BitopOutput {
                    destination: input.destination.output(),
                    operation: input.operation,
                    source_count: input.sources.len(),
                    result_length_bytes,
                    destination_overwritten: true,
                    cluster_requires_same_slot: true,
                })
            },
        )
        .build()
}

// Geospatial operations ---------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum GeoUnit {
    Meters,
    Kilometers,
    Feet,
    Miles,
}

impl Default for GeoUnit {
    fn default() -> Self {
        Self::Meters
    }
}

impl GeoUnit {
    fn redis_token(&self) -> &'static str {
        match self {
            Self::Meters => "m",
            Self::Kilometers => "km",
            Self::Feet => "ft",
            Self::Miles => "mi",
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeoMemberInput {
    member: BinaryInput,
    longitude: ExactDecimalInput,
    latitude: ExactDecimalInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeoaddInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Between 1 and 1000 binary-safe members with exact decimal coordinates.
    #[schemars(length(min = 1, max = 1000))]
    members: Vec<GeoMemberInput>,
    /// Only add new members.
    #[serde(default)]
    nx: bool,
    /// Only update existing members.
    #[serde(default)]
    xx: bool,
    /// Report both additions and coordinate changes instead of additions only.
    #[serde(default)]
    ch: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum GeoaddCountMode {
    Added,
    AddedOrChanged,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeoaddOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    affected: u64,
    count_mode: GeoaddCountMode,
}

fn geoadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_geoadd")
        .title("Add Redis Geospatial Members")
        .description(
            "Add or update 1 to 1000 binary-safe geospatial members using exact finite coordinate tokens. NX and XX are mutually exclusive; NX, XX, and CH require Redis 6.2 or newer.",
        )
        .output_schema(output_schema::<GeoaddOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GeoaddInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_geoadd")?;
                validate_items(&input.members, "members")?;
                state.validate_requested_entries(input.members.len(), "members")?;
                if input.nx && input.xx {
                    return Err(tower_mcp::Error::tool("nx and xx are mutually exclusive"));
                }
                if (input.nx || input.xx || input.ch)
                    && state
                        .redis_version()
                        .is_some_and(|version| version < RedisVersion::new(6, 2, 0))
                {
                    return Err(tower_mcp::Error::tool(
                        "GEOADD nx, xx, and ch options require Redis 6.2 or newer",
                    ));
                }
                let requested = input.members.len();
                let mut command = command("redis_geoadd", AccessMode::ReadWrite, "GEOADD");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if input.nx {
                    command.arg("NX");
                }
                if input.xx {
                    command.arg("XX");
                }
                if input.ch {
                    command.arg("CH");
                }
                for (index, member) in input.members.iter().enumerate() {
                    command
                        .arg(member.longitude.bounded_token(
                            &format!("members[{index}].longitude"),
                            -180.0,
                            180.0,
                        )?)
                        .arg(member.latitude.bounded_token(
                            &format!("members[{index}].latitude"),
                            -85.051_128_78,
                            85.051_128_78,
                        )?)
                        .arg(member.member.decode(&format!("members[{index}].member"))?);
                }
                let affected =
                    redis_nonnegative(state.raw(command, "GEOADD failed").await?, "GEOADD")?;
                state.output(&GeoaddOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested,
                    affected,
                    count_mode: if input.ch {
                        GeoaddCountMode::AddedOrChanged
                    } else {
                        GeoaddCountMode::Added
                    },
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeodistInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    from: BinaryInput,
    to: BinaryInput,
    #[serde(default)]
    unit: GeoUnit,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeodistOutput {
    key: String,
    key_encoding: InputEncoding,
    from: EncodedOutput,
    to: EncodedOutput,
    unit: GeoUnit,
    /// Exact Redis decimal distance; null when either member is missing.
    distance: Option<String>,
}

fn geodist_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_geodist")
        .title("Measure Redis Geospatial Distance")
        .description("Measure the distance between two binary-safe geospatial members in an explicit typed unit.")
        .output_schema(output_schema::<GeodistOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GeodistInput>| async move {
                let mut command = command("redis_geodist", AccessMode::ReadOnly, "GEODIST");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.from.decode("from")?)
                    .arg(input.to.decode("to")?)
                    .arg(input.unit.redis_token());
                let distance = match state.raw(command, "GEODIST failed").await? {
                    RedisValue::Nil => None,
                    value => Some(redis_decimal(value, "GEODIST")?),
                };
                state.output(&GeodistOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    from: input.from.output(),
                    to: input.to.output(),
                    unit: input.unit,
                    distance,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeoMembersInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Between 1 and 1000 binary-safe members; replies remain request-aligned.
    #[schemars(length(min = 1, max = 1000))]
    members: Vec<BinaryInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeohashEntry {
    member: EncodedOutput,
    geohash: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeohashOutput {
    key: String,
    key_encoding: InputEncoding,
    count: usize,
    members: Vec<GeohashEntry>,
}

fn geohash_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_geohash")
        .title("Read Redis Geospatial Hashes")
        .description("Read request-aligned geohash strings for 1 to 1000 binary-safe members; missing members are null.")
        .output_schema(output_schema::<GeohashOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GeoMembersInput>| async move {
                validate_items(&input.members, "members")?;
                state.validate_requested_entries(input.members.len(), "members")?;
                let mut command = command("redis_geohash", AccessMode::ReadOnly, "GEOHASH");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for (index, member) in input.members.iter().enumerate() {
                    command.arg(member.decode(&format!("members[{index}]"))?);
                }
                let values = redis_array(state.raw(command, "GEOHASH failed").await?, "GEOHASH")?;
                if values.len() != input.members.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "GEOHASH returned {} values for {} members",
                        values.len(), input.members.len()
                    )));
                }
                let members = input
                    .members
                    .iter()
                    .zip(values)
                    .map(|(member, value)| {
                        let geohash = match value {
                            RedisValue::Nil => None,
                            value => Some(redis_decimal(value, "GEOHASH")?),
                        };
                        Ok(GeohashEntry {
                            member: member.output(),
                            geohash,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                state.output_collection(
                    &GeohashOutput {
                        key: input.key,
                        key_encoding: input.key_encoding,
                        count: members.len(),
                        members,
                    },
                    input.members.len(),
                    "Retry GEOHASH with fewer members.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeoPosition {
    longitude: String,
    latitude: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeoposEntry {
    member: EncodedOutput,
    position: Option<GeoPosition>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeoposOutput {
    key: String,
    key_encoding: InputEncoding,
    count: usize,
    members: Vec<GeoposEntry>,
}

fn decode_geo_position(value: RedisValue, context: &str) -> tower_mcp::Result<Option<GeoPosition>> {
    if matches!(value, RedisValue::Nil) {
        return Ok(None);
    }
    let mut values = redis_array(value, context)?;
    if values.len() != 2 {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned {} coordinate values instead of two",
            values.len()
        )));
    }
    let latitude = redis_decimal(values.pop().expect("coordinate length checked"), context)?;
    let longitude = redis_decimal(values.pop().expect("coordinate length checked"), context)?;
    Ok(Some(GeoPosition {
        longitude,
        latitude,
    }))
}

fn geopos_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_geopos")
        .title("Read Redis Geospatial Positions")
        .description("Read request-aligned exact longitude/latitude strings for 1 to 1000 binary-safe members; missing members are null.")
        .output_schema(output_schema::<GeoposOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GeoMembersInput>| async move {
                validate_items(&input.members, "members")?;
                state.validate_requested_entries(input.members.len(), "members")?;
                let mut command = command("redis_geopos", AccessMode::ReadOnly, "GEOPOS");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for (index, member) in input.members.iter().enumerate() {
                    command.arg(member.decode(&format!("members[{index}]"))?);
                }
                let values = redis_array(state.raw(command, "GEOPOS failed").await?, "GEOPOS")?;
                if values.len() != input.members.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "GEOPOS returned {} values for {} members",
                        values.len(), input.members.len()
                    )));
                }
                let members = input
                    .members
                    .iter()
                    .zip(values)
                    .map(|(member, value)| {
                        Ok(GeoposEntry {
                            member: member.output(),
                            position: decode_geo_position(value, "GEOPOS")?,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                state.output_collection(
                    &GeoposOutput {
                        key: input.key,
                        key_encoding: input.key_encoding,
                        count: members.len(),
                        members,
                    },
                    input.members.len(),
                    "Retry GEOPOS with fewer members.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum GeoSearchCenter {
    Member {
        member: BinaryInput,
    },
    Coordinates {
        longitude: ExactDecimalInput,
        latitude: ExactDecimalInput,
    },
}

impl GeoSearchCenter {
    fn append(&self, command: &mut crate::RedisCommand, name: &str) -> tower_mcp::Result<()> {
        match self {
            Self::Member { member } => {
                command.arg("FROMMEMBER").arg(member.decode(name)?);
            }
            Self::Coordinates {
                longitude,
                latitude,
            } => {
                command
                    .arg("FROMLONLAT")
                    .arg(longitude.bounded_token(&format!("{name}.longitude"), -180.0, 180.0)?)
                    .arg(latitude.bounded_token(
                        &format!("{name}.latitude"),
                        -85.051_128_78,
                        85.051_128_78,
                    )?);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum GeoSearchShape {
    Radius {
        radius: ExactDecimalInput,
        unit: GeoUnit,
    },
    Box {
        width: ExactDecimalInput,
        height: ExactDecimalInput,
        unit: GeoUnit,
    },
}

impl GeoSearchShape {
    fn append(&self, command: &mut crate::RedisCommand, name: &str) -> tower_mcp::Result<()> {
        match self {
            Self::Radius { radius, unit } => {
                command
                    .arg("BYRADIUS")
                    .arg(radius.positive_token(&format!("{name}.radius"))?)
                    .arg(unit.redis_token());
            }
            Self::Box {
                width,
                height,
                unit,
            } => {
                command
                    .arg("BYBOX")
                    .arg(width.positive_token(&format!("{name}.width"))?)
                    .arg(height.positive_token(&format!("{name}.height"))?)
                    .arg(unit.redis_token());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum GeoSort {
    Ascending,
    Descending,
}

impl GeoSort {
    fn redis_token(&self) -> &'static str {
        match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        }
    }
}

fn default_geo_count() -> usize {
    100
}

fn validate_geo_search(count: usize) -> tower_mcp::Result<()> {
    if count == 0 || count > MAX_ITEMS {
        return Err(tower_mcp::Error::tool(format!(
            "count must be between 1 and {MAX_ITEMS}"
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeosearchInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    center: GeoSearchCenter,
    shape: GeoSearchShape,
    #[serde(default)]
    sort: Option<GeoSort>,
    /// Hard maximum number of returned members.
    #[serde(default = "default_geo_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    /// Permit Redis to stop as soon as enough matches are found. Cannot be sorted.
    #[serde(default)]
    any: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeosearchEntry {
    member: EncodedOutput,
    distance: String,
    geohash_integer: String,
    position: GeoPosition,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeosearchOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    count: usize,
    /// True when Redis returned the requested limit; additional matches may exist.
    limit_reached: bool,
    results: Vec<GeosearchEntry>,
}

fn decode_geosearch_entry(value: RedisValue) -> tower_mcp::Result<GeosearchEntry> {
    let mut values = redis_array(value, "GEOSEARCH")?;
    if values.len() != 4 {
        return Err(tower_mcp::Error::tool(format!(
            "GEOSEARCH returned {} fields instead of four",
            values.len()
        )));
    }
    let position = decode_geo_position(values.pop().expect("entry length checked"), "GEOSEARCH")?
        .ok_or_else(|| tower_mcp::Error::tool("GEOSEARCH returned a null coordinate"))?;
    let geohash_integer = redis_decimal(values.pop().expect("entry length checked"), "GEOSEARCH")?;
    let distance = redis_decimal(values.pop().expect("entry length checked"), "GEOSEARCH")?;
    let member = match values.pop().expect("entry length checked") {
        RedisValue::BulkString(value) => EncodedOutput::from(value),
        RedisValue::SimpleString(value) => EncodedOutput::from(value.into_bytes()),
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "GEOSEARCH returned an unexpected member: {other:?}"
            )));
        }
    };
    Ok(GeosearchEntry {
        member,
        distance,
        geohash_integer,
        position,
    })
}

fn geosearch_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_geosearch")
        .title("Search Redis Geospatial Index")
        .description(
            "Search a circle or box around a member or exact coordinate. COUNT is mandatory and bounded; every result includes exact distance, integer geohash, and coordinates. Requires Redis 6.2 or newer.",
        )
        .output_schema(output_schema::<GeosearchOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GeosearchInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                validate_geo_search(input.count)?;
                let mut command = command("redis_geosearch", AccessMode::ReadOnly, "GEOSEARCH");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                input.center.append(&mut command, "center")?;
                input.shape.append(&mut command, "shape")?;
                if let Some(sort) = &input.sort {
                    command.arg(sort.redis_token());
                }
                command.arg("COUNT").arg(input.count.to_string());
                if input.any {
                    command.arg("ANY");
                }
                command.arg("WITHDIST").arg("WITHHASH").arg("WITHCOORD");
                let results = redis_array(
                    state.raw(command, "GEOSEARCH failed").await?,
                    "GEOSEARCH",
                )?
                .into_iter()
                .map(decode_geosearch_entry)
                .collect::<tower_mcp::Result<Vec<_>>>()?;
                let count = results.len();
                let output = GeosearchOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested: input.count,
                    count,
                    limit_reached: count == input.count,
                    results,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry GEOSEARCH with a smaller count or search area.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeosearchstoreInput {
    destination: BinaryInput,
    source: BinaryInput,
    center: GeoSearchCenter,
    shape: GeoSearchShape,
    #[serde(default)]
    sort: Option<GeoSort>,
    /// Hard maximum number of stored members.
    #[serde(default = "default_geo_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    #[serde(default)]
    any: bool,
    /// Store distance as the destination score instead of geohash.
    #[serde(default)]
    store_distance: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GeosearchstoreOutput {
    destination: EncodedOutput,
    source: EncodedOutput,
    requested: usize,
    stored: u64,
    store_distance: bool,
    destination_overwritten: bool,
    cluster_requires_same_slot: bool,
}

fn geosearchstore_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_geosearchstore")
        .title("Store Redis Geospatial Search")
        .description(
            "Permanently overwrite a destination with at most 1000 geospatial search results. Source and destination must share a Redis Cluster slot. Requires Redis 6.2+ and full access.",
        )
        .output_schema(output_schema::<GeosearchstoreOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<GeosearchstoreInput>| async move {
                state.require(AccessMode::Full, "redis_geosearchstore")?;
                state.validate_requested_entries(input.count, "count")?;
                validate_geo_search(input.count)?;
                let mut command = command(
                    "redis_geosearchstore",
                    AccessMode::Full,
                    "GEOSEARCHSTORE",
                );
                command
                    .arg(input.destination.decode("destination")?)
                    .arg(input.source.decode("source")?);
                input.center.append(&mut command, "center")?;
                input.shape.append(&mut command, "shape")?;
                if let Some(sort) = &input.sort {
                    command.arg(sort.redis_token());
                }
                command.arg("COUNT").arg(input.count.to_string());
                if input.any {
                    command.arg("ANY");
                }
                if input.store_distance {
                    command.arg("STOREDIST");
                }
                let stored = redis_nonnegative(
                    state.raw(command, "GEOSEARCHSTORE failed").await?,
                    "GEOSEARCHSTORE",
                )?;
                state.output(&GeosearchstoreOutput {
                    destination: input.destination.output(),
                    source: input.source.output(),
                    requested: input.count,
                    stored,
                    store_distance: input.store_distance,
                    destination_overwritten: true,
                    cluster_requires_same_slot: true,
                })
            },
        )
        .build()
}

// HyperLogLog operations --------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PfaddInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Up to 1000 binary-safe observations. An empty list initializes a missing HyperLogLog.
    #[serde(default)]
    #[schemars(length(max = 1000))]
    elements: Vec<BinaryInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PfaddOutput {
    key: String,
    key_encoding: InputEncoding,
    observed: usize,
    /// True when a zero-element call created an empty HyperLogLog.
    key_created: bool,
    /// True when observations changed the HyperLogLog registers.
    register_changed: bool,
    approximate: bool,
}

fn pfadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_pfadd")
        .title("Add Redis HyperLogLog Observations")
        .description(
            "Add up to 1000 binary-safe observations to a Redis HyperLogLog. An empty list initializes a missing key; otherwise the response reports whether a register changed, not how many elements were new.",
        )
        .output_schema(output_schema::<PfaddOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<PfaddInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_pfadd")?;
                if input.elements.len() > MAX_ITEMS {
                    return Err(tower_mcp::Error::tool(format!(
                        "elements must contain at most {MAX_ITEMS} items"
                    )));
                }
                if !input.elements.is_empty() {
                    state.validate_requested_entries(input.elements.len(), "elements")?;
                }
                let mut command = command("redis_pfadd", AccessMode::ReadWrite, "PFADD");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for (index, element) in input.elements.iter().enumerate() {
                    command.arg(element.decode(&format!("elements[{index}]"))?);
                }
                let changed =
                    redis_nonnegative(state.raw(command, "PFADD failed").await?, "PFADD")?;
                if changed > 1 {
                    return Err(tower_mcp::Error::tool(format!(
                        "PFADD returned invalid change flag {changed}"
                    )));
                }
                state.output(&PfaddOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    observed: input.elements.len(),
                    key_created: input.elements.is_empty() && changed == 1,
                    register_changed: !input.elements.is_empty() && changed == 1,
                    approximate: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PfcountInput {
    /// Between 1 and 1000 HyperLogLog keys. Multiple keys must share a Redis Cluster slot.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<BinaryInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PfcountOutput {
    key_count: usize,
    /// Exact decimal representation of Redis's approximate cardinality estimate.
    estimated_cardinality: String,
    approximate: bool,
    relative_standard_error: String,
    cluster_requires_same_slot: bool,
}

fn pfcount_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_pfcount")
        .title("Count Redis HyperLogLog Cardinality")
        .description(
            "Return Redis's approximate cardinality estimate for 1 to 1000 HyperLogLog keys. Multiple keys must share a Cluster slot; the estimate has about 0.81% standard error.",
        )
        .output_schema(output_schema::<PfcountOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<PfcountInput>| async move {
                validate_items(&input.keys, "keys")?;
                state.validate_requested_entries(input.keys.len(), "keys")?;
                let mut command = command("redis_pfcount", AccessMode::ReadOnly, "PFCOUNT");
                for (index, key) in input.keys.iter().enumerate() {
                    command.arg(key.decode(&format!("keys[{index}]"))?);
                }
                let estimate =
                    redis_nonnegative(state.raw(command, "PFCOUNT failed").await?, "PFCOUNT")?;
                state.output(&PfcountOutput {
                    key_count: input.keys.len(),
                    estimated_cardinality: estimate.to_string(),
                    approximate: true,
                    relative_standard_error: "~0.81%".to_string(),
                    cluster_requires_same_slot: input.keys.len() > 1,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PfmergeInput {
    destination: BinaryInput,
    /// Up to 1000 source HyperLogLog keys. An empty list stores an empty HyperLogLog.
    #[serde(default)]
    #[schemars(length(max = 1000))]
    sources: Vec<BinaryInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PfmergeOutput {
    destination: EncodedOutput,
    source_count: usize,
    destination_overwritten: bool,
    approximate: bool,
    cluster_requires_same_slot: bool,
}

fn pfmerge_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_pfmerge")
        .title("Merge Redis HyperLogLogs")
        .description(
            "Permanently overwrite a destination with the union of up to 1000 HyperLogLogs; an empty source list stores an empty HyperLogLog. Every source must share the destination's Redis Cluster slot. Requires full access.",
        )
        .output_schema(output_schema::<PfmergeOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<PfmergeInput>| async move {
                state.require(AccessMode::Full, "redis_pfmerge")?;
                if input.sources.len() > MAX_ITEMS {
                    return Err(tower_mcp::Error::tool(format!(
                        "sources must contain at most {MAX_ITEMS} items"
                    )));
                }
                if !input.sources.is_empty() {
                    state.validate_requested_entries(input.sources.len(), "sources")?;
                }
                let mut command = command("redis_pfmerge", AccessMode::Full, "PFMERGE");
                command.arg(input.destination.decode("destination")?);
                for (index, source) in input.sources.iter().enumerate() {
                    command.arg(source.decode(&format!("sources[{index}]"))?);
                }
                require_ok(state.raw(command, "PFMERGE failed").await?, "PFMERGE")?;
                state.output(&PfmergeOutput {
                    destination: input.destination.output(),
                    source_count: input.sources.len(),
                    destination_overwritten: true,
                    approximate: true,
                    cluster_requires_same_slot: !input.sources.is_empty(),
                })
            },
        )
        .build()
}

#[cfg(feature = "bitmaps")]
pub(super) fn add_bitmap_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(getbit_tool(state.clone()));
    router = router.tool(bitcount_tool(state.clone()));
    router = router.tool(bitpos_tool(state.clone()));
    router = router.tool(bitfield_ro_tool(state.clone()));
    router
}

#[cfg(feature = "geospatial")]
pub(super) fn add_geospatial_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(geodist_tool(state.clone()));
    router = router.tool(geohash_tool(state.clone()));
    router = router.tool(geopos_tool(state.clone()));
    router = router.tool(geosearch_tool(state.clone()));
    router
}

#[cfg(feature = "hyperloglog")]
pub(super) fn add_hyperloglog_read_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(pfcount_tool(state))
}

#[cfg(feature = "bitmaps")]
pub(super) fn add_bitmap_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(setbit_tool(state.clone()));
    router = router.tool(bitfield_tool(state.clone()));
    router
}

#[cfg(feature = "geospatial")]
pub(super) fn add_geospatial_write_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(geoadd_tool(state))
}

#[cfg(feature = "hyperloglog")]
pub(super) fn add_hyperloglog_write_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(pfadd_tool(state))
}

#[cfg(feature = "bitmaps")]
pub(super) fn add_bitmap_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(bitop_tool(state))
}

#[cfg(feature = "geospatial")]
pub(super) fn add_geospatial_destructive_tools(
    router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router.tool(geosearchstore_tool(state))
}

#[cfg(feature = "hyperloglog")]
pub(super) fn add_hyperloglog_destructive_tools(
    router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router.tool(pfmerge_tool(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitfield_encodings_and_offsets_are_bounded() {
        assert!(
            BitfieldEncoding {
                signed: true,
                width: 64
            }
            .token("encoding")
            .is_ok()
        );
        assert!(
            BitfieldEncoding {
                signed: false,
                width: 64
            }
            .token("encoding")
            .is_err()
        );
        assert!(
            BitfieldOffset::Absolute {
                value: MAX_BITMAP_WRITE_BIT_OFFSET
            }
            .token(1, "offset", true)
            .is_ok()
        );
        assert!(
            BitfieldOffset::Absolute {
                value: MAX_BITMAP_WRITE_BIT_OFFSET + 1
            }
            .token(1, "offset", true)
            .is_err()
        );
    }

    #[test]
    fn exact_decimal_validation_preserves_tokens_and_bounds() {
        let coordinate = ExactDecimalInput::Exact("-122.41940000000001".into());
        assert_eq!(
            coordinate
                .bounded_token("longitude", -180.0, 180.0)
                .expect("coordinate"),
            "-122.41940000000001"
        );
        assert!(
            ExactDecimalInput::Exact("nan".into())
                .finite_token("coordinate")
                .is_err()
        );
        assert!(
            ExactDecimalInput::Exact("0".into())
                .positive_token("radius")
                .is_err()
        );
    }
}
