//! Owned response conversion and binary-safe decoding shared by all adapters.

use redis_tower_core::{Frame, RedisError as TowerError};

use crate::{RedisError, RedisErrorKind, RedisValue};

/// Preserve redis-rs URL protocol selection while using tower's URL factory.
/// Tower's parser does not understand the `protocol` query parameter itself.
/// The legacy standalone default is RESP2; explicit RESP3 remains available.
pub(crate) fn connection_factory(
    url: &str,
) -> Result<redis_tower::reconnect::UrlConnectionFactory, RedisError> {
    let target = crate::transport::Target::parse(url)?;
    Ok(
        redis_tower::reconnect::UrlConnectionFactory::new(target.url)
            .with_connection_config(target.config),
    )
}

#[cfg(test)]
fn connection_url(url: &str) -> Result<(String, redis_tower_core::ProtocolVersion), RedisError> {
    let target = crate::transport::Target::parse(url)?;
    Ok((target.url, target.config.protocol()))
}

fn server_error(message: &str) -> RedisError {
    let code = message.split_whitespace().next().unwrap_or("ERR");
    let kind = match code {
        "NOAUTH" | "WRONGPASS" => RedisErrorKind::Authentication,
        "NOPERM" => RedisErrorKind::Authorization,
        "CROSSSLOT" => RedisErrorKind::InvalidRequest,
        _ => RedisErrorKind::Server,
    };
    RedisError::new(kind, message).with_code(code)
}

impl From<TowerError> for RedisError {
    fn from(error: TowerError) -> Self {
        fn classify(error: &TowerError) -> RedisError {
            let kind = match error {
                TowerError::Redis(message) => return server_error(message),
                TowerError::ReconnectFailed { last_error, .. } => return classify(last_error),
                TowerError::ConnectTimeout
                | TowerError::CommandTimeout
                | TowerError::PoolAcquisitionTimeout { .. } => RedisErrorKind::Timeout,
                TowerError::Connection { source, .. }
                    if source.kind() == std::io::ErrorKind::TimedOut =>
                {
                    RedisErrorKind::Timeout
                }
                TowerError::Connection { .. }
                | TowerError::ConnectionClosed
                | TowerError::CircuitOpen => RedisErrorKind::Connection,
                TowerError::Protocol(_)
                | TowerError::UnexpectedResponse { .. }
                | TowerError::TypeMismatch { .. } => RedisErrorKind::InvalidResponse,
                TowerError::InvalidUrl(_)
                | TowerError::IndexOutOfBounds { .. }
                | TowerError::ConnectionInUse => RedisErrorKind::InvalidRequest,
                _ => RedisErrorKind::Other,
            };
            RedisError::new(kind, error.to_string())
        }
        classify(&error)
    }
}

impl From<Frame> for RedisValue {
    fn from(frame: Frame) -> Self {
        fn pairs(values: Vec<(Frame, Frame)>) -> Vec<(RedisValue, RedisValue)> {
            values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect()
        }
        match frame {
            Frame::Null | Frame::BulkString(None) | Frame::Array(None) => Self::Nil,
            Frame::Integer(value) => Self::Integer(value),
            Frame::BulkString(Some(value)) | Frame::StreamedStringChunk(value) => {
                Self::BulkString(value.to_vec())
            }
            Frame::StreamedString(chunks) => Self::BulkString(chunks.concat()),
            Frame::SimpleString(value) if value.as_ref() == b"OK" => Self::Okay,
            Frame::SimpleString(value) => match String::from_utf8(value.to_vec()) {
                Ok(value) => Self::SimpleString(value),
                Err(error) => Self::BulkString(error.into_bytes()),
            },
            Frame::Array(Some(values)) | Frame::StreamedArray(values) => {
                Self::Array(values.into_iter().map(Self::from).collect())
            }
            Frame::Set(values) | Frame::StreamedSet(values) => {
                Self::Set(values.into_iter().map(Self::from).collect())
            }
            Frame::Map(values) | Frame::StreamedMap(values) => Self::Map(pairs(values)),
            // An attribute prefix without its following value is not a
            // complete reply. The wire codec rejects it before dispatch.
            Frame::Attribute(_) | Frame::StreamedAttribute(_) => {
                Self::Unsupported("RESP3 attribute prefix has no attached response value".into())
            }
            Frame::Double(value) => Self::Double(value),
            Frame::SpecialFloat(value) => match value.as_ref() {
                b"inf" | b"+inf" => Self::Double(f64::INFINITY),
                b"-inf" => Self::Double(f64::NEG_INFINITY),
                b"nan" => Self::Double(f64::NAN),
                _ => Self::Unsupported(format!("invalid special float: {value:?}")),
            },
            Frame::Boolean(value) => Self::Boolean(value),
            Frame::BigNumber(value) => Self::BigNumber(value.to_vec()),
            Frame::VerbatimString(format, text) => {
                match (
                    String::from_utf8(format.to_vec()),
                    String::from_utf8(text.to_vec()),
                ) {
                    (Ok(format), Ok(text)) => Self::VerbatimString { format, text },
                    // The public verbatim variant is UTF-8-only; retain binary
                    // payloads losslessly in the byte-string variant.
                    _ => Self::BulkString(text.to_vec()),
                }
            }
            Frame::Push(values) | Frame::StreamedPush(values) => {
                let mut values = values.into_iter();
                let kind = values
                    .next()
                    .map(|kind| match kind.as_str() {
                        Some(kind) => kind.to_owned(),
                        None => format!("{kind:?}"),
                    })
                    .unwrap_or_default();
                Self::Push {
                    kind,
                    data: values.map(Self::from).collect(),
                }
            }
            Frame::Error(message) | Frame::BlobError(message) => {
                let message = String::from_utf8_lossy(&message);
                let (code, message) = match message.split_once(' ') {
                    Some((code, message)) => (code.to_owned(), Some(message.to_owned())),
                    None => (message.into_owned(), None),
                };
                Self::ServerError { code, message }
            }
            other => Self::Unsupported(format!("{other:?}")),
        }
    }
}

fn malformed(expected: &str) -> RedisError {
    RedisError::new(
        RedisErrorKind::InvalidResponse,
        format!("expected Redis {expected}"),
    )
}

// Typed tools reject errors at every depth, as the old typed conversion did.
// Raw tools never call this decoder, so nested server errors remain in-band.
fn validate(value: &RedisValue) -> Result<(), RedisError> {
    match value {
        RedisValue::ServerError { code, message } => {
            return Err(server_error(&match message {
                Some(message) => format!("{code} {message}"),
                None => code.clone(),
            }));
        }
        RedisValue::Unsupported(_) => return Err(malformed("supported response")),
        RedisValue::Array(values)
        | RedisValue::Set(values)
        | RedisValue::Push { data: values, .. } => {
            for value in values {
                validate(value)?;
            }
        }
        RedisValue::Map(values) => {
            for (key, value) in values {
                validate(key)?;
                validate(value)?;
            }
        }
        RedisValue::Attribute { data, attributes } => {
            validate(data)?;
            for (key, value) in attributes {
                validate(key)?;
                validate(value)?;
            }
        }
        RedisValue::ClusterNodes(values) => {
            for (_, value) in values {
                validate(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn data(mut value: RedisValue) -> RedisValue {
    while let RedisValue::Attribute { data, .. } = value {
        value = *data;
    }
    value
}

/// Internal decoding contract. The byte and sequence hooks distinguish bulk
/// payloads (`Vec<u8>`) from arrays, without specialization or unsafe casts.
pub(crate) trait FromRedisValue: Sized {
    fn decode(value: RedisValue) -> Result<Self, RedisError>;

    fn from_redis_value(value: RedisValue) -> Result<Self, RedisError> {
        validate(&value)?;
        Self::decode(data(value))
    }

    fn sequence(values: Vec<RedisValue>) -> Result<Vec<Self>, RedisError> {
        values.into_iter().map(Self::from_redis_value).collect()
    }

    fn bytes(_bytes: Vec<u8>) -> Result<Vec<Self>, RedisError> {
        Err(malformed("array or set"))
    }
}

impl FromRedisValue for u8 {
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        let integer = i64::from_redis_value(value)?;
        integer.try_into().map_err(|_| malformed("byte in 0..=255"))
    }

    fn bytes(bytes: Vec<u8>) -> Result<Vec<Self>, RedisError> {
        Ok(bytes)
    }
}

impl<T: FromRedisValue> FromRedisValue for Vec<T> {
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        match value {
            RedisValue::Nil => Ok(Vec::new()),
            RedisValue::BulkString(bytes) => T::bytes(bytes),
            RedisValue::SimpleString(value) => T::bytes(value.into_bytes()),
            RedisValue::VerbatimString { text, .. } => T::bytes(text.into_bytes()),
            RedisValue::Okay => T::bytes(b"OK".to_vec()),
            RedisValue::Array(values) | RedisValue::Set(values) => T::sequence(values),
            RedisValue::Map(pairs) => T::sequence(
                pairs
                    .into_iter()
                    .map(|(key, value)| RedisValue::Array(vec![key, value]))
                    .collect(),
            ),
            _ => Err(malformed("bulk bytes, array, set or map")),
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Option<T> {
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        match value {
            RedisValue::Nil => Ok(None),
            value => T::from_redis_value(value).map(Some),
        }
    }
}

impl FromRedisValue for String {
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        match value {
            RedisValue::SimpleString(value) | RedisValue::VerbatimString { text: value, .. } => {
                Ok(value)
            }
            RedisValue::BulkString(value) => {
                String::from_utf8(value).map_err(|_| malformed("UTF-8 string"))
            }
            RedisValue::Okay => Ok("OK".into()),
            RedisValue::Integer(value) => Ok(value.to_string()),
            RedisValue::Double(value) => Ok(value.to_string()),
            _ => Err(malformed("string")),
        }
    }
}

macro_rules! number {
    ($($ty:ty),*) => { $(
        impl FromRedisValue for $ty {
            fn decode(value: RedisValue) -> Result<Self, RedisError> {
                let text = match value {
                    RedisValue::Integer(value) => value.to_string(),
                    RedisValue::Double(value) => value.to_string(),
                    RedisValue::SimpleString(value) => value,
                    RedisValue::BulkString(value) => String::from_utf8(value).map_err(|_| malformed(stringify!($ty)))?,
                    _ => return Err(malformed(stringify!($ty))),
                };
                text.parse().map_err(|_| malformed(concat!(stringify!($ty), " number within range")))
            }
        }
    )* };
}
number!(i64, u64, usize, i32, u32, f64, f32);

impl FromRedisValue for bool {
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        match value {
            RedisValue::Boolean(value) => Ok(value),
            RedisValue::Integer(value) => Ok(value != 0),
            RedisValue::Nil => Ok(false),
            RedisValue::Okay => Ok(true),
            RedisValue::BulkString(value) if value == b"0" => Ok(false),
            RedisValue::BulkString(value) if value == b"1" => Ok(true),
            RedisValue::SimpleString(value) if value == "0" => Ok(false),
            RedisValue::SimpleString(value) if value == "1" => Ok(true),
            _ => Err(malformed("boolean or 0/1")),
        }
    }
}

impl FromRedisValue for () {
    fn decode(_value: RedisValue) -> Result<Self, RedisError> {
        Ok(())
    }
}

impl<A: FromRedisValue, B: FromRedisValue> FromRedisValue for (A, B) {
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        let values = match value {
            RedisValue::Array(values) => values,
            RedisValue::Map(mut pairs) if pairs.len() == 1 => {
                let (key, value) = pairs.pop().expect("one pair checked");
                vec![key, value]
            }
            _ => return Err(malformed("two-element tuple or single map entry")),
        };
        let [first, second]: [RedisValue; 2] = values
            .try_into()
            .map_err(|_| malformed("two-element tuple"))?;
        Ok((A::from_redis_value(first)?, B::from_redis_value(second)?))
    }

    fn sequence(values: Vec<RedisValue>) -> Result<Vec<Self>, RedisError> {
        // RESP3 pair arrays and RESP2 flat pairs are both used by hash and
        // sorted-set commands. Reject odd/mixed shapes instead of truncating.
        if values
            .iter()
            .all(|value| matches!(value, RedisValue::Array(_)))
        {
            return values.into_iter().map(Self::from_redis_value).collect();
        }
        if !values.len().is_multiple_of(2) {
            return Err(malformed("even-length flat pairs"));
        }
        let mut values = values.into_iter();
        let mut result = Vec::with_capacity(values.len() / 2);
        while let Some(first) = values.next() {
            let second = values.next().expect("even length checked");
            result.push((A::from_redis_value(first)?, B::from_redis_value(second)?));
        }
        Ok(result)
    }
}

impl<K: FromRedisValue + Ord, V: FromRedisValue> FromRedisValue
    for std::collections::BTreeMap<K, V>
{
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        Vec::<(K, V)>::from_redis_value(value).map(|pairs| pairs.into_iter().collect())
    }
}

impl<K: FromRedisValue + Eq + std::hash::Hash, V: FromRedisValue> FromRedisValue
    for std::collections::HashMap<K, V>
{
    fn decode(value: RedisValue) -> Result<Self, RedisError> {
        Vec::<(K, V)>::from_redis_value(value).map(|pairs| pairs.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccessMode, RedisCommand};
    use redis_tower_core::Command;

    fn bulk(bytes: &[u8]) -> RedisValue {
        RedisValue::BulkString(bytes.to_vec())
    }

    #[test]
    fn binary_optional_payloads_preserve_null_and_empty() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(vec![0xff, 0, 0x80].into())),
            Frame::BulkString(None),
            Frame::BulkString(Some(Vec::new().into())),
        ]));
        assert_eq!(
            Vec::<Option<Vec<u8>>>::from_redis_value(frame.into()).unwrap(),
            vec![Some(vec![0xff, 0, 0x80]), None, Some(vec![])]
        );
        assert_eq!(
            Option::<Vec<u8>>::from_redis_value(RedisValue::Nil).unwrap(),
            None
        );
        assert_eq!(
            Vec::<u8>::from_redis_value(bulk(&[0xff, 0])).unwrap(),
            vec![0xff, 0]
        );
        assert!(String::from_redis_value(bulk(&[0xff])).is_err());
        for null in [Frame::Null, Frame::BulkString(None), Frame::Array(None)] {
            assert_eq!(RedisValue::from(null), RedisValue::Nil);
        }
    }

    #[test]
    fn top_level_errors_fail_but_nested_madd_errors_remain_data() {
        let command = RedisCommand::new("test", AccessMode::ReadOnly, "TS.MADD");
        for error in [
            Frame::Error(b"ERR invalid timestamp"[..].into()),
            Frame::BlobError(b"ERR invalid timestamp"[..].into()),
        ] {
            let converted = RedisValue::from(error.clone());
            assert!(matches!(converted, RedisValue::ServerError { ref code, .. } if code == "ERR"));
            assert!(command.parse_response(error.clone()).is_err());
            let response = command
                .parse_response(Frame::Array(Some(vec![Frame::Integer(7), error])))
                .unwrap();
            assert!(matches!(&response, RedisValue::Array(values) if values[1] == converted));
            assert_eq!(
                Vec::<i64>::from_redis_value(response).unwrap_err().kind(),
                RedisErrorKind::Server
            );
        }
    }

    #[test]
    fn maps_sets_attributes_and_streamed_forms_are_owned() {
        let pair = (
            Frame::BulkString(Some(vec![0xff].into())),
            Frame::Integer(3),
        );
        let expected = vec![(bulk(&[0xff]), RedisValue::Integer(3))];
        assert_eq!(
            RedisValue::from(Frame::Map(vec![pair.clone()])),
            RedisValue::Map(expected.clone())
        );
        assert_eq!(
            RedisValue::from(Frame::StreamedMap(vec![pair.clone()])),
            RedisValue::Map(expected.clone())
        );
        assert!(matches!(
            RedisValue::from(Frame::Attribute(vec![pair])),
            RedisValue::Unsupported(_)
        ));
        assert_eq!(
            RedisValue::from(Frame::Set(vec![Frame::Integer(4)])),
            RedisValue::Set(vec![RedisValue::Integer(4)])
        );
        assert_eq!(
            RedisValue::from(Frame::StreamedString(vec![
                b"a"[..].into(),
                vec![0xff].into()
            ])),
            bulk(&[b'a', 0xff])
        );
        assert_eq!(
            RedisValue::from(Frame::BigNumber(b"12345678901234567890"[..].into())),
            RedisValue::BigNumber(b"12345678901234567890".to_vec())
        );
        assert_eq!(
            RedisValue::from(Frame::Push(vec![
                Frame::SimpleString(b"message"[..].into()),
                Frame::Integer(7)
            ])),
            RedisValue::Push {
                kind: "message".into(),
                data: vec![RedisValue::Integer(7)]
            }
        );
        assert_eq!(
            RedisValue::from(Frame::VerbatimString(
                b"txt"[..].into(),
                b"hello"[..].into()
            )),
            RedisValue::VerbatimString {
                format: "txt".into(),
                text: "hello".into()
            }
        );
        assert_eq!(
            RedisValue::from(Frame::SpecialFloat(b"inf"[..].into())),
            RedisValue::Double(f64::INFINITY)
        );
    }

    #[test]
    fn typed_map_flat_pairs_nested_pairs_and_scan_tuple_agree() {
        type Pairs = Vec<(Vec<u8>, f64)>;
        let flat = RedisValue::Array(vec![
            bulk(b"a"),
            bulk(b"1.5"),
            bulk(&[0xff]),
            RedisValue::Double(2.0),
        ]);
        let map = RedisValue::Map(vec![
            (bulk(b"a"), bulk(b"1.5")),
            (bulk(&[0xff]), RedisValue::Double(2.0)),
        ]);
        let nested = RedisValue::Array(vec![
            RedisValue::Array(vec![bulk(b"a"), bulk(b"1.5")]),
            RedisValue::Array(vec![bulk(&[0xff]), RedisValue::Double(2.0)]),
        ]);
        let expected = vec![(b"a".to_vec(), 1.5), (vec![0xff], 2.0)];
        for value in [flat.clone(), map, nested] {
            assert_eq!(Pairs::from_redis_value(value).unwrap(), expected);
        }
        let scan = RedisValue::Array(vec![bulk(b"42"), flat]);
        assert_eq!(
            <(u64, Pairs)>::from_redis_value(scan).unwrap(),
            (42, expected)
        );
        let attr = RedisValue::Attribute {
            data: Box::new(bulk(b"17")),
            attributes: vec![(bulk(b"meta"), RedisValue::Boolean(true))],
        };
        assert_eq!(u64::from_redis_value(attr).unwrap(), 17);
        assert_eq!(
            Vec::<i64>::from_redis_value(RedisValue::Set(vec![RedisValue::Integer(2)])).unwrap(),
            vec![2]
        );
    }

    #[test]
    fn malformed_decoding_is_explicit_and_does_not_truncate() {
        for value in [
            RedisValue::Array(vec![bulk(b"key")]),
            RedisValue::Array(vec![RedisValue::Array(vec![bulk(b"key")])]),
        ] {
            let error = Vec::<(Vec<u8>, Vec<u8>)>::from_redis_value(value).unwrap_err();
            assert_eq!(error.kind(), RedisErrorKind::InvalidResponse);
            assert!(error.message().contains("expected Redis"));
        }
        assert!(u64::from_redis_value(RedisValue::Integer(-1)).is_err());
        assert!(i64::from_redis_value(bulk(b"nonsense")).is_err());
        assert!(Vec::<Option<Vec<u8>>>::from_redis_value(bulk(b"not an array")).is_err());
        assert!(<(u64, Vec<u8>)>::from_redis_value(RedisValue::Array(vec![])).is_err());
    }

    #[test]
    fn scalar_and_binary_decoding_matches_legacy_oracle() {
        fn compare<T>(value: RedisValue)
        where
            T: FromRedisValue + redis::FromRedisValue + PartialEq + std::fmt::Debug,
        {
            let expected = <T as redis::FromRedisValue>::from_redis_value(
                value.clone().into_redis_rs().unwrap(),
            )
            .unwrap();
            assert_eq!(
                <T as FromRedisValue>::from_redis_value(value).unwrap(),
                expected
            );
        }
        compare::<Vec<u8>>(bulk(&[0, 255, 128]));
        compare::<Option<Vec<u8>>>(RedisValue::Nil);
        compare::<Vec<Option<Vec<u8>>>>(RedisValue::Array(vec![
            bulk(b""),
            RedisValue::Nil,
            bulk(&[255]),
        ]));
        compare::<String>(RedisValue::Okay);
        compare::<bool>(RedisValue::Boolean(true));
        compare::<bool>(RedisValue::Integer(0));
        compare::<i64>(bulk(b"-42"));
        compare::<u64>(bulk(b"18446744073709551615"));
        compare::<f64>(RedisValue::Double(1.25));
        compare::<Vec<(Vec<u8>, Vec<u8>)>>(RedisValue::Map(vec![(bulk(b"field"), bulk(&[255]))]));
        compare::<(String, i64)>(RedisValue::Map(vec![(
            bulk(b"field"),
            RedisValue::Integer(9),
        )]));
        compare::<std::collections::BTreeMap<String, i64>>(RedisValue::Array(vec![
            bulk(b"field"),
            RedisValue::Integer(9),
        ]));
        assert_eq!(
            RedisValue::from(Frame::Boolean(true)),
            RedisValue::Boolean(true)
        );
        assert_eq!(
            RedisValue::from(Frame::SimpleString(b"OK"[..].into())),
            RedisValue::Okay
        );
        assert_eq!(
            RedisValue::from(Frame::SimpleString(vec![255, 0].into())),
            bulk(&[255, 0])
        );
    }

    #[test]
    fn error_categories_and_codes_survive_conversion() {
        for (message, kind, code) in [
            (
                "NOAUTH authentication required",
                RedisErrorKind::Authentication,
                "NOAUTH",
            ),
            (
                "WRONGPASS invalid password",
                RedisErrorKind::Authentication,
                "WRONGPASS",
            ),
            (
                "NOPERM permission denied",
                RedisErrorKind::Authorization,
                "NOPERM",
            ),
            (
                "CROSSSLOT keys differ",
                RedisErrorKind::InvalidRequest,
                "CROSSSLOT",
            ),
            ("WRONGTYPE wrong value", RedisErrorKind::Server, "WRONGTYPE"),
        ] {
            let error = RedisError::from(TowerError::Redis(message.into()));
            assert_eq!(error.kind(), kind);
            assert_eq!(error.code(), Some(code));
        }
        for error in [TowerError::ConnectTimeout, TowerError::CommandTimeout] {
            assert_eq!(RedisError::from(error).kind(), RedisErrorKind::Timeout);
        }
        assert_eq!(
            RedisError::from(TowerError::ConnectionClosed).kind(),
            RedisErrorKind::Connection
        );
        assert_eq!(
            RedisError::from(TowerError::ReconnectFailed {
                attempts: 3,
                last_error: std::sync::Arc::new(TowerError::ConnectTimeout)
            })
            .kind(),
            RedisErrorKind::Timeout
        );
    }

    #[test]
    fn url_protocol_is_explicit_and_legacy_default_is_preserved() {
        use redis_tower_core::ProtocolVersion;
        assert_eq!(
            connection_url("redis://localhost/2").unwrap(),
            ("redis://localhost/2".into(), ProtocolVersion::Resp2)
        );
        assert_eq!(
            connection_url("redis://localhost/2?protocol=resp3").unwrap(),
            ("redis://localhost/2".into(), ProtocolVersion::Resp3)
        );
        assert_eq!(
            connection_url("unix:///tmp/redis.sock?db=2&protocol=resp2").unwrap(),
            ("unix:///tmp/redis.sock?db=2".into(), ProtocolVersion::Resp2)
        );
        assert!(connection_url("redis://localhost/?protocol=invalid").is_err());
    }
}
