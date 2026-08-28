# Redis Query Engine index design

How to design Search indexes (FT.*) through this MCP surface. Requires the
Search capability (Redis 8, Redis Stack, or the RediSearch module); tools
report a capability error when it is absent.

## Start from the queries, not the data

Write down the exact queries first, then index only the fields they filter,
sort, or return. Every indexed field costs memory and write amplification —
each document write updates every index that matches its prefix.

Create with `redis_ft_create`; verify shape and doc counts with
`redis_ft_info`; list existing indexes with `redis_ft_list`.

## HASH or JSON documents

- `on: hash` indexes flat hash fields written by `redis_hset`.
- `on: json` indexes JSONPath expressions over documents written by
  `redis_json_set`; one JSON index can reach nested arrays and objects
  (`$.reviews[*].rating`).

Use a key `prefix` per logical collection (`product:`) so unrelated keys
never enter the index.

## Field types and when each is right

- `text` — tokenized full-text search with stemming and optional per-field
  `weight`. Only use for fields humans search with words. `sortable` on a
  text field costs extra memory.
- `tag` — exact-match categorical values (status, tenant, SKU). Cheaper than
  text: no tokenization or stemming. Case sensitivity and separators are
  explicit options. The query syntax is `@status:{active}`.
- `numeric` — range filters and sorting (`@price:[10 100]`).
- `geo` — longitude/latitude points for radius queries.
- `vector` — KNN similarity; see below.

Common mistake: indexing identifiers as `text`. Tokenization splits and
stems them; use `tag`.

## Vector fields

`redis_ft_create` accepts typed `flat` and `hnsw` vector fields with
dimension, distance metric (`cosine`, `l2`, `ip`), and type. Choosing:

- `flat` — exact brute-force KNN. Right up to roughly the low hundreds of
  thousands of vectors, or whenever recall must be exact.
- `hnsw` — approximate graph search for larger sets; `m` and
  `ef_construction`/`ef_runtime` trade build cost and memory against recall.

Write vectors with `redis_vector_set_hash` (binary-safe float32 blobs) or as
JSON arrays; read them back with `redis_vector_get_hash`. Query with
`redis_ft_vector_search` for pure KNN, or `redis_ft_hybrid_search` to combine
a filter expression with vector ranking — hybrid filters cut the candidate
set before ranking, which is usually the difference between usable and not.

For no-index similarity over a single collection, Redis 8 native vector sets
(`redis_vadd`, `redis_vsim`) are lighter than a Search index; choose Search
when similarity must combine with text/tag/numeric filters over documents.

## Querying well

- `redis_ft_search` always emits an explicit LIMIT and returns typed
  documents plus a continuation offset; page rather than raising the limit.
- Restrict output with `return_fields`; returning whole large documents
  through search results is the most common self-inflicted bandwidth
  problem.
- `redis_ft_aggregate` runs the pipeline (group, reduce, apply, sort) on the
  server; use it instead of fetching raw documents to aggregate client-side.
  Cursors (`with_cursor`, then `redis_ft_cursor_read` / and
  `redis_ft_cursor_del`) stream large aggregate results in bounded pages.
- Debug relevance and plans with `redis_ft_explain` and per-stage timing
  with `redis_ft_profile`.
- Synonyms (`redis_ft_synupdate`, `redis_ft_syndump`) and custom stopword
  dictionaries (`redis_ft_dictadd`, `redis_ft_dictdump`) tune text recall
  without reindexing documents.

## Operating indexes

- Schema changes: `redis_ft_alter` adds fields to an existing index
  (existing docs backfill in the background); removing or retyping a field
  requires a new index. Create the new index, verify with `redis_ft_info`,
  swap the alias, then `redis_ft_dropindex` the old one.
- Aliases (`redis_ft_aliasadd`, `redis_ft_aliasupdate`) give applications a
  stable name across rebuilds — always query through an alias in
  production.
- `redis_ft_dropindex` is destructive; by default it drops only the index,
  not the documents.
- On Redis Cluster, Search indexes are node-local structures; this library's
  Search tools operate against the node that answers. Plan index placement
  accordingly rather than assuming database-wide coverage.
