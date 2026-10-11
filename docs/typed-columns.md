# Typed attribute columns

`attributes` is JSON text, and no engine can prune on JSON: `latency_ms > 500`
means decoding and parsing every row. log-less therefore promotes attribute
keys that are **stable** (one type) and **frequent** (on most of a service's
records) to their own typed Parquet columns, and keeps the JSON for everything
else. Code: `crates/logless-core/src/typed.rs`.

## The contract

This is what a consumer (DuckDB, Polars, the console) may rely on.

**Naming.** A typed column is named `attr_` + the attribute key, lowercased,
with every byte outside `[a-z0-9_]` replaced by `_`:

| key | column |
|---|---|
| `latency_ms` | `attr_latency_ms` |
| `http.status_code` | `attr_http_status_code` |
| `User-Id` | `attr_user_id` |

A key is never promoted if it is empty, longer than 64 bytes, has no ASCII
letter or digit, or sanitises to a column name another key already owns (for
example `User.Id` once `user_id` holds `attr_user_id`). No base column starts
with `attr_`.

**Types.** Each column has exactly one type, fixed when it is registered and
never changed:

| type | Parquet / Arrow | holds |
|---|---|---|
| `int64` | INT64 / `Int64` | integer values |
| `float64` | DOUBLE / `Float64` | float values, and integers with magnitude at most 2^53 |
| `bool` | BOOLEAN / `Boolean` | booleans |
| `string` | BYTE_ARRAY UTF8 / `Utf8` | strings |
| `timestamp` | INT64 TIMESTAMP(NANOS, UTC) / `Timestamp(ns, "UTC")` | strings already in canonical RFC 3339 form |

The canonical timestamp form is `YYYY-MM-DDTHH:MM:SS[.fff|.ffffff|.fffffffff]Z`:
UTC, a `Z`, and the shortest of 0, 3, 6 or 9 fraction digits that is exact,
years 1678–2261. `2024-05-01T10:00:00.250Z` conforms; `...+01:00`, `.000Z`,
`.1Z` and `2024-05-01 10:00:00Z` do not, and stay in the JSON byte for byte.

**Where a value goes.** A value that conforms to its column's type is written
to the column and *removed* from `attributes`. A value that does not conform
(wrong type, a non-canonical timestamp string, an integer too large for a
float column, a second occurrence of the same key on one record) stays in
`attributes` and the column is NULL on that row. Nothing is coerced into a
different value and nothing is dropped. So:

* `attr_x IS NOT NULL` means the record had key `x` with a conforming value.
* To find every record with key `x` regardless of type, check both the column
  and `attributes`.
* `attributes` is NULL when every attribute of the record was promoted.

**Per file.** A file contains only the typed columns that have at least one
value in it, after the 11 base columns, in column-name order. Files written
before a key was promoted lack that column. **Read the store with schema union
by name**:

```sql
SELECT count(*) FROM read_parquet('store/**/*.parquet',
                                  hive_partitioning = true,
                                  union_by_name = true)
WHERE attr_latency_ms > 500;
```

Because a column name has one type for the life of the store, union by name
never meets a type conflict.

**Self-description.** Each typed Arrow field carries metadata
`logless.attr_key` (the original key) and `logless.attr_type`. Each file's
Parquet key-value metadata has `logless.typed_columns`, a JSON array of
`{"column", "key", "type", "service", "promoted_at"}` for the columns it holds,
and `logless.schema_version = "2"`.

**Discovery.** `GET /admin/v1/typed-columns` returns the registry
(`docs/admin-api.md`); `logless catalog --config ...` prints it.

## Promotion

Promotion runs in the merge (per partition of each WAL segment) and in
compaction. For each service with at least `min_rows` records in the batch, a
key is a candidate when:

* it is on at least `min_presence` of the service's records, and
* at least `min_type_share` of its values conform to one type. Floats win over
  integers when both appear (SDKs serialise `2.0` as `2`); then integers, bools,
  canonical timestamps, strings. Strings whose mean length exceeds
  `max_string_bytes` are not promoted.

Candidates are taken most-present first (ties by key), up to `max_per_service`
promotions charged to that service and `max_columns` in total. Services are
visited in name order, so when several services share a key the first one by
name is charged for it; the column then holds conforming values from every
service.

Key statistics are computed on a deterministic stride sample of at most 32,768
rows per batch; `min_rows` counts every row. The decision is deterministic for
a given batch and registry, which matters because a re-merge after a crash
rewrites the same file path.

```toml
[typed_columns]
max_per_service = 16    # 0 promotes nothing
max_columns = 128
min_rows = 200
min_presence = 0.5
min_type_share = 0.95
max_string_bytes = 256
```

Compaction applies the current registry when it rewrites files, so files
written as JSON-only before a promotion gain the typed column once compacted.
Compaction sees more rows per partition than one merge, so it can also promote
keys that a single small segment was too small to judge.

## Registry and recovery

The registry is the `typed_columns` table in the catalog. New promotions are
registered *before* the file that uses them is written, so the registry is
always a superset of what any file holds. `logless catalog --rebuild`
re-registers every column declared in file footers and never removes one: a
registered column with no file is harmless, while a forgotten one could be
re-promoted with a different type.

Replay (`crate::scan`) and compaction rebuild each record's attributes from
the typed columns plus the JSON remainder. What is not preserved: attribute
*order* (typed values come first), and integers stored in a `float64` column,
which replay as floats.

## Measured

`cargo build --release -p logless-cli --bin typed-columns && target/release/typed-columns 500000 5`
on an Apple M4 Max with a load average around 45 from other work, so the
end-to-end write time (which includes fsync) varies between runs; the
encode-only times are the stable comparison. Synthetic data: 500,000 request
records, 4 services, 7 keys on every record (int latency, int status, route,
bool, float, millisecond timestamp, high-cardinality user id) and 10 rare keys
at 2% each. All 7 common keys are promoted and none of the rare ones.

| | JSON only | typed | typed / JSON |
|---|---|---|---|
| encode (`to_record_batch`, no I/O) | 359.7 ms | 268.7 ms | 0.75x |
| write (encode + Parquet + fsync) | 678.1 ms | 491.2 ms | 0.72x |
| file bytes | 35,795,779 | 30,881,020 | 0.86x |
| attribute bytes (`attributes` + `attr_*`) | 16,848,927 | 11,923,277 | 0.71x |
| promotion plan, empty registry | | 34.1 ms | |
| promotion plan, keys already registered | | 21.2 ms | |

Writes got cheaper, not dearer: serialising JSON was the expensive part and the
promoted values no longer go through it. Across four runs the encode ratio was
0.75x–0.81x; one run's end-to-end write measured 2.31x the other way, which
was fsync variance on the loaded machine, not the encoder.

Reading `latency_ms > 500` (996 of 500,000 rows, 0.20%):

| | JSON only | typed |
|---|---|---|
| compressed bytes read | 16,848,927 | 586,301 (0.035x) |
| time | 319.0 ms | 1.3 ms (0.004x) |
| row groups read | 8 / 8 | 8 / 8 |

**Row-group pruning on min/max is 0 of 8** for `latency_ms > 500`,
`http.status_code >= 500` and a last-6-minutes range on the timestamp
attribute. Files are sorted by `(service, trace_id, timestamp)` and trace ids are
random, so every row group spans the full value range of an attribute that is
not correlated with service. The gain is in reading one narrow column instead
of parsing JSON, not in skipping row groups. Pruning would need a sort or
clustering by the predicate column, or page-level statistics, neither of which
was measured.

The shape meant to lose: each of 4 services has its own 16 keys of random
integers, so a mixed-service file gets 64 typed columns, each about 75% NULL.
It still came out ahead (encode 0.49x, bytes 0.84x at 500,000 rows; encode
0.40x, bytes 0.87x at 2,000 rows). A shape where promotion costs more than it
saves was not found.

Not measured: DuckDB or Polars query times against the store (log-less has no
query engine and this measurement reads Parquet directly), page-index pruning,
and behaviour on real customer attribute distributions.
