# KV-Block Index

The block-level store that maps KV-block keys to the pods holding them, plus the
token-to-block-key processing that feeds it. This is the shared state the
[`kvcache.Indexer`](../README.md) reads and the [`kvevents`](../../kvevents/README.md)
subscriber writes.

## What It Does

- **Token processing.** `TokenProcessor` (backed by `ChunkedTokenDatabase`)
  turns a token sequence into KV-block keys: it chunks tokens into fixed-size
  blocks and hashes each block, chaining the previous block's hash so a key
  encodes its full prefix. `ComputeBlockExtraFeatures` folds multimodal
  metadata into per-block `BlockExtraFeatures` that taint the hash.
- **Block index.** `Index` maps each block key to the set of pods (`PodEntry`,
  carrying a pod identifier and device tier) that currently hold it. Producers
  `Add` and `Evict` entries; the indexer's `Lookup` reads them.
- **Ordered walk.** `KeyWalker` is an optional capability for callers that
  fold entries in key order: it visits the requested keys in sequence,
  reporting misses, and lends each key's entries as `EntryRef` values that
  carry the index's pod and tier ordinals. The in-memory backend has it and
  the decorators keep it; the [`kvcache`](../README.md) prefix matcher walks
  when it can and falls back to `Lookup`.

## Backends

`NewIndex` selects a backend from `IndexConfig` (first configured wins):

| Backend | Constructor | Use |
|---------|-------------|-----|
| In-memory | `NewInMemoryIndex` | Single-replica, LRU-bounded local index. |
| Cost-aware memory | `NewCostAwareMemoryIndex` | Local index with cost-aware eviction. |
| Redis | `NewRedisIndex` | Shared index across EPP replicas (Redis-protocol server). |

Two decorators wrap any backend: `NewTracedIndex` adds OpenTelemetry spans
(no-op when tracing is off) and `NewInstrumentedIndex` records the
[metrics](../metrics/README.md).

### Redis Pod Cleanup

Redis stores each pod's request-key and encoded-field pairs in a `kvblock:pod:<podIdentifier>:entries` set. Each member is the decimal request key, a NUL byte, and the exact JSON-encoded hash field. `Add` and `Evict` maintain that set with the request-key hashes in Redis transactions. `Clear` scans the pod's set in batches, removes its fields across all tiers and groups, and leaves unrelated keys untouched. This adds a set member and a write command per indexed entry in exchange for cleanup cost that depends on the pod's entries.

Databases containing request-key hashes without reverse-index sets require migration before `Clear` can remove those entries. Stop all index writers, then either flush the dedicated KV index database or backfill the reverse-index sets before starting replicas that maintain them. Do not flush a database shared with unrelated application data. Writers that do not maintain the reverse index must not run alongside writers that do.

### Redis Benchmarks

The package benchmarks measure `Clear` with a fixed number of target entries and a growing unrelated keyspace, as well as the `Add` write path. They use miniredis unless `KV_CACHE_BENCH_REDIS_ADDR` points to a dedicated Redis database.

```bash
go test ./pkg/kvcache/kvblock -run '^$' -bench 'BenchmarkRedis(Clear|Add)$' -benchmem -benchtime=3x -count=5
```

For real Redis, set `KV_CACHE_BENCH_REDIS_ADDR=redis://127.0.0.1:<port>`. The benchmarks flush that database during setup and cleanup; it must contain no application data. Real Redis runs also report the dataset's `used_memory` increase.

## Grouped (HMA) Entries

`GroupCatalog` tracks group identity for heterogeneous-memory-aware (HMA)
models, where a block may be announced by a group of engines; eviction is
reference-counted at the group level so a block is removed only once no group
still references it.

## Key Types

| Symbol | Role |
|--------|------|
| `Index` | Block-key -> pods mapping (`Add` / `Lookup` / `Evict`). |
| `KeyWalker` / `EntryRef` | Optional ordered walk over requested keys; entries with pod and tier ordinals. |
| `TokenProcessor` | Tokens -> block keys; exposes `BlockSize`. |
| `BlockHash` | A single block key. |
| `PodEntry` | A pod holding a block, with its device tier. |
| `BlockExtraFeatures` / `ComputeBlockExtraFeatures` | Per-block multimodal metadata folded into the hash. |
| `PlaceholderRange` | Placeholder-token range for a multimodal item. |

## Related Documentation

- [KV-Cache Indexer](../README.md) -- reads this index to score pods
- [KV-Events](../../kvevents/README.md) -- writes to this index from engine events
