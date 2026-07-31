# SPEC — Phase 1: Ingestion (Wikipedia EventStreams → Messaging → Raw)

**Depends on:** `SPEC-agnostic-architecture.md` (`MessagingBackend`/`StorageBackend` contracts, principles P1–P7, conventions 9.1–9.4).
**Does not redecide anything already fixed there — only references it.**

---

## 1. Objective

Continuously consume Wikipedia's `recentchange` feed, publish each event to the configured messaging system, and persist it as partitioned raw files in the configured bucket — idempotently, reprocessably, and fully shuttable-down without leaving behind any lingering cost.

## 2. Scope

**In scope:**
- Connecting to and consuming the Wikipedia SSE (with automatic reconnection).
- Publishing to messaging (`MessagingBackend`).
- Consuming from messaging and writing raw files (`StorageBackend`).
- Event deduplication (SSE may redeliver on reconnect).
- Structured logging and basic count metrics.

**Out of scope (later phases):**
- Any Spark-based reading or Delta consolidation (Phase 2).
- Business schema validation or rejection rules (Phase 3).
- CI/CD (Phase 2.5).

## 3. Required handlers (convention 9.2)

| Handler | Responsibility | Implements |
|---|---|---|
| `WikiEventsHandler` | Connect to the SSE, parse the JSON payload line by line, reconnect with backoff on drop | — (source, has no backend contract) |
| `PubSubEmulatorHandler` | Publish/consume via Pub/Sub Emulator | `MessagingBackend` |
| `PubSubCloudHandler` | Publish/consume via real Pub/Sub | `MessagingBackend` |
| `S3CompatibleStorageHandler` | Write/read files against any S3-compatible endpoint — serves **both** `minio` and `s3` backends (Section 4.2 of `SPEC-agnostic-architecture.md`), differing only by whether `S3_ENDPOINT_URL` is set | `StorageBackend` |
| `GcsStorageHandler` | Write/read files in GCS | `StorageBackend` |

Selection factory (DRY — a single point of backend decision, never scattered through the code):

```python
# src/shared/backend_factory.py
def get_messaging_backend() -> MessagingBackend:
    backend = os.environ["MESSAGING_BACKEND"]  # "emulator" | "cloud"
    return {"emulator": PubSubEmulatorHandler, "cloud": PubSubCloudHandler}[backend]()

def get_storage_backend() -> StorageBackend:
    backend = os.environ["STORAGE_BACKEND"]  # "minio" | "s3" | "gcs"
    if backend in ("minio", "s3"):
        return S3CompatibleStorageHandler()  # reads S3_ENDPOINT_URL itself — set for minio, unset for s3
    return {"gcs": GcsStorageHandler}[backend]()
```

No other module should read `MESSAGING_BACKEND`/`STORAGE_BACKEND` directly — always through this factory (avoids duplicating selection logic, DRY principle).

### 3.1 Retrofit note (this phase was already implemented with only `minio`/`gcs`)

If `MinioStorageHandler` already exists as a standalone class from an earlier implementation pass, rename it to `S3CompatibleStorageHandler` and parametrize it with an optional endpoint rather than creating a third, separate class — the whole point of the `StorageBackend` interface (P2) is that this kind of addition should be small and additive, not a rewrite. Concretely: add `S3_ENDPOINT_URL` (optional) and `AWS_REGION` (required only when unset) to its constructor/config read, update `backend_factory.py` as above, add the new `.env.example` variables (Section 8), and add a test confirming `STORAGE_BACKEND=s3` with no endpoint set resolves to the real AWS S3 client config. Nothing in `producer/`, `consumer/`, or any downstream phase should need to change — they only ever call the `StorageBackend` interface.

## 4. Data flow

```
WikiEventsHandler (SSE)
        │  parse_recentchange_event(raw_line) -> dict
        ▼
Producer (src/producer/main.py)
        │  MessagingBackend.publish(topic="recentchange-raw", message=event)
        ▼
Messaging (Pub/Sub Emulator or Pub/Sub)
        │
        ▼
Consumer (src/consumer/main.py)
        │  MessagingBackend.subscribe(...) -> buffer -> periodic flush
        │  StorageBackend.write(df, path=partition_path, format="parquet")
        ▼
Bucket (MinIO or GCS) — raw/dt=YYYY-MM-DD/hour=HH/*.parquet
```

## 5. Raw format and partitioning

- **Format:** Parquet (not Delta). Delta is introduced starting in Phase 2 (bronze) — raw is a faithful dump with no need for a transaction log.
- **Partitioning:** `raw/dt={YYYY-MM-DD}/hour={HH}/part-{uuid}.parquet`, based on `meta.dt` (event time), not processing time — this prevents a late reconnection from writing into the wrong partition.
- **Consumer flush:** by buffer size (e.g., 500 messages) OR time (e.g., 60s), whichever comes first — avoids tiny files and avoids loss if the process is killed.

## 6. Data contract (raw)

Fields from the original event (Section 3 of the architecture SPEC) **plus** ingestion metadata added by the producer:

| Added field | Type | Description |
|---|---|---|
| `_event_id` | string | Original event `id` — deduplication key |
| `_ingested_at` | timestamp | Moment the producer received the event (processing time) |
| `_producer_instance` | string | Producer instance identifier (useful if running multiple replicas later) |
| `_extra_fields` | string (JSON) | Fields present in the original payload that aren't part of the known schema in `event_schema.py` — captures schema drift at the source, never dropped. Empty (`"{}"`) when there's no extra field. |
| `_schema_version` | string | Version of the set of known fields in `event_schema.py` at parse time — allows tracking when a field moved from "extra" to "official". |

`parse_recentchange_event()` (in `src/shared/event_schema.py`, reused by producer/consumer/bronze/silver — DRY) is the single place that decides what's a known field vs. `_extra_fields`. No other module reimplements this logic.

## 7. Idempotency and deduplication

- The SSE reconnects and may redeliver events already seen. The producer keeps a local cache (LRU, configurable size, e.g., the last 10,000 `_event_id`s) to discard obvious duplicates before publishing.
- Definitive (guaranteed) deduplication happens in the consumer, at write time: before flushing, it removes duplicates by `_event_id` within the current buffer.
- This is defense in depth, not a substitute for schema validation (which is Phase 3's responsibility).

## 8. Configuration (environment variables)

| Variable | Values | Required |
|---|---|---|
| `MESSAGING_BACKEND` | `emulator` \| `cloud` | Yes |
| `STORAGE_BACKEND` | `minio` \| `s3` \| `gcs` | Yes |
| `PUBSUB_PROJECT_ID` | string | Yes |
| `PUBSUB_EMULATOR_HOST` | host:port | Only if `emulator` |
| `BUCKET_NAME` | string | Yes |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | string | Only if `minio` or `s3` |
| `S3_ENDPOINT_URL` | URL | Only if `minio` (e.g., `http://localhost:9000`); leave unset for `s3` |
| `AWS_REGION` | string | Only if `s3` |
| `GOOGLE_APPLICATION_CREDENTIALS` | path | Only if `gcs` |
| `WIKI_STREAM_URL` | URL | No (default: official endpoint from Section 3) |
| `CONSUMER_FLUSH_SIZE` | int | No (default: 500) |
| `CONSUMER_FLUSH_INTERVAL_SECONDS` | int | No (default: 60) |

## 9. Logging

Every log event follows the structured format (architecture SPEC convention, principle P5), at minimum:

```json
{"timestamp": "...", "component": "producer|consumer", "level": "INFO|WARNING|ERROR", "event_count": 123, "message": "..."}
```

## 10. Code structure for this phase

```
src/
├── shared/
│   ├── backend_factory.py
│   ├── logger.py                    # structured JSON logging, reused by all phases
│   └── event_schema.py              # parse_recentchange_event(), reused by producer/consumer
├── handlers/
│   ├── wiki_events_handler.py       # WikiEventsHandler
│   ├── messaging/
│   │   ├── pubsub_emulator_handler.py
│   │   └── pubsub_cloud_handler.py
│   └── storage/
│       ├── s3_compatible_storage_handler.py   # serves both minio and s3 (Section 3.1)
│       └── gcs_storage_handler.py
├── producer/
│   └── main.py
└── consumer/
    └── main.py
```

## 11. Acceptance criteria (Given/When/Then)

1. **Ingestion without loss**
   Given the local stack running (`docker-compose up`), When the producer runs for 10 minutes consuming the real SSE, Then the number of messages published to the topic equals the number of events received from the SSE (net of reconnection duplicates, which must be logged as `WARNING`, not silently dropped).

2. **Correctly partitioned persistence**
   Given messages consumed from messaging, When the consumer flushes, Then the Parquet files appear under `raw/dt=.../hour=.../` matching each event's `meta.dt`, not the write time.

3. **Deduplication**
   Given a simulated SSE reconnection that redelivers the last 50 events, When the producer and consumer process that redelivery, Then no duplicate `_event_id` appears in the final raw files.

4. **Backend swap without code change**
   Given the producer/consumer code already implemented, When I switch `MESSAGING_BACKEND=emulator` to `MESSAGING_BACKEND=cloud` (with matching credentials), Then the pipeline works without any change to `src/producer/`, `src/consumer/`, or `src/shared/`.

4a. **Three-way storage swap without code change**
   Given the same producer/consumer code, When I set `STORAGE_BACKEND` to each of `minio`, `s3`, and `gcs` in turn (with matching credentials), Then the pipeline writes to the correct destination in all three cases, with no change to any source file — only `.env` differs.

5. **Clean shutdown**
   Given the pipeline running locally, When I run `docker-compose down`, Then no process, container, or orphaned volume remains active.

6. **Structured logging**
   Given any run of the producer or consumer, When I inspect stdout, Then every line is a parseable JSON following the Section 9 format.

## 12. Required tests

- `tests/test_wiki_events_handler.py`: parsing of a valid payload and a malformed payload (must not crash the handler, must log `WARNING` and continue).
- `tests/test_deduplication.py`: the `_event_id` cache correctly discards redeliveries.
- `tests/test_ingestion_smoke.py`: runs the local stack for a short period (via docker-compose in CI) and validates published count == persisted count.
- `tests/test_s3_compatible_storage_handler.py`: confirms `S3CompatibleStorageHandler` builds a MinIO-pointed client when `S3_ENDPOINT_URL` is set, and a real-AWS-pointed client when it's unset — without needing a real AWS account to run in CI (mock the client construction, not the network call).

## 13. Operating commands

```bash
# Bring up the local stack
docker compose -f local-stack/docker-compose.yml up -d

# Run the producer (time window set via env, or manual Ctrl+C)
python -m src.producer.main

# Run the consumer
python -m src.consumer.main

# Shut everything down (no leftover process/cost)
docker compose -f local-stack/docker-compose.yml down -v
```
