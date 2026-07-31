# Progress

**Current phase:** Phase 1 — Ingestion (`docs/SPEC-phase1-ingestion.md`), on branch `phase1-ingestion`.

**Implemented:** All components — interfaces (`MessagingBackend`/`StorageBackend`), `WikiEventsHandler`, `PubSubEmulatorHandler`/`PubSubCloudHandler`, `S3CompatibleStorageHandler` (serving `minio`/`s3`, retrofitted from the earlier `MinioStorageHandler`)/`GcsStorageHandler`, `backend_factory`, producer, consumer — plus all required tests. Verified end-to-end against the real local stack (Pub/Sub Emulator + MinIO), including a repeatability check (smoke test run twice back-to-back) that caught and fixed a column-order determinism bug in `event_schema.py`.

**Left:** Push `phase1-ingestion`, open a PR, merge to `main` (regular merge commit, per `docs/implementation-workflow.md`) — then start Phase 2 (`docs/SPEC-phase2-bronze.md`).
