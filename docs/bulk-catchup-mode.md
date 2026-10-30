# Feature Specification: Bulk Apply Catch-Up Mode for Oplog Replication

**Feature**: Bulk apply catch-up mode (`bulkApplyLagSeconds`)
**Created**: 2026-10-30
**Status**: Implemented
**Input**: "When the mongo connector falls behind, run in bulk_upserts instead of single mode like it does when running from a change date"

## Background

`OplogThread` tails `local.oplog.rs` and applies every relevant entry to each
doc manager serially: `upsert`, `update`, `remove`, `handle_command`, and
`insert_file` are invoked once per oplog entry. Each call costs roughly two
synchronous round-trips to the target system (e.g. a meta write plus the
document write). When the connector resumes from a stale checkpoint or falls
behind during a write burst, it must chew through the entire backlog at this
serial rate — the slowest possible mode exactly when throughput matters most.

`bulk_upsert` already exists on the doc-manager API but is only used by the
initial/resume collection dump, and in the Kibo Solr doc managers it is
specialized for one-shot reindexing (it exits the process when finished). This
feature adds a streaming bulk-apply path that engages automatically when the
oplog thread is far enough behind, without changing steady-state behavior.
Backlog entries are normalized into per-document operations and deduplicated
to the last op per document within each batch window, so hot documents cost
one write instead of one write per change.

Two related replication gaps in the existing serial path were discovered
while implementing and are addressed as part of this change:

1. **`applyOps` entries were silently dropped.** On MongoDB 5.x+, multi-document
   writes issued as a single command (`insert_many`, `update_many`,
   `delete_many`) are recorded in the oplog as a single `op: "c"` entry with
   `o.applyOps` holding the sub-operations. `handle_command` does not understand
   `applyOps`, so all batched writes were silently lost in both the serial and
   catch-up paths.
2. **`diff`-format update entries were mishandled.** On MongoDB 5.x+, modifier
   updates are recorded as `o: {"$v": N, "diff": {...}}` rather than
   `$set`/`$unset` specs. The catch-up path resolves modifier updates by
   fetching the current document from the source, which sidesteps spec format
   entirely. (The serial `update()` path in `mongo_doc_manager` still mishandles
   `diff` specs — it applies them via `findAndModify` as if they were
   replacement documents — and remains a known pre-existing issue outside this
   feature's scope.)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Fast Recovery From a Large Oplog Backlog (Priority: P1)

**Scenario**: As an operations engineer, I need the connector to catch up
quickly after it has fallen behind (restart, maintenance window, write storm),
so that the search index returns to freshness in minutes rather than hours.

**Why this priority**: This is the core value of the feature. Backlog replay is
currently bound by per-entry round-trips; batching collapses thousands of
round-trips into a handful of bulk writes.

**Independent Test**: Seed a replica set with thousands of writes, wait until
they are older than the configured lag threshold, start the connector against a
stale checkpoint, and verify the backlog is applied via `bulk_apply` in batches
and the target system converges to the source.

**Acceptance Scenarios**:

1. **Given** the connector is `bulkApplyLagSeconds` or more behind the newest
   oplog entry, **When** an insert/update/delete entry is processed, **Then**
   the entry is normalized into a buffered op and applied through each doc
   manager's `bulk_apply` method in batches of `bulkApplyBatchSize`
   deduplicated documents
2. **Given** multiple backlog entries touch the same document within a batch
   window, **When** the batch is flushed, **Then** only the last op for that
   document is applied (`i → u → d` applies one remove; `d → i` applies one
   upsert; `i → u → u` applies one upsert)
3. **Given** a batch window of `bulkApplyBatchSize` containing duplicate
   writes, **When** the deduplicated count is below the threshold, **Then**
   the buffer keeps filling until it holds `bulkApplyBatchSize` unique
   documents or a flush boundary is reached
4. **Given** a buffered batch has been applied successfully, **When** the flush
   completes, **Then** the oplog checkpoint advances to the timestamp of the
   last buffered oplog entry
5. **Given** the connector has caught up to within the lag threshold, **When**
   processing continues, **Then** remaining buffered ops are flushed first and
   subsequent entries resume serial per-entry application

---

### User Story 2 - Correct Replication of All Oplog Entry Types (Priority: P1)

**Scenario**: As a downstream consumer of the replicated data, I need catch-up
mode to produce the same final state as serial mode for every oplog entry
type, so that switching to bulk apply never changes correctness.

**Why this priority**: A speedup that changes semantics is a regression.
Modifier updates, deletes, replacements, commands, GridFS writes, and
`applyOps` batches must all converge to the same target state as before.

**Independent Test**: Generate a mixed backlog (inserts, `$set`/`diff` updates,
replacement updates, deletes, batched writes, collection commands) and verify
the target contains the same final documents as a serial-mode run against the
same oplog.

**Acceptance Scenarios**:

1. **Given** a modifier update entry (`$set`/`$unset` or `diff` format) in the
   backlog, **When** its batch is flushed, **Then** the current version of the
   document is fetched from the source and upserted
2. **Given** a modifier update whose document no longer exists in the source,
   **When** its batch is flushed, **Then** a remove op is applied instead
3. **Given** a command entry (drop, rename, create, dropDatabase) or a GridFS
   file entry in the backlog, **When** it is reached, **Then** buffered ops are
   flushed first and the command/file op is applied serially, preserving order
4. **Given** an `applyOps` entry containing only CRUD sub-operations, **When**
   it is processed in either mode, **Then** each sub-operation is applied as if
   it were a normal oplog entry at the parent entry's timestamp
5. **Given** an `applyOps` entry containing a non-CRUD sub-operation, **When**
   it is processed, **Then** the parent command entry is handled serially by
   `handle_command` unchanged

---

### User Story 3 - Configurable Threshold With Opt-Out (Priority: P2)

**Scenario**: As a platform administrator, I need the lag threshold to be
configurable per deployment with a way to turn the mode off entirely, so
that a problematic rollout can be reversed with a config change.

**Why this priority**: Safe rollout. The mode is enabled by default at a
conservative 5-minute threshold, so it only engages during meaningful
backlog events; setting it to 0 restores exact pre-feature behavior, and
doc managers without a `bulk_apply` override must still work correctly.

**Independent Test**: Run the connector with `bulkApplyLagSeconds` set to 0
and verify byte-for-byte identical behavior to the previous release; then
verify a doc manager using only the base-class `bulk_apply` (serial
fallback) still receives all ops correctly.

**Acceptance Scenarios**:

1. **Given** `bulkApplyLagSeconds` is explicitly set to 0, **When** any
   oplog entry is processed, **Then** it is applied serially exactly as
   before and `bulk_apply` is never invoked
2. **Given** a doc manager that does not override `bulk_apply`, **When**
   catch-up mode is enabled, **Then** the base-class implementation replays
   ops serially through `upsert`/`remove` and the result is correct
3. **Given** `bulk_apply` raises an exception for a batch, **When** the flush
   fails, **Then** the affected doc manager replays that batch's ops serially
   with the same per-op error handling as the serial path

---

### User Story 4 - Batched Writes to Solr Targets (Priority: P2)

**Scenario**: As the owner of the order and shipment search indexes, I need
catch-up mode to batch `solr.add` calls per core, so that Solr indexing
throughput during backlog replay improves materially.

**Why this priority**: This is where the realized speedup lands for Kibo's
custom doc managers — single-doc `solr.add` calls are the dominant cost today.

**Independent Test**: Enable catch-up mode with the order doc manager pointed
at Solr, generate a backlog, and verify batches of cleaned docs are sent to the
correct core per namespace while removes keep their existing semantics.

**Acceptance Scenarios**:

1. **Given** the order doc manager, **When** catch-up upserts arrive for the
   order and B2B cores, **Then** cleaned docs are batched per Solr core at
   `bulkSize` and sent with the same `connector_update` handler and
   commitWithin settings as `bulk_upsert`
2. **Given** a remove op for the order doc manager, **When** it is processed,
   **Then** it is logged and skipped, mirroring the existing `remove`
   behavior
3. **Given** the shipment doc manager, **When** a mixed stream of upserts and
   removes arrives, **Then** consecutive upserts flush via batched
   `solr.add` and consecutive removes via batched `solr.delete`, preserving
   oplog order

---

### Edge Cases

- What happens when the bulk buffer is partially applied and the doc manager
  throws mid-batch? (Checkpoint must not advance past unapplied entries; the
  serial fallback replays the whole chunk, which is idempotent.)
- What happens when the tailable cursor dies (AutoReconnect/OperationFailure)
  with unflushed buffered ops? (Buffer is discarded; checkpoint is behind, so
  the entries are re-read on reconnect.)
- What happens when a document is updated and deleted within the same batch
  window? (Buffer dedup keeps only the last op per document — `i → u → d`
  collapses to a single remove, `d → i` collapses to a single upsert. For ops
  spanning different flush windows, per-collection ordering in
  `bulk_write(ordered=True)` and per-core op ordering preserve
  last-op-wins.)
- Can dedup lose information? (No — every normalized op carries the
  document's complete final state: upserts carry the full document, modifier
  updates resolve to the current source document at flush time, and removes
  are idempotent. Only the document's *latest* op matters for the target's
  final state.)
- What happens when a fetched document was updated again after its entry but
  before the flush? (The newer state is upserted early; the later entry
  converges identically — replication is convergent, not exact-replay.)
- What happens when a namespace is remapped? (Fetches unmap the destination
  namespace back to the source namespace and apply the configured projection,
  matching dump behavior.)
- What happens on a sharded source? (Fetches go through `mongos_client` when
  present, matching rollback behavior.)
- What happens to skipped entries between buffered ops? (They only update the
  trailing timestamp used for checkpoint bookkeeping; they produce no ops.)
- What happens when the backlog is smaller than `bulkApplyBatchSize` and the
  source goes quiet? (The tailable cursor blocks awaiting new data, so the
  batch would never fill. `bulkApplyIdleSeconds` — default 60 — bounds how
  long buffered ops may sit unflushed; the lag-logger thread checks buffer
  age every 30s and flushes when it exceeds the threshold.)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: When the connector is more than `bulkApplyLagSeconds` behind the
  newest oplog entry, CRUD entries MUST be buffered and applied through each
  doc manager's `bulk_apply` method in batches of at most
  `bulkApplyBatchSize` deduplicated documents
- **FR-002**: `bulkApplyLagSeconds` MUST default to 300 seconds (5
  minutes); setting it to 0 MUST disable catch-up mode entirely. Both
  options MUST be exposed as `--bulk-apply-lag-seconds` and
  `--bulk-apply-batch-size` CLI flags and JSON config keys
- **FR-003**: Buffered ops MUST preserve oplog order end-to-end: a command or
  GridFS entry MUST flush pending buffered ops before being applied serially
- **FR-003a**: The buffer MUST deduplicate ops per document, keeping only the
  last op for each `(namespace, _id)` within a flush window. Every normalized
  op carries the document's complete state (or resolves to it at flush time),
  so intermediate ops for the same document are redundant. The
  `bulkApplyBatchSize` threshold applies to the count of deduplicated
  documents, so a window containing duplicates keeps filling until it holds
  that many unique documents
- **FR-004**: The checkpoint MUST only advance past entries whose ops were
  successfully flushed or applied
- **FR-005**: Modifier update entries (`$set`/`$unset` and MongoDB 5.x `diff`
  formats) MUST be resolved to the document's current source state via
  batched `$in` fetches honoring the namespace projection; documents missing
  from the source MUST become remove ops
- **FR-006**: Replacement-style update entries MUST upsert `o` with `_id`
  taken from `o2` and `$v` removed
- **FR-007**: `applyOps` command entries whose sub-operations are all CRUD ops
  MUST be expanded and processed as normal entries at the parent timestamp,
  in both serial and catch-up modes; any other `applyOps` entry MUST fall
  back to serial command handling
- **FR-008**: `DocManagerBase.bulk_apply(ops)` MUST exist with a serial
  fallback over `upsert`/`remove` so existing doc managers work unmodified;
  the op tuple format MUST be `(op_type, namespace, payload, timestamp)` with
  `op_type` in `{"upsert", "remove"}`
- **FR-009**: On `bulk_apply` failure the affected doc manager MUST replay
  that chunk serially with per-op `OperationFailed`/`ConnectionFailed`
  handling identical to the serial path
- **FR-010**: Catch-up mode MUST NOT invoke `bulk_upsert`, which retains its
  one-shot dump semantics (including process exit in the Solr doc managers)
- **FR-011**: Unflushed buffered ops MUST be discarded on cursor exceptions;
  they are re-read from the checkpoint after reconnect
- **FR-011a**: Buffered ops MUST be flushed when they have been pending at
  least `bulkApplyIdleSeconds` (default 60; 0 disables), even if the batch
  never fills and no further entries arrive — the check is driven by the
  replication lag thread since the tailable cursor blocks at the tail.
  Buffer mutation, flush, and serial application MUST be serialized by a
  shared lock so a timer-triggered flush cannot interleave with or reorder
  application of newer entries
- **FR-012**: `mongo_doc_manager.bulk_apply` MUST group writes by destination
  collection and meta collection and issue ordered `bulk_write` calls chunked
  at `chunk_size`, preserving per-collection op order
- **FR-013**: The Solr doc managers MUST batch upserts per Solr core with the
  same clean-doc, commitWithin, handler, and B2B-core-unavailable semantics
  as `bulk_upsert`, without the reindex process exit
- **FR-014**: Remove ops MUST keep each doc manager's existing delete
  semantics (order DM: log-and-skip; shipment DM: batched `solr.delete` by
  mapped id; mongo DMs: ordered `DeleteOne` on data and meta collections)

### Key Entities

- **Normalized Op**: `(op_type, namespace, payload, timestamp)` tuple produced
  by `OplogThread` during catch-up; `op_type` is `"upsert"` (payload is a
  full document) or `"remove"` (payload is a document `_id`)
- **Catch-Up Buffer**: Map of `(namespace, _id)` to normalized ops held by
  `OplogThread` between flushes; only the last op per document survives a
  flush window, and `fetch` entries are placeholders for modifier updates
  resolved at flush time
- **Checkpoint**: BSON timestamp of the last fully-applied oplog entry per
  replica set, stored in the oplog progress dictionary and the oplog file
- **Meta Document**: Per-document bookkeeping written by the mongo doc
  managers (`_id`/`doc_id`, `ns`, `_ts`) in the `__mongo_connector` database;
  stamped with each surviving op's own oplog timestamp, not the batch
  timestamp

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: With catch-up enabled, a backlog of N CRUD entries touching U
  unique documents (U ≤ N) is applied with O(U / bulkApplyBatchSize) bulk
  calls instead of ~2N round-trips, plus at most one `$in` fetch query per
  touched source namespace per batch for modifier updates
- **SC-002**: Given identical oplog input, catch-up mode produces the same
  final target state as serial mode for all supported entry types
- **SC-003**: With catch-up disabled (`bulkApplyLagSeconds: 0`), behavior is
  identical to the previous release; `bulk_apply` is never invoked. When
  the lag never exceeds the 5-minute default, no ops take the bulk path
- **SC-004**: Batched writes (`insert_many`, `update_many`, `delete_many`)
  replicate correctly on MongoDB 5.x+ where they were previously dropped
- **SC-005**: Checkpoint advances only past flushed entries; a crash or
  cursor failure mid-batch results in re-application, never skipped entries
- **SC-006**: Existing doc managers and doc managers lacking a `bulk_apply`
  override continue to work without modification

## Out of Scope / Known Issues

- Serial-path handling of `diff`-format update specs in
  `mongo_doc_manager.update()` (applies `{"diff": ...}` as a document
  replacement, corrupting the target doc). Pre-existing; affects both modes
  equally except that catch-up mode resolves updates via source fetches and
  is unaffected.
- `applyOps` entries representing transactions (with `lsid`/`txnNumber`) are
  expanded the same as batched writes; the connector has never had
  transaction semantics.
- `filter_oplog_entry` treats a `diff` update payload as a replacement doc
  when include/exclude fields are configured (pre-existing).
- Non-CRUD sub-operations inside `applyOps` fall back to serial
  `handle_command`, which does not apply them.

## Assumptions

- The source MongoDB is a replica set (or sharded cluster) whose oplog is
  tailable; lag is measured in seconds between an entry's `ts` and the newest
  oplog `ts`
- Fetching current documents from the source during catch-up is acceptable
  read load; it is bounded to one `$in` query per touched namespace per batch
- Convergent application (applying a document's current source state) is
  equivalent-or-better than exact replay for this system's targets
- The Kibo doc managers (`mozu_order_solr_doc_manager`,
  `mozu_tenant_filtered_doc_manager`, `kibo_shipment_solr_doc_manager`) are
  deployed alongside this connector version
- `bulk_apply` is a new optional doc-manager API; older connectors simply
  never call it, and older doc managers fall back to the serial default
- A 5-minute default threshold is conservative enough that steady-state
  replication (typically seconds of lag) never engages the bulk path; it
  only activates during genuine backlog events (restarts, outages, write
  storms)
