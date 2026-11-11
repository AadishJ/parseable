# Asynchronous stream deletion

`DELETE /logstream/{name}` returns `202 Accepted` after recording a durable tombstone and scheduling deletion. Acceptance does not mean deletion has completed.

The tombstone records a unique deletion ID and the IDs of the ingestors registered when deletion starts. Before requesting acknowledgements, the background worker seals that participant set by adding any ingestors registered between the initial snapshot and the durable marker; IDs are never dropped. Later-starting ingestors encounter the marker during startup migration. The background worker performs an initial recursive object-store deletion without waiting for acknowledgements, retaining the tombstone. It then contacts every recorded participant, including unreachable nodes; a liveness failure is not an acknowledgement. Each ingestor validates the deletion ID, blocks new ingestion and uploads for the stream, drains participating ingestion, local conversion and object-store uploads, removes staging and resets statistics. It acknowledges only after those steps succeed. A matching deletion-ID response is required, so an older ingestor's generic success response is insufficient.

Only after all participants acknowledge does the worker perform a second recursive object-store deletion to remove uploads that arrived during or after the initial pass. It then clears local state and removes the tombstone. The second pass may be redundant, but is required before permitting name reuse. Failed requests, either deletion pass failing, cleanup errors, missing registrations and unavailable nodes leave deletion pending. Reconciliation, restart recovery and repeated DELETE requests retry both passes and contact the participants again rather than trust cached acknowledgements.

Ingestors register before startup migration. Migration checks outstanding tombstones before loading streams, and startup upload plans use only loaded streams. Storage errors during migration fail startup rather than authorize writes to a tombstoned stream.

## Operational limitations

- An offline ingestor delays final deletion and name reuse until it can acknowledge. Other streams can continue to operate.
- **Removing a required ingestor's registration does not remove its ID from existing tombstones.** Those deletions can remain pending indefinitely. Automatic safe node retirement/fencing is not implemented. Restore the required registration and allow the ingestor to complete cleanup; do not manually remove the tombstone or participant ID unless the old writer has been reliably fenced and its stale staging handled.
- An unreachable node is not necessarily stopped. Deleting its registration alone does not revoke its storage credentials or prevent it from returning.
- Upgrade participating ingestors before expecting deletion to finish: nodes that do not implement the deletion-ID acknowledgement protocol cannot satisfy the barrier.
- Legacy `null` tombstones are upgraded with a fresh deletion ID and the registrations available when recovery runs. Registrations removed before that upgrade cannot be reconstructed.
- Registering a different node ID does not prove its staging is clean. Do not introduce stale staging under a new identity; safe membership retirement and fencing are not implemented.
- This barrier covers recorded ingestors using the protocol. It is not storage-level fencing against unregistered/older writers, direct object-store writes, or independently operating deletion owners. Cross-owner deletion claiming and safe retirement remain separate concerns.
