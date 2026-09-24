# Background index building

This page records the contract for the planned background index builder. The
builder is not implemented yet; the current implementation does not populate
an index from documents that already exist.

When the builder is implemented, `create_index()` will register the index and
return its name without waiting for the initial population scan to finish.
The index will remain unavailable to query planning until the scan completes.
Queries will continue using their existing access paths while the build is in
progress.

Writes made during the build will maintain the index through the normal
document write path. This lets the builder process a snapshot of existing
documents while later inserts, updates, and deletes keep the index current.

The build will publish the index only after all existing documents have been
processed. Publication will invalidate cached query plans for the collection.
Until publication, the index definition may be visible through index metadata,
but the index must not be selected for reads.

The build state will be persisted in a reserved internal collection. A
checkpoint will contain the last processed primary key, allowing the worker to
resume after a restart. The checkpoint and each batch of index entries will be
written atomically.

Collection ID `0` is reserved for this state collection. IDs `1` through `9`
remain reserved for future internal collections, and user-created collections
start at ID `10`. Internal collections are addressed directly by storage code
and do not appear in the public catalog.

Background build failures will not be returned by the already completed
`create_index()` call. They will be recorded as build state and exposed through
the planned index-build status API. Transient write conflicts will be retried;
permanent failures will leave the index unavailable until they are retried or
removed.
