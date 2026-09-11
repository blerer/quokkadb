# Concepts

QuokkaDB is an application database: it runs inside your application and persists its data in a local directory. This chapter will explain the concepts needed to use that model correctly without requiring storage-engine knowledge.

## Planned topics

- Documents, typed models, and the relationship between them.
- Collections as named groups of documents or models.
- `_id` fields and collection ID-creation strategies.
- Typed field expressions and BSON query documents.
- Indexes and how query and sort fields affect index choice.
- Write atomicity, concurrent access, and the absence of transactions.
- Durability choices, recovery, and what a synchronized write guarantees.
