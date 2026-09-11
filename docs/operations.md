# Operations

QuokkaDB runs with the application that opens it. Operational documentation will focus on choosing and protecting the database directory, configuring resource use and durability, and reading the signals the application can expose.

## Planned topics

- Choosing a durable database directory and file-system permissions.
- Opening with `Options` and choosing a built-in configuration profile.
- Write-ahead-log durability and per-write `sync()` overrides.
- Storage, cache, compression, and compaction settings.
- Reading `Metrics` and integrating tracing output.
- Restart and recovery behavior.
- Backup, restore, and upgrade guidance as supported procedures are defined.
