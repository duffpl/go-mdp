# v2.6.0

This release fixes dump-processing hangs and incomplete or incorrect anonymization, adds bounded input buffering, and exposes library progress without writing logs or SQL to stdout.

- Cancel and join pipeline work on failure; interrupt closable streams and never emit the original INSERT when its transformation fails.
- Preserve statement boundaries and trailing SQL, honor explicit INSERT columns and database-specific schema order, and count rows correctly when strings contain tuple separators.
- Resolve nested and JSON template dependencies transitively, including column variables evaluated per JSON field or array element.
- Add concurrency-safe `Processor.Progress()` snapshots with byte/statement counters, worker activity, wait durations, and failure location.
- Reuse faker random generators without changing deterministic mappings, and reduce extended-INSERT buffer allocation and quoted-literal scanning costs.
- Add Linux amd64 and arm64 release archives, plus checksums, built and tested with Go 1.24.13.

## Upgrade notes

- The default in-flight raw SQL budget is **4 MiB**; the maximum input statement is **64 MiB**. Set `settings.maxInFlightBytes`, `settings.maxStatementBytes`, and `settings.workers` for your workload. A single statement exceeding the in-flight budget runs alone. These settings do not cap total process memory.
- Default settings prioritize memory over maximum concurrency. On the representative 154 MiB dump, tuning to ten workers / 16 MiB brought elapsed time to 2.25s versus 2.07s for v2.5.0, while reducing median peak RSS from 932 to 390 MiB. These measurements are workload-specific; this release does not claim a universal speedup.
- Configuration construction now rejects statically missing variable references, dependency cycles (including unused variables), later-scope variable dependencies, and recursive named-template invocations. Whole-map or computed-key accesses conservatively require all entries in that map. See README for dependency rules.
- On success, library streams remain open. On cancellation/failure, closable streams are interrupted and partial output must be discarded. Non-closable blocked I/O and indefinitely blocking custom template functions still need caller-provided interruption.
- SQL mode changes affecting quote interpretation, such as `NO_BACKSLASH_ESCAPES` and `ANSI_QUOTES`, remain unsupported.

Library integration:

```sh
go get github.com/duffpl/go-mdp/v2@v2.6.0
```

The progress API reports bytes accepted by the supplied writer, not downstream flush or database-import completion. Host services should poll and log snapshots separately from SQL output.
