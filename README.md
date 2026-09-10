# go-mdP – MySQL Dump Processor

Stream-process MySQL dumps to anonymize or transform data using fast, declarative templates. Pipe `mysqldump` directly into go-mdP or run it on a dump file; it parses CREATE TABLE and INSERT statements, applies your rules, and writes a sanitized dump.

Inspired by:
- https://github.com/DivanteLtd/anonymizer
- https://github.com/humanmade/go-anonymize-mysqldump


## Key features
- Streaming: processes statements as they arrive; suitable for large dumps
- Parallel: uses all CPU cores when transforming INSERTs
- Powerful templating: Go text/template + Sprig + built-in helpers (md5, bcrypt, argon2) + locale-aware fake data helpers
- Variable system: global, table, row, and column-scoped variables with dependency resolution
- Deterministic fake data per input value (same input → same fake output), useful for consistency


## Installation
- With Go 1.24+:
  go install github.com/duffpl/go-mdp/v2@latest
- Or build from source in this repo:
  ./build.sh

The resulting binary is called `go-mdp`.


## When should I use this?
- You need to share production-like data without exposing PII
- You want reproducible, rule-based transformation at dump time
- You prefer streaming (no full DB restore required) and speed


## Important requirements and limitations
- Your dump must include CREATE TABLE statements for every table you want to transform. The processor needs table schemas to map column indexes to names. If a table’s CREATE TABLE was not seen before its INSERT statements, processing that table will fail.
  Tip: mysqldump normally includes schema if you don’t use `--no-create-info`.
- The tool only parses and transforms:
  - CREATE TABLE (to capture schema)
  - INSERT (to transform values)
  Other statements are passed through unchanged.
- Only value literals inside INSERT are transformed; expressions are not supported as sources. Configured tables require explicit CREATE column definitions; CREATE TABLE ... LIKE/AS SELECT and INSERT ... SELECT are rejected.
- Parsing uses default MySQL quoting rules. SQL mode changes that alter quoting, such as NO_BACKSLASH_ESCAPES or ANSI_QUOTES, are not interpreted.


## Quick start
1) Create a config file (JSON) describing transformations. Example:

{
  "settings": { "locale": "default" },
  "skipTables": ["audit_log", "job_queue"],
  "globalVariables": {
    "domain": "example.com"
  },
  "tables": [
    {
      "name": "member",
      "rowVariables": {
        "counter": "{{ .RowMeta.Index }}"
      },
      "columns": [
        {
          "name": "email",
          "transformations": [
            { "template": "user-{{ .Row.id }}@{{ .GlobalVariables.domain }}" }
          ]
        },
        {
          "name": "first_name",
          "transformations": [
            { "template": "{{ transformFirstName .FieldValue }}" }
          ]
        },
        {
          "name": "last_name",
          "transformations": [
            { "template": "{{ transformLastName .FieldValue }}" }
          ]
        },
        {
          "name": "password",
          "transformations": [
            { "template": "{{ bcryptHash \"changeme\" }}" }
          ]
        }
      ]
    }
  ]
}

2) Run with a dump file:
- Pipe from mysqldump
  mysqldump --databases mydb | go-mdp -c config.json > sanitized.sql
- Or on a file
  go-mdp -i dump.sql -o sanitized.sql -c config.json

3) Restore `sanitized.sql` wherever you need it.


## CLI usage
Basic flags:
- -i, --input string       Input SQL file. If empty, stdin is used
- -o, --output string      Output SQL file. If empty, stdout is used
- -c, --config string      Path to JSON config (default: config.json)
- -f, --config-data string Base64-encoded JSON config content (alternative to -c)
- -z, --config-zipped      If set with -f, the base64 content is gzipped

Example: inlining a compressed config
- create config.json then gzip+base64:
  gzip -c config.json | base64 > cfg.b64
- run:
  go-mdp -f "$(cat cfg.b64)" -z < dump.sql > sanitized.sql


## Configuration reference (JSON)
Top-level:
- settings.locale: locale for fake data helpers; one of: default, fi, se, no, dk
- settings.workers: concurrent INSERT workers; defaults to `runtime.GOMAXPROCS(0)` when omitted or zero. Set a smaller value for memory-constrained or CPU-limited environments.
- settings.maxInFlightBytes: budget for raw SQL awaiting output, including active transformations; default 4194304 (4 MiB). A single statement larger than this budget runs alone. This is not a process memory limit: ASTs, transformed output, and the next statement being read require additional memory.
- settings.maxStatementBytes: maximum input statement size; default 67108864 (64 MiB). Larger statements fail with an error. Raise this explicitly for dumps with very large extended INSERTs or blobs. Zero selects the default; negative limits are rejected.
- skipTables: array of table names whose INSERT statements should be dropped (CREATE TABLE is preserved)
- globalVariables: name -> template string evaluated once globally (can reference previously defined global variables)
- tableVariables: name -> template string evaluated per-table (can reference global + already defined table vars)
- rowVariables: name -> template string evaluated per row (has access to row data and counters)
- columnVariables: name -> template string evaluated per column before the column’s main template(s)
- postSql: raw SQL string appended to the very end of the output
- tables: array of table configs

Table config:
- name: table name to match in INSERT/CREATE TABLE
- columns: array of column configs
- rowVariables / columnVariables / tableVariables: optional overrides/additions for this table

Column config:
- name: column name
- transformations: array of transformation objects (two formats supported):

  Flat format (simpler):
    { "template": "{{ .FieldValue }}@example.com" }
    { "json": [ { "path": "user.name", "template": "anon" } ] }

  Nested format:
    { "type": "template", "options": { "template": "{{ .FieldValue }}@example.com" } }
    { "type": "json", "options": { "fields": [ { "path": "user.name", "template": "anon" } ] } }

  Both formats can be mixed in the same config. When type is omitted, it is inferred from the key ("template" or "json").

  Transformation types:
  - template: Go template string; output replaces the column value
  - json: transform fields inside a JSON column value
    - options.fields: array of { "path": "json.path", "template": "..." }
    - path uses GJSON syntax (e.g. "user.name", "contacts.#.email")


## Template data model
In any template you can use these fields:
- .Row: map[string]any of the current INSERT row by column name; original literal values
- .RowMeta.Index: 1-based counter of rows processed for this table
- .FieldValue: the original value of the current field (string when coming from SQL literal)
- .GlobalVariables: map[string]string rendered from config.globalVariables
- .TableVariables: map[string]string rendered per table
- .RowVariables: map[string]string rendered per row so far
- .ColumnVariables: map[string]string rendered per column (for templates that declare and consume them)

Dependency-aware variables
- Variables (global/table/row/column) are themselves templates and may reference previously defined variables. The engine resolves dependencies and evaluates them in order.


## Template functions available
From Sprig (https://masterminds.github.io/sprig/): string, math, date, list helpers, etc.
Built-in helpers:
- md5 string -> string           Simple MD5 hex
- bcryptHash string -> string    Generate bcrypt hash
- argon2Hash string -> string    Generate Argon2i hash
From faker (locale-aware, deterministic):
- transformFirstName string -> string
- transformLastName string -> string
- transformFullName string -> string
- transformStreet string -> string
- transformCity string -> string
- transformCompanyName string -> string

Tip: You can combine Sprig and faker functions, e.g.:
- {{ lower (transformCity .FieldValue) }}
- {{ printf "%s.%s@%s" (lower (first .Row.first_name)) (lower .Row.last_name) .GlobalVariables.domain }}


## How it works
- The processor reads SQL statements while respecting quoted strings, escaped quotes, comments, and MySQL `DELIMITER` directives. Multiple statements on one line and the final EOF fragment are preserved.
- CREATE TABLE statements are parsed in input order. Each INSERT receives the schema for its database/table at that point, and explicit INSERT column lists determine value order.
- Only tables listed in your config are transformed
- For each INSERT row in those tables:
  1) Render row variables (if any)
  2) For each configured column, render column variables (if any) and then apply its template(s)
  3) Replace the original column value with the rendered result
- The final INSERT is re-serialized and written; non-target statements pass through unchanged
- After completion, postSql (if set) is appended

Performance notes
- Raw SQL in flight is bounded by `settings.maxInFlightBytes`; memory also depends on statement size, ASTs, templates, and worker count.
- Multiple workers parse and transform INSERTs concurrently; output remains in input order.
- Password hashes are intentionally expensive. If all anonymized rows should share one replacement password hash, compute it in `globalVariables` and reference that variable from column templates rather than hashing it for every row. Hash salts and faker mappings are otherwise unchanged.
- The CLI buffers output in 64 KiB chunks and checks the final flush. Diagnostics are returned as errors; a failed transformation never emits the original INSERT. Any output from a failed run is partial and should not be imported as a completed dump.

Template dependency validation

`NewProcessor` resolves transitive variable dependencies in evaluation order,
including references in `if`/`else`, `range`, `with`, nested pipelines, scoped
aliases, literal `index` lookups, and invoked named templates. JSON field
templates participate in the same graph. Their column variables are rendered
for each target field or array element, with `.FieldValue` set to that value.

Construction rejects statically undefined variable references, dependency cycles
(including unused configured variables), and dependencies on later variable
scopes. Global variables can depend on globals; table variables on globals and
table variables; row variables additionally on rows' variables; column variables
on all four variable scopes. A template in a conditional branch still contributes
dependencies even if the branch will not execute for a particular row.

Whole-map reads, computed `index` keys, and reassignment of map aliases are
conservative: they require every configured entry in the referenced map. This
can introduce a cycle when a variable reads its own map; prefer explicit keys
or aliases declared with `:=` in that case. Computed keys themselves cannot be
validated statically. Recursive named-template invocations are rejected during
construction. Dependency discovery does not inspect custom functions' internals.

Tuning library workloads

Tune `workers` and `maxInFlightBytes` together using a representative dump. A small
byte budget can leave workers idle when extended INSERTs occupy most of it. If
`Progress()` repeatedly shows the reader in `waiting_capacity` with idle workers,
try a larger budget and measure both elapsed time and process peak memory. If the
writer spends time in `writing_output`, also check the destination's throughput.
These snapshots describe current activity; one sample does not establish a bottleneck.

For example, set these fields on the loaded config before calling `NewProcessor`:

```go
cfg.Settings.Workers = 8
cfg.Settings.MaxInFlightBytes = 8 << 20 // 8 MiB of raw SQL awaiting output
```

On a 154 MiB anonymization fixture with Go 1.26.5 and a 10-CPU Apple M2 Pro,
five-run medians were:

| Workers | In-flight budget | Elapsed | Process peak RSS |
| --- | --- | --- | --- |
| 10 (default on that host) | 4 MiB (default) | 2.81 s | 238 MiB |
| 8 | 8 MiB | 2.43 s | 309 MiB |
| 10 | 16 MiB | 2.25 s | 390 MiB |

These are workload-specific examples, not automatic presets or memory limits.
Observed peak RSS reached 370 MiB for 8 workers / 8 MiB and 427 MiB for
10 workers / 16 MiB. The measurements cover one process reading decompressed SQL
and discarding output; budget separately for compression, database piping, and
concurrent jobs in a host service. Fewer workers can be useful with less CPU or
memory available. Defaults remain unchanged.

Library cancellation
- `Processor.Process` leaves streams open on success. On error or context cancellation, it closes streams implementing `io.Closer` to interrupt blocked I/O. An output wrapper may implement `Abort()` to close its underlying stream without flushing buffered data.
- Non-closable readers/writers that can block must be interrupted by their caller, for example via connection deadlines. Cancelling a context cannot interrupt arbitrary `Read`, `Write`, or template functions by itself.
- `Process` waits for pipeline goroutines to finish before returning. Call `ProcessedTables()` after completion. Use separate processors for concurrent streams.
- The CLI handles interrupt/termination signals, closes its streams on cancellation, and rejects input/output paths referring to the same file.


## Examples
- Simple email rewrite:
  {
    "tables": [
      {
        "name": "users",
        "columns": [
          { "name": "email", "transformations": [ { "template": "user-{{ .Row.id }}@example.com" } ] }
        ]
      }
    ]
  }

- Name anonymization per locale:
  Use settings.locale: fi | se | no | dk | default
  { "settings": { "locale": "se" }, ... }


## Troubleshooting
- Error: cannot parse statement for table X …
  Your statement may be too complex or malformed; ensure it’s a standard CREATE TABLE/INSERT from mysqldump.
- Panic or error about missing schema
  Ensure CREATE TABLE for the table appears before INSERTs in the stream. Don’t use --no-create-info.
- Strange output encoding or quoting
  Values are re-serialized using TiDB/pingcap formatter with single quotes and back-quoted identifiers.


## Library progress and logging

The library does not write logs or call logging callbacks. Poll `p.Progress()`
from your application's monitoring goroutine while `p.Process(...)` runs.
Snapshots remain available during blocked I/O or synchronous template calls;
they do not take the lock held for the duration of `Process`.

```go
snapshot := p.Progress()
// Pass these to your existing structured logger or metrics system:
// snapshot.State, snapshot.Elapsed, snapshot.BytesRead, snapshot.BytesWritten
// snapshot.Reader, snapshot.Writer, snapshot.Workers, snapshot.LastTable
// snapshot.StatementsCompleted, snapshot.RowsTransformed
// snapshot.InFlightBytes, snapshot.InFlightLimit, snapshot.Failure
```

Snapshot states are `idle`, `running`, `stopping`, `completed`, `failed`, and
`canceled`. Each accepted `Process` call resets the counters; after it returns,
the final snapshot and elapsed time remain available. Worker entries are indexed
by worker ID and identify their table, statement, row range, and stage duration.

| Activity | Meaning |
| --- | --- |
| Reader `reading_input` | A read from the supplied reader is active |
| Reader `framing` / `dispatching` / `parsing_schema` | Reading SQL boundaries or preparing a statement |
| Reader `waiting_capacity` / `waiting_queue` / `waiting_worker` | Waiting for downstream capacity |
| Worker `parsing_insert` / `transforming` | Parsing or transforming the indicated INSERT |
| Writer `waiting_statement` / `waiting_result` | Waiting for input or the next ordered result |
| Writer `writing_output` | A write to the supplied writer is active |
| `idle` / `done` | Worker is available or the stage has finished |

`Activity.Elapsed` is the time in the current stage, not cumulative CPU time.
An active read/write is not proof of a stall: the host may be decompressing,
filtering, buffering, or sending data inside that call. `LastTable` is the last
identified table; a statement still being read may not have a known table yet.

Use differences between sampled byte counts and elapsed times for throughput.
`LastReadAt` and `LastWriteAt` remain zero until bytes have been read or a write
has returned with accepted bytes. Statement counters count framed units,
including passthrough SQL and trailing comments. `StatementsSkipped` counts
INSERTs omitted by `skipTables`. `RowsTransformed` counts rows in successfully
transformed INSERTs; it excludes passthrough/skipped rows and can include rows
whose subsequent output write fails.

Input counts start at the reader supplied to go-mdp, after any upstream filters
or decompression. Output counts mean bytes accepted by the supplied writer;
`completed` does not mean buffers were flushed or a database finished importing.
The host must report those later phases separately. `InFlightBytes` is the
charged raw SQL budget, not heap usage or RSS. Collect process memory metrics
in the host and label them as process-wide when several jobs run concurrently.

Snapshots contain identifiers and counters, not SQL text, row values, template
contents, or error messages. `Failure` preserves the first failing activity;
the returned processing error remains available separately and may contain
parser/template details unsuitable for routine logs. Choose the fields your
application should emit. Run logging separately from SQL processing and avoid
waiting for a blocked logging destination before closing SQL/compression streams.

## Benchmarking

Benchmark a local, decompressed SQL dump with its JSON anonymization config:

```sh
GO_MDP_BENCH_INPUT=/path/to/dump.sql \
GO_MDP_BENCH_CONFIG=/path/to/config.json \
go test ./processor -run '^$' -bench '^BenchmarkProcessor_ExternalDump$' -benchtime=1x -count=3 -benchmem
```

The benchmark includes file reads and processor initialization, discards output,
and reports total allocated bytes rather than peak resident memory. It skips
unless both paths are set. Keep private dumps outside the repository. Run
benchmarks sequentially, without competing tests or imports. Add `-cpuprofile`
and `-memprofile` paths for profiling; `go tool pprof -alloc_space` shows
allocation volume. `BenchmarkReadSQLLongLiterals` provides synthetic coverage
for large plain and escaped SQL literals without a private dump.

## License
Apache-2.0. See LICENSE.
