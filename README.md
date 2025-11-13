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
- With Go 1.20+:
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
- Only value literals inside INSERT are transformed; expressions are not supported as sources.


## Quick start
1) Create a config file (JSON) describing transformations. Example:

{
  "settings": { "locale": "default" },
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
            { "type": "template", "options": { "template": "user-{{ .Row.id }}@{{ .GlobalVariables.domain }}" } }
          ]
        },
        {
          "name": "first_name",
          "transformations": [
            { "type": "template", "options": { "template": "{{ transformFirstName .FieldValue }}" } }
          ]
        },
        {
          "name": "last_name",
          "transformations": [
            { "type": "template", "options": { "template": "{{ transformLastName .FieldValue }}" } }
          ]
        },
        {
          "name": "password",
          "transformations": [
            { "type": "template", "options": { "template": "{{ bcryptHash \"changeme\" }}" } }
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
- transformations: array of objects with shape:
  { "type": "template" | "value", "options": { ... } }
  - type: template
    - options.template: Go template string; output replaces the value
  - type: value
    - options.value: static value to set (string, number, etc.)

Note: Internally, all transformations are executed via compiled templates. The `value` type is equivalent to a template that outputs a constant.


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
- The processor reads the input stream and groups it into SQL statements by semicolons
- CREATE TABLE statements are parsed to record table schemas (column order and names)
- Only tables listed in your config are transformed
- For each INSERT row in those tables:
  1) Render row variables (if any)
  2) For each configured column, render column variables (if any) and then apply its template(s)
  3) Replace the original column value with the rendered result
- The final INSERT is re-serialized and written; non-target statements pass through unchanged
- After completion, postSql (if set) is appended

Performance notes
- The file is streamed; memory use is modest
- Multiple CPU workers parse/transform lines concurrently


## Examples
- Simple email rewrite:
  {
    "tables": [
      {
        "name": "users",
        "columns": [
          { "name": "email", "transformations": [ { "type": "template", "options": { "template": "user-{{ .Row.id }}@example.com" } } ] }
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


## License
Apache-2.0. See LICENSE.