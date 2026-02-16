# Test Data

This directory contains SQL fixtures and benchmark data for processor tests.

## Test Fixtures (`fixtures/`)

SQL files used by unit tests to verify anonymization functionality.

| File | Purpose |
|------|---------|
| `users.sql` | Basic user table with PII (email, name, password) |
| `orders.sql` | Non-sensitive order data for passthrough tests |
| `members.sql` | Multiple rows for row indexing tests |
| `contacts.sql` | Row variable testing |
| `texts.sql` | Special characters testing |
| `profiles.sql` | Empty value testing |
| `items.sql` | Multi-value INSERT testing |

## Benchmark Data (`benchmark/`)

Generated SQL files for performance benchmarks.

| File | Rows | Size | Description |
|------|------|------|-------------|
| `small.sql` | 100 | ~21 KB | Quick benchmarks |
| `medium.sql` | 1,000 | ~208 KB | Standard benchmarks |
| `large.sql` | 10,000 | ~2 MB | Stress testing |

The benchmark data contains a `benchmark_users` table with 12 columns:
- `id`, `email`, `first_name`, `last_name`, `phone`
- `address`, `city`, `country`, `company`
- `notes`, `created_at`, `updated_at`

### Regenerating Benchmark Data

```bash
cd benchmark
go run generate.go
```

## Running Tests

```bash
# Run all processor tests
go test -v ./processor/...

# Run only benchmarks
go test -bench=. -benchmem -run='^$' ./processor/...

# Run specific benchmark
go test -bench=BenchmarkProcessor_Medium -benchmem -run='^$' ./processor/...
```

## Available Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkProcessor_Small_100rows` | 100 rows, 7 column transformations |
| `BenchmarkProcessor_Medium_1000rows` | 1,000 rows, 7 column transformations |
| `BenchmarkProcessor_Large_10000rows` | 10,000 rows, 7 column transformations |
| `BenchmarkProcessor_MD5_Only` | 1,000 rows, single MD5 hash transformation |
| `BenchmarkProcessor_SimpleTemplate` | 1,000 rows, simple template substitution |
