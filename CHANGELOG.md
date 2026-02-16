# Changelog

## [Unreleased] - 2024-12-22

### Changed

#### Parser Migration
- Migrated from deprecated `github.com/pingcap/parser` (Sep 2020) to `github.com/pingcap/tidb/pkg/parser` (Dec 2024)
- Using `test_driver` instead of `types/parser_driver` for cleaner dependency tree
- Note: New parser is ~8% slower (1.27s → 1.37s for 500k rows) but provides 4+ years of bug fixes and better MySQL 8.x compatibility

#### Dependency Updates
| Package | Old Version | New Version |
|---------|-------------|-------------|
| `github.com/Masterminds/sprig/v3` | 3.2.2 | 3.3.0 |
| `github.com/bobg/go-generics/v3` | 3.0.1 | 3.7.0 |
| `github.com/sirupsen/logrus` | 1.6.0 | 1.9.3 |
| `github.com/spf13/cobra` | 1.0.0 | 1.10.2 |
| `golang.org/x/crypto` | 0.9.0 | 0.46.0 |
| `golang.org/x/exp` | May 2023 | Dec 2024 |
| `github.com/Masterminds/semver/v3` | 3.1.1 | 3.4.0 |
| `github.com/google/uuid` | 1.1.2 | 1.6.0 |
| `github.com/shopspring/decimal` | 1.2.0 | 1.4.0 |
| `github.com/spf13/cast` | 1.3.1 | 1.10.0 |
| `github.com/spf13/pflag` | 1.0.5 | 1.0.10 |
| `go.uber.org/atomic` | 1.6.0 | 1.11.0 |
| `go.uber.org/multierr` | 1.5.0 | 1.11.0 |
| `go.uber.org/zap` | 1.16.0 | 1.27.0 |

#### Go Version
- Updated from Go 1.21 to Go 1.24

#### Dependency Cleanup
- Reduced indirect dependencies from ~50 to ~20
- Replaced `github.com/imdario/mergo` with `dario.cat/mergo`
- Removed legacy TiDB/parser dependencies

### Performance Optimizations (Prior)
- Implemented pre-computed row indices to eliminate lock contention during parallel processing
- Added `sync.Cond` for schema synchronization between CREATE TABLE and INSERT processing
- Achieved ~2.7x speedup with 8 workers vs single worker (129.8ms → 47.6ms on test data)
