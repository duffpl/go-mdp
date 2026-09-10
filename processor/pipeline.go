package processor

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

const defaultMaxInFlightBytes int64 = 4 << 20
const defaultMaxStatementBytes int64 = 64 << 20

type byteBudget struct {
	mu          sync.Mutex
	used, limit int64
	changed     chan struct{}
}

func (b *byteBudget) acquire(ctx context.Context, size int64) (int64, error) {
	// A single oversized statement can run alone, up to maxStatementBytes.
	size = min(size, b.limit)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		b.mu.Lock()
		if size <= b.limit-b.used {
			b.used += size
			b.mu.Unlock()
			return size, nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-changed:
		}
	}
}
func (b *byteBudget) release(size int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= size
	close(b.changed)
	b.changed = make(chan struct{})
}

type statementResult struct {
	text   string
	weight int64
}
type orderedStatement struct {
	result  chan statementResult
	detail  Activity
	skipped bool
}
type statementWork struct {
	line, delimiter string
	schema          TableSchema
	config          *PreparedTableConfig
	startRow        int
	rowCount        int
	result          chan statementResult
	weight          int64
	detail          Activity
}

type schemaKey struct{ database, table string }

// Process transforms SQL in input order. Concurrent calls on the same Processor
// return an error. Streams remain open on success. On cancellation/error, Process closes
// streams implementing io.Closer to interrupt blocked I/O; output implementing
// Abort() is aborted instead, without flushing buffered data. Non-closable
// streams must be interruptible by their caller if they can block. Process
// joins all pipeline goroutines before returning. Failed output is partial.
func (p *Processor) Process(input io.Reader, output io.Writer, parent context.Context) (resultErr error) {
	if !p.processMu.TryLock() {
		return fmt.Errorf("processor is already processing a stream")
	}
	defer p.processMu.Unlock()
	progress := newRunProgress(parent, p.Config.Settings.Workers)
	p.progress.Store(progress)
	defer func() { progress.finish(resultErr) }()
	p.processedTables = nil
	if err := parent.Err(); err != nil {
		return err
	}
	if input == nil {
		return fmt.Errorf("input must not be nil")
	}
	if output == nil {
		output = io.Discard
	}
	ctx, cancel := context.WithCancelCause(parent)
	interrupted := make(chan struct{})
	stopInterrupt := context.AfterFunc(ctx, func() {
		defer close(interrupted)
		if c, ok := input.(io.Closer); ok {
			_ = c.Close()
		}
		if a, ok := output.(interface{ Abort() }); ok {
			a.Abort()
		} else if c, ok := output.(io.Closer); ok {
			_ = c.Close()
		}
	})
	defer func() {
		if !stopInterrupt() {
			<-interrupted
			if resultErr == nil {
				resultErr = context.Cause(ctx)
			}
		}
		cancel(nil)
	}()
	var pipeline sync.WaitGroup
	workers := len(progress.data.Workers)
	maxBytes := p.Config.Settings.MaxInFlightBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxInFlightBytes
	}
	maxStatement := p.Config.Settings.MaxStatementBytes
	if maxStatement == 0 {
		maxStatement = defaultMaxStatementBytes
	}
	budget := &byteBudget{limit: maxBytes, changed: make(chan struct{})}
	progress.setBudget(budget)
	work := make(chan statementWork)
	ordered := make(chan orderedStatement, workers)
	var tables []string
	defer func() { p.processedTables = tables }()
	var firstFailure sync.Once
	fail := func(err error, source string, workerID int) {
		if err != nil {
			firstFailure.Do(func() {
				progress.failed(source, workerID)
				cancel(err)
			})
		}
	}

	// The reader/dispatcher owns schemas and counters. CREATE is parsed here,
	// so INSERT jobs carry immutable schema snapshots and never wait on workers.
	pipeline.Add(1)
	go func() {
		defer pipeline.Done()
		defer close(work)
		defer close(ordered)
		schemas := make(map[schemaKey]TableSchema)
		counters := make(map[schemaKey]int)
		database := ""
		stmtParser := parser.New()
		progress.reader("framing", Activity{Statement: 1})
		err := readSQL(ctx, progressReader{input, progress}, maxStatement, func(line, delimiter string) (dispatchErr error) {
			if err := ctx.Err(); err != nil {
				return err
			}
			number := progress.statementRead()
			defer func() {
				if dispatchErr == nil {
					progress.reader("framing", Activity{Statement: number + 1})
				}
			}()
			progress.reader("waiting_capacity", Activity{Statement: number})
			weight, err := budget.acquire(ctx, int64(len(line)))
			if err != nil {
				return err
			}
			info := identifySQL(line)
			if info.kind == "use" {
				database = info.database
			}
			if info.database == "" {
				info.database = database
			}
			// Header tokens are substrings of line. Do not let map keys retain
			// an entire extended INSERT after its output has been written.
			key := schemaKey{strings.Clone(info.database), strings.Clone(info.table)}
			table := key.table
			if key.database != "" && table != "" {
				table = key.database + "." + table
			}
			detail := Activity{Statement: number, Table: table}
			progress.reader("dispatching", detail)
			cfg := p.tableTransformations[info.database+"."+info.table]
			if cfg == nil {
				cfg = p.tableTransformations[info.table]
			}
			job := statementWork{line: line, delimiter: delimiter, weight: weight, result: make(chan statementResult, 1), detail: detail}
			skipped := false
			switch info.kind {
			case "create":
				tables = append(tables, info.table)
				if cfg != nil && !cfg.Skip {
					progress.reader("parsing_schema", detail)
					stmt, err := parseSingle(stmtParser, line, delimiter)
					if err != nil {
						return fmt.Errorf("cannot parse CREATE TABLE %s: %w", info.table, err)
					}
					create, ok := stmt.(*ast.CreateTableStmt)
					if !ok {
						return fmt.Errorf("expected CREATE TABLE for %s", info.table)
					}
					schema, err := schemaFromCreate(create)
					if err != nil {
						return err
					}
					schemas[key] = schema
					counters[key] = 0
					if len(line) > 64<<10 {
						stmtParser = parser.New()
					}
				}
			case "insert":
				if cfg != nil {
					if cfg.Skip {
						job.line = ""
						skipped = true
					} else {
						schema, ok := schemas[key]
						if !ok {
							return fmt.Errorf("cannot process INSERT for table %s: no CREATE TABLE statement found earlier in the input, schema is unknown", info.table)
						}
						job.schema = schema
						job.config = cfg
						job.startRow = counters[key] + 1
						job.rowCount = countInsertRows(line)
						job.detail.FirstRow, job.detail.Rows = job.startRow, job.rowCount
						counters[key] += job.rowCount
					}
				}
			}
			progress.reader("waiting_queue", job.detail)
			select {
			case ordered <- orderedStatement{job.result, job.detail, skipped}:
			case <-ctx.Done():
				return ctx.Err()
			}
			if job.config == nil {
				job.result <- statementResult{job.line, job.weight}
				return nil
			}
			progress.reader("waiting_worker", job.detail)
			select {
			case work <- job:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil {
			fail(fmt.Errorf("input/dispatch error: %w", err), "reader", 0)
		} else {
			progress.reader("done", Activity{})
		}
	}()

	for workerID := range workers {
		pipeline.Add(1)
		go func() {
			defer pipeline.Done()
			defer progress.worker(workerID, "done", Activity{})
			workerFail := func(err error) {
				fail(err, "worker", workerID)
			}
			stmtParser := parser.New()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-work:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
					progress.worker(workerID, "parsing_insert", job.detail)
					node, err := parseSingle(stmtParser, job.line, job.delimiter)
					if err != nil {
						workerFail(fmt.Errorf("cannot parse INSERT for table %s: %w", job.schema.Name, err))
						return
					}
					if len(job.line) > 64<<10 {
						// The lexer also retains input and decoded literal buffers.
						stmtParser = parser.New()
					}
					stmt, ok := node.(*ast.InsertStmt)
					if !ok {
						workerFail(fmt.Errorf("expected INSERT for table %s", job.schema.Name))
						return
					}
					if len(stmt.Lists) != job.rowCount {
						workerFail(fmt.Errorf("unsupported INSERT tuple syntax for table %s", job.schema.Name))
						return
					}
					progress.worker(workerID, "transforming", job.detail)
					transformed, err := p.processInsertStatement(ctx, stmt, job.config, job.startRow, job.schema)
					if err != nil {
						workerFail(fmt.Errorf("cannot process INSERT for table %s: %w", job.schema.Name, err))
						return
					}
					// Keep leading comments/whitespace, which may follow the previous
					// statement on the same line. Executable comments are replaced as a unit.
					prefix := leadingSQLTrivia(job.line)
					transformed = prefix + strings.TrimSuffix(transformed, ";\n") + job.delimiter
					progress.transformed(job.rowCount)
					job.result <- statementResult{transformed, job.weight}
					progress.worker(workerID, "idle", Activity{})
				}
			}
		}()
	}
	done := make(chan struct{})
	pipeline.Add(1)
	go func() {
		defer pipeline.Done()
		defer close(done)
		defer progress.writer("done", Activity{})
		write := func(s string, detail Activity) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if s == "" {
				return nil
			}
			progress.writer("writing_output", detail)
			n, err := io.WriteString(output, s)
			progress.wrote(n)
			if err == nil && n != len(s) {
				err = io.ErrShortWrite
			}
			if err != nil {
				return fmt.Errorf("output error: %w", err)
			}
			return nil
		}
		for {
			progress.writer("waiting_statement", Activity{})
			var next orderedStatement
			var ok bool
			select {
			case <-ctx.Done():
				return
			case next, ok = <-ordered:
			}
			if !ok {
				if ctx.Err() != nil {
					return
				}
				if p.Config.PostSQL != "" {
					fail(write("\n"+p.Config.PostSQL, Activity{}), "writer", 0)
				}
				return
			}
			var result statementResult
			progress.writer("waiting_result", next.detail)
			select {
			case <-ctx.Done():
				return
			case result = <-next.result:
			}
			if err := write(result.text, next.detail); err != nil {
				fail(err, "writer", 0)
				return
			}
			budget.release(result.weight)
			progress.completed(next.skipped)
		}
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
	pipeline.Wait()
	return context.Cause(ctx)
}

func parseSingle(p *parser.Parser, line, delimiter string) (ast.StmtNode, error) {
	if delimiter != ";" && strings.HasSuffix(line, delimiter) {
		line = strings.TrimSuffix(line, delimiter) + ";"
	}
	nodes, _, err := p.Parse(line, mysql.UTF8Charset, mysql.UTF8Charset)
	if err != nil {
		return nil, err
	}
	if len(nodes) != 1 {
		return nil, fmt.Errorf("expected one SQL statement, got %d", len(nodes))
	}
	node := nodes[0]
	// TiDB retains both the returned result slice and parser-stack values.
	// Release those references so idle workers do not retain their previous
	// extended INSERT's entire AST. The caller now owns this statement.
	clear(nodes)
	p.Reset()
	return node, nil
}

func leadingSQLTrivia(line string) string {
	pos := 0
	for pos < len(line) {
		if isSQLSpace(line[pos]) {
			pos++
			continue
		}
		tail := line[pos:]
		if tail[0] == '#' || (strings.HasPrefix(tail, "--") && (len(tail) == 2 || isSQLSpace(tail[2]))) {
			if end := strings.IndexByte(tail, '\n'); end >= 0 {
				pos += end + 1
				continue
			}
			break
		}
		if strings.HasPrefix(tail, "/*") && !strings.HasPrefix(tail, "/*!") {
			if end := strings.Index(tail[2:], "*/"); end >= 0 {
				pos += end + 4
				continue
			}
		}
		break
	}
	return line[:pos]
}
