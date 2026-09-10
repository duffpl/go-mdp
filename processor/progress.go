package processor

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"
)

// Activity describes a pipeline stage without SQL text, row values, templates,
// or error messages. Statement is a one-based framed statement number (including
// passthrough SQL and trailing trivia). FirstRow and Rows refer to an INSERT.
type Activity struct {
	Stage     string        `json:"stage"`
	Statement int64         `json:"statement,omitempty"`
	Table     string        `json:"table,omitempty"`
	FirstRow  int           `json:"firstRow,omitempty"`
	Rows      int           `json:"rows,omitempty"`
	StartedAt time.Time     `json:"startedAt"`
	Elapsed   time.Duration `json:"elapsed"`
}

// Progress is a point-in-time view of a Process call. Counts include only bytes
// delivered by Read / accepted by Write; buffered or remote destinations may
// not have flushed, persisted, or imported them. RowsTransformed counts rows in
// successfully transformed INSERTs, even if a later output write fails.
// InFlightBytes is the charged raw-input budget, not heap usage or process RSS.
// Worker activities can overlap and their elapsed times must not be summed as
// wall time. Reading/writing means an I/O call is active, not proof of a stall.
type Progress struct {
	State               string        `json:"state"` // idle, running, stopping, completed, failed, canceled
	StartedAt           time.Time     `json:"startedAt"`
	FinishedAt          time.Time     `json:"finishedAt"`
	Elapsed             time.Duration `json:"elapsed"`
	BytesRead           int64         `json:"bytesRead"`
	BytesWritten        int64         `json:"bytesWritten"`
	StatementsRead      int64         `json:"statementsRead"`
	StatementsCompleted int64         `json:"statementsCompleted"`
	StatementsSkipped   int64         `json:"statementsSkipped"`
	RowsTransformed     int64         `json:"rowsTransformed"`
	LastTable           string        `json:"lastTable,omitempty"` // last identified table, not necessarily the current read
	LastReadAt          time.Time     `json:"lastReadAt"`
	LastWriteAt         time.Time     `json:"lastWriteAt"`
	InFlightBytes       int64         `json:"inFlightBytes"`
	InFlightLimit       int64         `json:"inFlightLimit"`
	Reader              Activity      `json:"reader"`
	Writer              Activity      `json:"writer"`
	Workers             []Activity    `json:"workers"` // index is the worker ID
	Failure             *Activity     `json:"failure,omitempty"`
}

type runProgress struct {
	mu     sync.Mutex
	data   Progress
	ctx    context.Context
	budget *byteBudget
}

func newRunProgress(ctx context.Context, workers int) *runProgress {
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	now := time.Now()
	r := &runProgress{ctx: ctx, data: Progress{
		State: "running", StartedAt: now, Workers: make([]Activity, workers),
		Reader: Activity{Stage: "initializing", StartedAt: now},
		Writer: Activity{Stage: "waiting_statement", StartedAt: now},
	}}
	for i := range r.data.Workers {
		r.data.Workers[i] = Activity{Stage: "idle", StartedAt: now}
	}
	return r
}

// Progress can be polled concurrently with Process, including while I/O or a
// template function is blocked. It never waits for those operations or calls
// user logging code. Before the first run State is idle; each accepted run
// resets all statistics. A rejected concurrent Process call leaves them alone.
// The returned snapshot is independent and may be modified by the caller.
func (p *Processor) Progress() Progress {
	r := p.progress.Load()
	if r == nil {
		return Progress{State: "idle"}
	}
	r.mu.Lock()
	s := r.data
	s.Workers = append([]Activity(nil), s.Workers...)
	if s.Failure != nil {
		failure := *s.Failure
		s.Failure = &failure
	}
	budget := r.budget
	ctx := r.ctx
	r.mu.Unlock()
	now := time.Now()
	if !s.FinishedAt.IsZero() {
		now = s.FinishedAt
	} else if s.Failure != nil || (ctx != nil && ctx.Err() != nil) {
		s.State = "stopping"
	}
	s.Elapsed = now.Sub(s.StartedAt)
	s.Reader.Elapsed = now.Sub(s.Reader.StartedAt)
	s.Writer.Elapsed = now.Sub(s.Writer.StartedAt)
	for i := range s.Workers {
		s.Workers[i].Elapsed = now.Sub(s.Workers[i].StartedAt)
	}
	if budget != nil {
		budget.mu.Lock()
		s.InFlightBytes, s.InFlightLimit = budget.used, budget.limit
		budget.mu.Unlock()
	}
	return s
}

func (r *runProgress) setBudget(b *byteBudget) {
	r.mu.Lock()
	r.budget = b
	r.mu.Unlock()
}

func (r *runProgress) reader(stage string, detail Activity) {
	r.mu.Lock()
	detail.Stage, detail.StartedAt = stage, time.Now()
	r.data.Reader = detail
	if detail.Table != "" {
		r.data.LastTable = detail.Table
	}
	r.mu.Unlock()
}

func (r *runProgress) readerStage(stage string) {
	r.mu.Lock()
	r.data.Reader.Stage, r.data.Reader.StartedAt = stage, time.Now()
	r.mu.Unlock()
}

func (r *runProgress) writer(stage string, detail Activity) {
	r.mu.Lock()
	detail.Stage, detail.StartedAt = stage, time.Now()
	r.data.Writer = detail
	r.mu.Unlock()
}

func (r *runProgress) worker(id int, stage string, detail Activity) {
	r.mu.Lock()
	detail.Stage, detail.StartedAt = stage, time.Now()
	r.data.Workers[id] = detail
	r.mu.Unlock()
}

func (r *runProgress) statementRead() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.StatementsRead++
	return r.data.StatementsRead
}

func (r *runProgress) transformed(rows int) {
	r.mu.Lock()
	r.data.RowsTransformed += int64(rows)
	r.mu.Unlock()
}

func (r *runProgress) completed(skipped bool) {
	r.mu.Lock()
	r.data.StatementsCompleted++
	if skipped {
		r.data.StatementsSkipped++
	}
	r.mu.Unlock()
}

func (r *runProgress) wrote(n int) {
	if n > 0 {
		r.mu.Lock()
		r.data.BytesWritten += int64(n)
		r.data.LastWriteAt = time.Now()
		r.mu.Unlock()
	}
}

// source is reader, writer, or a nonnegative worker ID supplied separately.
func (r *runProgress) failed(source string, worker int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.data.Failure != nil {
		return
	}
	activity := r.data.Reader
	if source == "writer" {
		activity = r.data.Writer
	} else if source == "worker" {
		activity = r.data.Workers[worker]
	}
	activity.Elapsed = time.Since(activity.StartedAt)
	r.data.Failure = &activity
}

func (r *runProgress) finish(err error) {
	r.mu.Lock()
	r.data.FinishedAt = time.Now()
	r.ctx = nil // Do not retain the host's request context after completion.
	r.data.State = "completed"
	if err != nil {
		r.data.State = "failed"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.data.State = "canceled"
		}
	}
	budget := r.budget
	r.mu.Unlock()
	if budget != nil {
		// Process has joined every pipeline goroutine before finishing.
		budget.mu.Lock()
		budget.used = 0
		budget.mu.Unlock()
	}
}

// The cancellation callback retains the original input's closer. This wrapper
// is only passed to readSQL, so it cannot hide Close or change stream ownership.
type progressReader struct {
	input io.Reader
	run   *runProgress
}

func (r progressReader) Read(p []byte) (int, error) {
	r.run.readerStage("reading_input")
	n, err := r.input.Read(p)
	r.run.mu.Lock()
	if n > 0 {
		r.run.data.BytesRead += int64(n)
		r.run.data.LastReadAt = time.Now()
	}
	if err == nil || err == io.EOF {
		r.run.data.Reader.Stage, r.run.data.Reader.StartedAt = "framing", time.Now()
	}
	r.run.mu.Unlock()
	return n, err
}
