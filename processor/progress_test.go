package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/templates"
)

func progressProcessor(t *testing.T, cfg config.Config) *Processor {
	t.Helper()
	p, err := NewProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func waitProgress(t *testing.T, p *Processor, ready func(Progress) bool) Progress {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		s := p.Progress()
		if ready(s) {
			return s
		}
		select {
		case <-deadline.C:
			t.Fatalf("progress condition not reached: %+v", s)
		case <-tick.C:
		}
	}
}

func TestProgressTotalsResetAndSnapshotIsolation(t *testing.T) {
	cfg := usersEmailConfig()
	cfg.SkipTables = []string{"skip_me"}
	cfg.PostSQL = "SELECT 99;"
	p := progressProcessor(t, cfg)
	if p.Progress().State != "idle" {
		t.Fatal("new processor is not idle")
	}
	sql := "CREATE TABLE users(id int,email text);INSERT INTO users VALUES (1,'private'),(2,'secret');" +
		"INSERT INTO skip_me VALUES ('private');SELECT 1;"
	var output bytes.Buffer
	if err := p.Process(strings.NewReader(sql), &output, context.Background()); err != nil {
		t.Fatal(err)
	}
	s := p.Progress()
	if s.State != "completed" || s.BytesRead != int64(len(sql)) || s.BytesWritten != int64(output.Len()) ||
		s.StatementsRead != 4 || s.StatementsCompleted != 4 || s.StatementsSkipped != 1 || s.RowsTransformed != 2 || s.LastTable != "skip_me" ||
		s.InFlightBytes != 0 || s.InFlightLimit != defaultMaxInFlightBytes || s.LastReadAt.IsZero() || s.LastWriteAt.IsZero() {
		t.Fatalf("incorrect totals: %+v", s)
	}
	data, err := json.Marshal(s)
	if err != nil || bytes.Contains(data, []byte("private")) || bytes.Contains(data, []byte("secret")) {
		t.Fatal("snapshot contained row data or failed to encode")
	}
	s.Workers[0].Stage = "mutated"
	if p.Progress().Workers[0].Stage == "mutated" {
		t.Fatal("snapshot shares worker storage")
	}
	if p.Progress().Elapsed != s.Elapsed {
		t.Fatal("completed elapsed time is not frozen")
	}
	if err := p.Process(strings.NewReader("SELECT 2;"), io.Discard, context.Background()); err != nil {
		t.Fatal(err)
	}
	s = p.Progress()
	if s.BytesRead != 9 || s.StatementsRead != 1 || s.RowsTransformed != 0 || s.StatementsSkipped != 0 || s.Failure != nil {
		t.Fatalf("previous run leaked into progress: %+v", s)
	}
}

func TestProgressReportsBlockedInputAndRejectsConcurrentRun(t *testing.T) {
	p := progressProcessor(t, config.Config{})
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Process(r, io.Discard, ctx) }()
	s := waitProgress(t, p, func(s Progress) bool { return s.Reader.Stage == "reading_input" })
	if err := p.Process(strings.NewReader(""), io.Discard, ctx); err == nil {
		t.Fatal("concurrent run accepted")
	}
	if p.Progress().StartedAt != s.StartedAt || s.BytesRead != 0 || s.Reader.Elapsed < 0 {
		t.Fatal("rejected call replaced progress or invalid read activity")
	}
	cancel()
	if err := awaitProcess(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.Progress().State != "canceled" {
		t.Fatal("cancellation was not recorded")
	}
}

func TestProgressReportsBlockedOutputAndBudget(t *testing.T) {
	cfg := config.Config{Settings: config.Settings{Workers: 1, MaxInFlightBytes: 1}}
	p := progressProcessor(t, cfg)
	w := &blockingWriter{entered: make(chan struct{}), closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Process(strings.NewReader("SELECT 1;SELECT 2;"), w, ctx) }()
	s := waitProgress(t, p, func(s Progress) bool {
		return s.Writer.Stage == "writing_output" && s.Reader.Stage == "waiting_capacity"
	})
	if s.Writer.Statement != 1 || s.Reader.Statement != 2 || s.InFlightBytes != 1 || s.BytesWritten != 0 || !s.LastWriteAt.IsZero() {
		t.Fatalf("incorrect blocked output progress: %+v", s)
	}
	cancel()
	if err := awaitProcess(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.Progress().InFlightBytes != 0 {
		t.Fatal("finished run retained in-flight bytes")
	}
}

func TestProgressReportsBlockedTransformationAndStopping(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	defer close(release)
	templates.RegisterTemplateFuncs(template.FuncMap{"progressGate": func() string { close(entered); <-release; return "masked" }})
	cfg := usersEmailConfig()
	cfg.TableConfigs[0].Columns[0].Templates = []config.Template{"{{ progressGate }}"}
	cfg.TableConfigs[0].Columns[0].Operations = nil
	p := progressProcessor(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.Process(strings.NewReader("CREATE TABLE users(id int,email text);INSERT INTO users VALUES(1,'private');"), io.Discard, ctx)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("template did not enter")
	}
	active := func(s Progress) bool {
		for _, w := range s.Workers {
			if w.Stage == "transforming" && w.Table == "users" && w.Statement == 2 && w.FirstRow == 1 && w.Rows == 1 {
				return s.Writer.Stage == "waiting_result"
			}
		}
		return false
	}
	waitProgress(t, p, active)
	cancel()
	waitProgress(t, p, func(s Progress) bool { return s.State == "stopping" })
	// A synchronous template is not forcibly interrupted; progress remains
	// readable while Process waits for it. Release it without closing twice.
	release <- struct{}{}
	if err := awaitProcess(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestProgressFailureContextAndPartialWrite(t *testing.T) {
	p := progressProcessor(t, usersEmailConfig())
	err := p.Process(strings.NewReader("CREATE TABLE users(id int,email text);INSERT INTO users VALUES(1);"), io.Discard, context.Background())
	s := p.Progress()
	if err == nil || s.State != "failed" || s.Failure == nil || s.Failure.Stage != "transforming" || s.Failure.Table != "users" || s.Failure.Statement != 2 {
		t.Fatalf("missing failure context: error=%v progress=%+v", err, s)
	}
	s.Failure.Table = "mutated"
	if p.Progress().Failure.Table == "mutated" {
		t.Fatal("failure storage is shared")
	}
	err = p.Process(strings.NewReader("SELECT 1;"), shortWriter{}, context.Background())
	s = p.Progress()
	if !errors.Is(err, io.ErrShortWrite) || s.BytesWritten != 8 || s.StatementsCompleted != 0 || s.Failure.Stage != "writing_output" {
		t.Fatalf("partial write not recorded: %+v", s)
	}
}
