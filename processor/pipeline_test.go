package processor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duffpl/go-mdp/v2/config"
)

type readProbe struct {
	io.ReadCloser
	entered chan struct{}
	once    sync.Once
}

func (r *readProbe) Read(b []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.ReadCloser.Read(b)
}

type blockingWriter struct {
	entered, closed chan struct{}
	once            sync.Once
}

func (w *blockingWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.closed
	return 0, io.ErrClosedPipe
}
func (w *blockingWriter) Close() error {
	select {
	case <-w.closed:
	default:
		close(w.closed)
	}
	return nil
}
func awaitProcess(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not finish")
		return nil
	}
}
func TestProcessCancellationInterruptsOwnedPipes(t *testing.T) {
	for _, postSQL := range []bool{false, true} {
		t.Run(map[bool]string{false: "output", true: "postSQL"}[postSQL], func(t *testing.T) {
			cfg := config.Config{}
			sql := "SELECT 1;"
			if postSQL {
				cfg.PostSQL = sql
				sql = ""
			}
			p, err := NewProcessor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			w := &blockingWriter{entered: make(chan struct{}), closed: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- p.Process(strings.NewReader(sql), w, ctx) }()
			select {
			case <-w.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("writer never entered")
			}
			cancel()
			if err := awaitProcess(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v", err)
			}
		})
	}
	t.Run("input", func(t *testing.T) {
		p, err := NewProcessor(config.Config{})
		if err != nil {
			t.Fatal(err)
		}
		r, w := io.Pipe()
		defer r.Close()
		defer w.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		probe := &readProbe{ReadCloser: r, entered: make(chan struct{})}
		go func() { done <- p.Process(probe, io.Discard, ctx) }()
		select {
		case <-probe.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("reader never entered")
		}
		// No EOF and no bytes: cancellation must close the blocked input read.
		cancel()
		if err := awaitProcess(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	})
}

type failingReader struct {
	io.Reader
	err error
}

func (r failingReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	if err == io.EOF {
		return n, r.err
	}
	return n, err
}

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }
func TestProcessPropagatesIOErrors(t *testing.T) {
	sentinel := errors.New("input failed")
	p, err := NewProcessor(config.Config{PostSQL: "MUST NOT APPEAR"})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = p.Process(failingReader{strings.NewReader("SELECT 1;"), sentinel}, &out, context.Background())
	if !errors.Is(err, sentinel) || strings.Contains(out.String(), "MUST NOT") {
		t.Fatalf("error=%v output=%q", err, out.String())
	}
	err = p.Process(strings.NewReader("SELECT 1;"), shortWriter{}, context.Background())
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %v", err)
	}
}
func TestSchemaIsScopedToDatabaseAndProcess(t *testing.T) {
	p, err := NewProcessor(usersEmailConfig())
	if err != nil {
		t.Fatal(err)
	}
	input := "USE a; CREATE TABLE users (id int,email varchar(255));" +
		"USE b; CREATE TABLE users (email varchar(255),id int);" +
		"INSERT INTO a.users VALUES (1,'private-a');INSERT INTO b.users VALUES ('private-b',2);"
	for range 2 {
		var out bytes.Buffer
		if err := p.Process(strings.NewReader(input), &out, context.Background()); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "private-") || !strings.Contains(out.String(), "user-1@") || !strings.Contains(out.String(), "user-2@") {
			t.Fatalf("wrong schema: %s", out.String())
		}
	}
	if err := p.Process(strings.NewReader("INSERT INTO users VALUES (3,'private');"), io.Discard, context.Background()); err == nil {
		t.Fatal("schema leaked from previous Process")
	}
}
func TestInsertColumnsAndSchemaValidation(t *testing.T) {
	for _, tt := range []struct {
		name, sql string
		bad       bool
	}{
		{"subset", "CREATE TABLE users(id int,email varchar(255),unused int); INSERT INTO users(email,id) VALUES ('private',1);", false},
		{"unknown", "CREATE TABLE users(id int,email varchar(255)); INSERT INTO users(no_such_column) VALUES ('private');", true},
		{"duplicate", "CREATE TABLE users(id int,email varchar(255)); INSERT INTO users(email,email) VALUES ('private','other');", true},
		{"row width", "CREATE TABLE users(id int,email varchar(255)); INSERT INTO users VALUES (1);", true},
		{"create like", "CREATE TABLE users LIKE source; INSERT INTO users VALUES (1,'private');", true},
		{"select source", "CREATE TABLE users(id int,email varchar(255)); INSERT INTO users SELECT 1,'private';", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewProcessor(usersEmailConfig())
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err = p.Process(strings.NewReader(tt.sql), &out, context.Background())
			if (err != nil) != tt.bad {
				t.Fatalf("got %v", err)
			}
			if strings.Contains(out.String(), "private") {
				t.Fatalf("original data leaked: %s", out.String())
			}
		})
	}
}
func TestCustomDelimitersPreserveRoutinesAndTransformTopLevelInsert(t *testing.T) {
	p, err := NewProcessor(usersEmailConfig())
	if err != nil {
		t.Fatal(err)
	}
	input := "CREATE TABLE users(id int,email varchar(255));\nDELIMITER $$\n" +
		"CREATE PROCEDURE demo() BEGIN SELECT ';'; SELECT 2; END$$\n" +
		"INSERT INTO users VALUES (1,'private')$$\nDELIMITER ;\n"
	var out bytes.Buffer
	if err := p.Process(strings.NewReader(input), &out, context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "BEGIN SELECT ';'; SELECT 2; END$$") || !strings.Contains(out.String(), "user-1@") || strings.Contains(out.String(), "private") {
		t.Fatalf("bad output: %s", out.String())
	}
}
func TestPipelineLimits(t *testing.T) {
	for _, settings := range []config.Settings{{Workers: -1}, {MaxInFlightBytes: -1}, {MaxStatementBytes: -1}} {
		if _, err := NewProcessor(config.Config{Settings: settings}); err == nil {
			t.Fatal("negative limit accepted")
		}
	}
	cfg := usersEmailConfig()
	cfg.Settings = config.Settings{Workers: 1, MaxInFlightBytes: 1, MaxStatementBytes: 1024}
	p, err := NewProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	input := "CREATE TABLE users(id int,email varchar(255));INSERT INTO users VALUES (1,'private');"
	if err := p.Process(strings.NewReader(input), io.Discard, context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Process(strings.NewReader("SELECT '"+strings.Repeat("x", 1024)+"';"), io.Discard, context.Background()); err == nil {
		t.Fatal("oversized statement accepted")
	}
}

func TestConcurrentProcessReturnsError(t *testing.T) {
	p, err := NewProcessor(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	probe := &readProbe{ReadCloser: r, entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Process(probe, io.Discard, ctx) }()
	select {
	case <-probe.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reader never entered")
	}
	if err := p.Process(strings.NewReader("SELECT 1;"), io.Discard, context.Background()); err == nil {
		t.Fatal("concurrent Process accepted")
	}
	cancel()
	if err := awaitProcess(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, nil }
func TestReaderWithoutProgressReturnsError(t *testing.T) {
	p, err := NewProcessor(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Process(emptyReader{}, io.Discard, context.Background()); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("got %v", err)
	}
}

type closeTrackingReader struct {
	io.Reader
	closed bool
}

func (r *closeTrackingReader) Close() error { r.closed = true; return nil }

type closeTrackingWriter struct {
	bytes.Buffer
	closed bool
}

func (w *closeTrackingWriter) Close() error { w.closed = true; return nil }
func TestSuccessfulProcessLeavesStreamsOpen(t *testing.T) {
	p, err := NewProcessor(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	r := &closeTrackingReader{Reader: strings.NewReader("SELECT 1;")}
	w := &closeTrackingWriter{}
	if err := p.Process(r, w, context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.closed || w.closed {
		t.Fatal("successful Process closed caller streams")
	}
}
func TestLegacyTemplatesApplyInSequence(t *testing.T) {
	cfg := usersEmailConfig()
	cfg.TableConfigs[0].Columns[0].Templates = []config.Template{"prefix-{{ .FieldValue }}", "{{ .FieldValue }}-suffix"}
	p, err := NewProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := p.Process(strings.NewReader("CREATE TABLE users(id int,email varchar(255)); INSERT INTO users VALUES (1,'value');"), &out, context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "prefix-value-suffix") {
		t.Fatalf("template chain was broken: %s", out.String())
	}
}
