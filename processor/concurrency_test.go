package processor

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/duffpl/go-mdp/v2/config"
)

func usersEmailConfig() config.Config {
	return config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"user-{{ .Row.id }}@anonymized.test"},
					},
				},
			},
		},
	}
}

// runProcessWithTimeout runs Process in a goroutine and fails the test if it
// does not return within the timeout (i.e. the pipeline deadlocked).
func runProcessWithTimeout(t *testing.T, cfg config.Config, input string, timeout time.Duration) error {
	t.Helper()
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- processor.Process(strings.NewReader(input), &bytes.Buffer{}, context.Background())
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatalf("Process did not return within %s — pipeline deadlocked", timeout)
		return nil
	}
}

// An INSERT for a configured table whose CREATE TABLE never appears in the
// input (e.g. a data-only dump) must fail fast instead of blocking a worker
// forever waiting for the schema.
func TestProcessor_InsertWithoutCreateTable_ReturnsError(t *testing.T) {
	input := "INSERT INTO users (id, email) VALUES (1, 'john.doe@example.com');\n"
	err := runProcessWithTimeout(t, usersEmailConfig(), input, 5*time.Second)
	if err == nil {
		t.Fatal("Expected error for INSERT without preceding CREATE TABLE, got nil")
	}
	if !strings.Contains(err.Error(), "users") {
		t.Errorf("Expected error to mention table 'users', got: %v", err)
	}
}

// An INSERT that appears before its CREATE TABLE must also error: waiting
// would deadlock with GOMAXPROCS=1 or when enough out-of-order INSERTs
// occupy every worker.
func TestProcessor_InsertBeforeCreateTable_ReturnsError(t *testing.T) {
	input := "INSERT INTO users (id, email) VALUES (1, 'john.doe@example.com');\n" +
		"CREATE TABLE users (id int, email varchar(255));\n"
	err := runProcessWithTimeout(t, usersEmailConfig(), input, 5*time.Second)
	if err == nil {
		t.Fatal("Expected error for INSERT appearing before CREATE TABLE, got nil")
	}
	if !strings.Contains(err.Error(), "users") {
		t.Errorf("Expected error to mention table 'users', got: %v", err)
	}
}

// Skipped tables never need a schema, so a data-only dump of a skipped table
// must keep working.
func TestProcessor_SkipTableInsertWithoutCreate_NoError(t *testing.T) {
	cfg := config.Config{SkipTables: []string{"users"}}
	input := "INSERT INTO users (id, email) VALUES (1, 'john.doe@example.com');\n"
	err := runProcessWithTimeout(t, cfg, input, 5*time.Second)
	if err != nil {
		t.Fatalf("Expected no error for skipped table without CREATE, got: %v", err)
	}
}

// Unconfigured tables are passed through without schema lookups, so INSERT
// before CREATE must keep working for them.
func TestProcessor_UnconfiguredTableInsertWithoutCreate_NoError(t *testing.T) {
	input := "INSERT INTO other (id) VALUES (1);\n"
	err := runProcessWithTimeout(t, usersEmailConfig(), input, 5*time.Second)
	if err != nil {
		t.Fatalf("Expected no error for unconfigured table without CREATE, got: %v", err)
	}
}

// The normal ordering must still work: CREATE first, INSERTs after.
func TestProcessor_CreateThenManyInserts_Succeeds(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE users (id int, email varchar(255));\n")
	for i := 1; i <= 200; i++ {
		fmt.Fprintf(&sb, "INSERT INTO users (id, email) VALUES (%d, 'user%d@example.com');\n", i, i)
	}
	err := runProcessWithTimeout(t, usersEmailConfig(), sb.String(), 10*time.Second)
	if err != nil {
		t.Fatalf("Expected success for CREATE-then-INSERT ordering, got: %v", err)
	}
}

// waitGoroutineBaseline polls until the goroutine count drops back to the
// baseline or the timeout expires.
func waitGoroutineBaseline(t *testing.T, baseline int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()
		current := runtime.NumGoroutine()
		if current <= baseline {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("Goroutines leaked: baseline %d, still running %d after %s\n%s",
				baseline, current, timeout, buf[:n])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// When a worker errors out mid-stream, the readStatements goroutine must not
// stay blocked forever on its output channel with unread input remaining.
func TestProcessor_NoGoroutineLeakOnProcessingError(t *testing.T) {
	cfg := usersEmailConfig()
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}
	var sb strings.Builder
	sb.WriteString("CREATE TABLE users (id int, email varchar(255));\n")
	// Unparseable statement for a configured table triggers a worker error.
	sb.WriteString("INSERT INTO users VALUES (broken;\n")
	// Enough trailing statements to overflow all channel buffers so the
	// reader goroutine has to block if nobody drains it.
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&sb, "INSERT INTO filler VALUES (%d);\n", i)
	}

	baseline := runtime.NumGoroutine()
	err = processor.Process(strings.NewReader(sb.String()), &bytes.Buffer{}, context.Background())
	if err == nil {
		t.Fatal("Expected processing error, got nil")
	}
	waitGoroutineBaseline(t, baseline, 5*time.Second)
}

// Cancelling the context mid-stream must unwind every pipeline goroutine.
func TestProcessor_NoGoroutineLeakOnContextCancel(t *testing.T) {
	cfg := usersEmailConfig()
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}
	var sb strings.Builder
	sb.WriteString("CREATE TABLE users (id int, email varchar(255));\n")
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, "INSERT INTO users (id, email) VALUES (%d, 'user%d@example.com');\n", i, i)
	}

	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processor.Process(strings.NewReader(sb.String()), &bytes.Buffer{}, ctx)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Expected context cancellation error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Process did not return after context cancellation")
	}
	waitGoroutineBaseline(t, baseline, 5*time.Second)
}

// waitForSchema must return once its context is cancelled, even when the
// cancellation races with entering the wait. Note: this is a probabilistic
// stress test — it exercises the cancellation path but cannot deterministically
// hit the missed-wakeup window between the ctx.Err() check and cond.Wait
// registration. The real guard is broadcasting under schemaMapLock in
// waitForSchema; do not treat this test alone as regression coverage for that.
func TestProcessor_WaitForSchema_CancelRace(t *testing.T) {
	processor, err := NewProcessor(config.Config{})
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}
	for i := 0; i < 2000; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := processor.waitForSchema(ctx, "never_created")
			done <- err
		}()
		cancel()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("Expected error from cancelled waitForSchema, got nil")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Iteration %d: waitForSchema missed the cancellation wakeup", i)
		}
	}
}
