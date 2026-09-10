package cmd

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/processor"
)

func TestBufferedCloseReportsFlushFailure(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	defer w.Close()
	out := &bufferedWriteCloser{w: bufio.NewWriter(w), f: w}
	if _, err := out.WriteString("buffered SQL"); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err == nil {
		t.Fatal("final flush failure was ignored")
	}
}
func TestRunProcessorRejectsSameFileAndHardLink(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "dump.sql")
	alias := filepath.Join(dir, "alias.sql")
	sql := "SELECT 1;\n"
	if err := os.WriteFile(input, []byte(sql), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(input, alias); err != nil {
		t.Fatal(err)
	}
	p, err := processor.NewProcessor(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{input, alias} {
		if err := runProcessor(context.Background(), p, input, output); err == nil {
			t.Fatal("same input/output accepted")
		}
		data, err := os.ReadFile(input)
		if err != nil || string(data) != sql {
			t.Fatalf("input was changed: %q, %v", data, err)
		}
	}
}
func TestRunProcessorFlushesSuccessfulOutput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "in.sql")
	output := filepath.Join(dir, "out.sql")
	sql := "SELECT 1; -- trailing comment\n"
	if err := os.WriteFile(input, []byte(sql), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := processor.NewProcessor(config.Config{PostSQL: "SELECT 2;"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runProcessor(context.Background(), p, input, output); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(data), sql) || !strings.HasSuffix(string(data), "SELECT 2;") {
		t.Fatalf("incomplete output: %q", data)
	}
}
