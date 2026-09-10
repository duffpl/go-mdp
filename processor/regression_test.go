package processor_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/processor"
)

func cfg() config.Config {
	return config.Config{TableConfigs: []config.TableConfig{{TableName: "users", Columns: []config.ColumnConfig{{ColumnName: "email", Templates: []config.Template{"anon-{{ .Row.id }}"}}}}}}
}
func newP(t testing.TB) *processor.Processor {
	t.Helper()
	p, e := processor.NewProcessor(cfg())
	if e != nil {
		t.Fatal(e)
	}
	return p
}

const schema = "CREATE TABLE users (id int, email varchar(255));\n"

func TestLivePipeProcessingError(t *testing.T) {
	for _, badSQL := range []string{"INSERT INTO users VALUES (broken;", "DELIMITER $$\nINSERT INTO users VALUES (broken$$", "DELIMITER --\nINSERT INTO users VALUES (broken--"} {
		t.Run(badSQL, func(t *testing.T) {
			r, w := io.Pipe()
			defer r.Close()
			defer w.Close()
			p := newP(t)
			done := make(chan error, 1)
			go func() { done <- p.Process(r, io.Discard, context.Background()) }()
			if _, e := io.WriteString(w, schema+badSQL); e != nil {
				t.Fatal(e)
			}
			select {
			case e := <-done:
				if e == nil {
					t.Error("expected parse error")
				}
			case <-time.After(time.Second):
				w.Close()
				select {
				case e := <-done:
					t.Errorf("parse error withheld until producer closes pipe: %v", e)
				case <-time.After(time.Second):
					t.Fatal("still hung after EOF")
				}
			}
		})
	}
}

func TestTrailingCommentSchemaHang(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e := newP(t).Process(strings.NewReader("SET @a=1; -- valid trailing comment\n"+schema+"INSERT INTO users VALUES (1,'private');\n"), io.Discard, ctx)
	if e != nil {
		t.Fatalf("valid SQL should complete; got %v", e)
	}
}
func TestMultipleInsertsSameLine(t *testing.T) {
	var b bytes.Buffer
	e := newP(t).Process(strings.NewReader(schema+"INSERT INTO users VALUES (1,'one'); INSERT INTO users VALUES (2,'two');\n"), &b, context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(b.String(), "anon-2") {
		t.Errorf("second INSERT lost: %s", b.String())
	}
}
func TestTrailingSQLWithNewline(t *testing.T) {
	var b bytes.Buffer
	p, e := processor.NewProcessor(config.Config{})
	if e != nil {
		t.Fatal(e)
	}
	if e = p.Process(strings.NewReader("SELECT 1\n"), &b, context.Background()); e != nil {
		t.Fatal(e)
	}
	if b.Len() == 0 {
		t.Error("unterminated final SQL silently dropped when it ends with newline")
	}
}
func TestExplicitColumns(t *testing.T) {
	var b bytes.Buffer
	e := newP(t).Process(strings.NewReader(schema+"INSERT INTO users (email,id) VALUES ('private',1);\n"), &b, context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(b.String(), "private") {
		t.Errorf("wrong column transformed, original PII retained: %s", b.String())
	}
}
func TestRowIndexQuotedTuple(t *testing.T) {
	c := cfg()
	c.TableConfigs[0].Columns[0].Templates = []config.Template{"{{ .RowMeta.Index }}"}
	p, e := processor.NewProcessor(c)
	if e != nil {
		t.Fatal(e)
	}
	var b bytes.Buffer
	e = p.Process(strings.NewReader(schema+"INSERT INTO users VALUES (1,'a),(b');\nINSERT INTO users VALUES (2,'two');\n"), &b, context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(b.String(), "(2,_utf8'2')") {
		t.Errorf("quoted tuple separator corrupts row index: %s", b.String())
	}
}
func TestDuplicateSchema(t *testing.T) {
	var s strings.Builder
	s.WriteString(schema)
	for i := 0; i < 200; i++ {
		s.WriteString("INSERT INTO users VALUES ")
		for j := 0; j < 100; j++ {
			if j > 0 {
				s.WriteByte(',')
			}
			fmt.Fprintf(&s, "(%d,'private')", i*100+j)
		}
		s.WriteString(";\n")
	}
	s.WriteString("CREATE TABLE users (email varchar(255), id int);\nINSERT INTO users VALUES ('last',1);\n")
	var b bytes.Buffer
	e := newP(t).Process(strings.NewReader(s.String()), &b, context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(b.String(), "private") {
		t.Error("later CREATE schema used by earlier INSERTs; PII retained and id overwritten")
	}
}
func TestFailedStatementLeaksOriginal(t *testing.T) {
	c := cfg()
	c.TableConfigs[0].Columns[0].Templates = []config.Template{"{{ index .Row \"missing\" 0 }}"}
	for i := 0; i < 20; i++ {
		p, e := processor.NewProcessor(c)
		if e != nil {
			t.Fatal(e)
		}
		var b bytes.Buffer
		e = p.Process(strings.NewReader(schema+"INSERT INTO users VALUES (1,'private-original');\n"), &b, context.Background())
		if e == nil {
			t.Fatal("expected transform error")
		}
		if strings.Contains(b.String(), "private-original") {
			t.Fatalf("failed transformation emits original statement: %s", b.String())
		}
	}
}
