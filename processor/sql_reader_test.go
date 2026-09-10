package processor

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
)

type chunkedReader struct {
	io.Reader
	size int
}

func (r chunkedReader) Read(b []byte) (int, error) { return r.Reader.Read(b[:min(len(b), r.size)]) }

func TestReadSQLBoundariesAndRoundTrip(t *testing.T) {
	tests := []struct {
		name, input string
		count       int
	}{
		{"same line", "SELECT 1; SELECT 2;", 2},
		{"trailing comment", "SET @a=1; -- comment\nCREATE TABLE t (id int);\n", 3},
		{"quotes", `INSERT INTO t VALUES ('a;\'b',"c;d",'e'';f');`, 1},
		{"comments", "/* ; */ SELECT 1; # ;\n-- ;\nSELECT 2;", 2},
		{"minus is not a comment", "SELECT 1--2;SELECT 3;", 2},
		{"version comment", "/*!40101 SET @a=1 */;\n", 2},
		{"eof without semicolon", "SELECT 1\n", 1},
		{"eof comment", "-- trailing comment\n", 1},
		{"custom delimiter", "DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT ';'; SELECT 2; END$$\nDELIMITER ;\nSELECT 3;\n", 5},
		{"slash delimiter", "DELIMITER //\nSELECT '//'//\nDELIMITER ;\n", 3},
	}
	for _, tt := range tests {
		for _, size := range []int{1, 2, 7, 64 << 10} {
			t.Run(fmt.Sprintf("%s/chunk%d", tt.name, size), func(t *testing.T) {
				var parts []string
				err := readSQL(context.Background(), chunkedReader{strings.NewReader(tt.input), size}, 1<<20, func(s, d string) error { parts = append(parts, s); return nil })
				if err != nil {
					t.Fatal(err)
				}
				if got := strings.Join(parts, ""); got != tt.input {
					t.Fatalf("bytes changed: %q", got)
				}
				if len(parts) != tt.count {
					t.Fatalf("got %d statements, want %d: %#v", len(parts), tt.count, parts)
				}
			})
		}
	}
}

func TestReadSQLRejectsMalformedAndOversizedInput(t *testing.T) {
	for _, s := range []string{"SELECT 'unterminated;", "/* unterminated", "DELIMITER\n", "DELIMITER bad value\n", "SELECT " + strings.Repeat("x", 100)} {
		if err := readSQL(context.Background(), strings.NewReader(s), 64, func(s, d string) error { return nil }); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}
func TestReadSQLAcrossBufferBoundary(t *testing.T) {
	input := "SELECT '" + strings.Repeat("x", 64<<10) + ";still quoted'; SELECT 2;"
	var out strings.Builder
	n := 0
	err := readSQL(context.Background(), strings.NewReader(input), 1<<20, func(s, d string) error { n++; out.WriteString(s); return nil })
	if err != nil || out.String() != input || n != 2 {
		t.Fatalf("error=%v statements=%d", err, n)
	}
}

func TestReadSQLQuotedEscapeRunsAcrossChunks(t *testing.T) {
	for _, quote := range []string{"'", "\"", "`"} {
		for slashes := range 6 {
			for _, padding := range []int{0, (64 << 10) - 10, (64 << 10) - 9} {
				first := "SELECT " + quote + strings.Repeat("x", padding) + strings.Repeat("\\", slashes) + quote
				if quote != "`" && slashes%2 == 1 {
					first += ";inside" + quote
				}
				first += ";"
				input := first + "SELECT 2;"
				for _, size := range []int{1, 2, 7, 64 << 10} {
					var parts []string
					err := readSQL(context.Background(), chunkedReader{strings.NewReader(input), size}, 1<<20,
						func(s, d string) error { parts = append(parts, s); return nil })
					if err != nil || len(parts) != 2 || parts[0] != first || parts[1] != "SELECT 2;" {
						t.Fatalf("quote=%q slashes=%d padding=%d chunk=%d: error=%v parts=%d", quote, slashes, padding, size, err, len(parts))
					}
				}
			}
		}
	}
}
func TestIdentifySQL(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  statementInfo
	}{
		{"/* CREATE TABLE wrong */\nINSERT\nLOW_PRIORITY IGNORE INTO `db`.`user-table` VALUES (1);", statementInfo{"insert", "db", "user-table"}},
		{"CREATE TEMPORARY TABLE IF NOT EXISTS `a``b` (id int);", statementInfo{"create", "", "a`b"}},
		{"/*!50000 INSERT INTO users VALUES (1) */;", statementInfo{"insert", "", "users"}},
		{"INSERT/**/INTO users/* comment */ VALUES (1);", statementInfo{"insert", "", "users"}},
		{"INSERT /*! LOW_PRIORITY */ INTO users VALUES (1);", statementInfo{"insert", "", "users"}},
		{"USE `second-db`;", statementInfo{kind: "use", database: "second-db"}},
		{"CREATE PROCEDURE p() BEGIN\nINSERT INTO users VALUES (1); END;", statementInfo{}},
		{"SELECT 'hello\nINSERT INTO users VALUES (1)';", statementInfo{}},
	} {
		if got := identifySQL(tt.input); got != tt.want {
			t.Errorf("%q: got %+v, want %+v", tt.input, got, tt.want)
		}
	}
}
func TestCountInsertRowsIgnoresQuotedAndNestedSeparators(t *testing.T) {
	for _, tt := range []struct {
		sql  string
		want int
	}{
		{"INSERT INTO t (id,name) VALUES (1,'a),(b');", 1},
		{"INSERT INTO t VALUES (1,'a'), /* ),( */ (2,'b');", 2},
		{`INSERT INTO t VALUES (1,'a\'),(b'),(2,'x');`, 2},
		{"INSERT INTO t VALUES (1,'a''),(b'),(2,'x');", 2},
		{"INSERT INTO t VALUES (f(1,2),'x'),(3,'y');", 2},
		{"/*!50000 INSERT INTO t VALUES (1,'x'),(2,'y') */;", 2},
	} {
		if got := countInsertRows(tt.sql); got != tt.want {
			t.Errorf("%q: got %d want %d", tt.sql, got, tt.want)
		}
	}
}
func FuzzReadSQLPreservesBytes(f *testing.F) {
	for _, s := range []string{"SELECT 1;SELECT 2;", "SELECT 'a;''b'; -- x\n", "DELIMITER $$\nSELECT ';'$$\nDELIMITER ;\n"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			t.Skip()
		}
		var out strings.Builder
		var parts []string
		err := readSQL(context.Background(), strings.NewReader(input), 8192, func(s, d string) error { out.WriteString(s); parts = append(parts, s, d); return nil })
		if err == nil && out.String() != input {
			t.Fatalf("changed input: %q -> %q", input, out.String())
		}
		var chunkedParts []string
		chunkedErr := readSQL(context.Background(), chunkedReader{strings.NewReader(input), 1}, 8192,
			func(s, d string) error { chunkedParts = append(chunkedParts, s, d); return nil })
		if (err == nil) != (chunkedErr == nil) || (err == nil && !slices.Equal(parts, chunkedParts)) {
			t.Fatalf("chunk-dependent framing: whole error=%v, chunked error=%v", err, chunkedErr)
		}
	})
}
