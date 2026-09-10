package processor

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/duffpl/go-mdp/v2/config"
)

// Exercise the same rows with both the small fixture statements and the larger
// single-line extended INSERTs commonly seen in mysqldump output.
func benchmarkStatementShape(b *testing.B, extended, passthrough bool) {
	input := loadBenchmarkData(b, "xlarge.sql")
	if extended {
		parts := strings.Split(input, "INSERT INTO `benchmark_users` VALUES ")
		var out strings.Builder
		out.WriteString(parts[0])
		for i, part := range parts[1:] {
			if i%40 == 0 {
				out.WriteString("INSERT INTO `benchmark_users` VALUES ")
			} else {
				out.WriteByte(',')
			}
			out.WriteString(strings.ReplaceAll(strings.TrimSuffix(strings.TrimSpace(part), ";"), "\n", ""))
			if i%40 == 39 || i == len(parts)-2 {
				out.WriteString(";\n")
			}
		}
		input = out.String()
	}
	cfg := benchmarkConfig()
	if passthrough {
		cfg = config.Config{}
	}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		p, err := NewProcessor(cfg)
		if err != nil {
			b.Fatal(err)
		}
		if err := p.Process(strings.NewReader(input), io.Discard, context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkProcessor_StatementShapeSmall(b *testing.B)    { benchmarkStatementShape(b, false, false) }
func BenchmarkProcessor_StatementShapeExtended(b *testing.B) { benchmarkStatementShape(b, true, false) }
func BenchmarkProcessor_StatementShapePassthrough(b *testing.B) {
	benchmarkStatementShape(b, true, true)
}

// Opt in with GO_MDP_BENCH_INPUT and GO_MDP_BENCH_CONFIG. Keep private dumps
// outside the repository; file reads and processor initialization are timed.
func BenchmarkProcessor_ExternalDump(b *testing.B) {
	inputName, configName := os.Getenv("GO_MDP_BENCH_INPUT"), os.Getenv("GO_MDP_BENCH_CONFIG")
	if inputName == "" || configName == "" {
		b.Skip("set GO_MDP_BENCH_INPUT and GO_MDP_BENCH_CONFIG")
	}
	data, err := os.ReadFile(configName)
	if err != nil {
		b.Fatal(err)
	}
	var cfg config.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(inputName)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		input, err := os.Open(inputName)
		if err != nil {
			b.Fatal(err)
		}
		p, err := NewProcessor(cfg)
		if err != nil {
			input.Close()
			b.Fatal(err)
		}
		err = p.Process(input, io.Discard, context.Background())
		closeErr := input.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
}

func BenchmarkReadSQLLongLiterals(b *testing.B) {
	for _, pattern := range []struct{ name, text string }{
		{"plain", "abcdefghij"},
		{"escaped", `{"name":"O\'Brien","path":"C:\\tmp","text":"a; b"}`},
	} {
		b.Run(pattern.name, func(b *testing.B) {
			literal := strings.Repeat(pattern.text, (1<<20)/len(pattern.text))
			input := strings.Repeat("INSERT INTO t VALUES ('"+literal+"');\n", 8)
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var written int
				err := readSQL(context.Background(), strings.NewReader(input), 2<<20,
					func(s, d string) error { written += len(s); return nil })
				if err != nil || written != len(input) {
					b.Fatalf("readSQL: error=%v bytes=%d want=%d", err, written, len(input))
				}
			}
		})
	}
}
