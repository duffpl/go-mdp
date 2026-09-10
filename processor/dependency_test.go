package processor

import (
	"strings"
	"testing"

	"github.com/duffpl/go-mdp/v2/config"
)

func TestNestedTransitiveDependencies(t *testing.T) {
	for _, source := range []config.Template{
		`{{if .Row.id}}{{printf "%s" (.ColumnVariables.final)}}{{else}}{{.ColumnVariables.base}}{{end}}`,
		`{{with .Row}}{{$.ColumnVariables.final}}{{end}}`,
		`{{$vars := .ColumnVariables}}{{with $vars}}{{.final}}{{end}}`,
		`{{range $i := list 1 2}}{{if eq $i 1}}{{$.ColumnVariables.final}}{{end}}{{end}}`,
		`{{index .ColumnVariables "final"}}`,
		`{{define "value"}}{{.ColumnVariables.final}}{{end}}{{template "value" .}}`,
	} {
		t.Run(string(source), func(t *testing.T) {
			cfg := dependencyConfig()
			cfg.TableConfigs[0].Columns = []config.ColumnConfig{{ColumnName: "value", Templates: []config.Template{source}}}
			out, err := processSQL(t, cfg, "CREATE TABLE t (id int, value text); INSERT INTO t VALUES (1,'a'),(2,'b');")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"safe-1-a!", "safe-2-b!"} {
				if !strings.Contains(out, want) {
					t.Fatalf("missing %q in %s", want, out)
				}
			}
		})
	}
}

func dependencyConfig() config.Config {
	return config.Config{
		GlobalVariables: map[string]config.Template{"prefix": "safe"},
		TableVariables:  map[string]config.Template{"prefix": `{{if true}}{{.GlobalVariables.prefix}}{{end}}`},
		RowVariables:    map[string]config.Template{"prefix": `{{with .TableVariables}}{{.prefix}}{{end}}-{{.Row.id}}`},
		ColumnVariables: map[string]config.Template{
			"base":  `{{.RowVariables.prefix}}-{{.FieldValue}}`,
			"final": `{{.ColumnVariables.base}}!`,
		},
		TableConfigs: []config.TableConfig{{TableName: "t"}},
	}
}

func TestJSONOnlyDependenciesUseEachFieldValue(t *testing.T) {
	cfg := dependencyConfig()
	cfg.TableConfigs[0].Columns = []config.ColumnConfig{{ColumnName: "value", Operations: []config.ColumnOperation{
		{Type: "json", JsonFields: []config.JsonFieldConfig{
			{Path: "name", Template: `{{.ColumnVariables.final}}`},
			{Path: "items.#.name", Template: `{{if .FieldValue}}{{.ColumnVariables.final}}{{end}}`},
		}},
		{Type: "json", JsonFields: []config.JsonFieldConfig{{Path: "name", Template: `{{.FieldValue}}?`}}},
	}}}
	out, err := processSQL(t, cfg, `CREATE TABLE t (id int, value json); INSERT INTO t VALUES (1,'{"name":"a","items":[{"name":"b"},{"name":"c"}]}');`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"safe-1-a!?", "safe-1-b!", "safe-1-c!"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in %s", want, out)
		}
	}
}

func TestInvalidDependenciesRejectedAtConstruction(t *testing.T) {
	for _, scope := range []string{"global", "table", "row", "column"} {
		t.Run(scope+" cycle even when unused", func(t *testing.T) {
			cfg := dependencyConfig()
			var vars map[string]config.Template
			var prefix string
			switch scope {
			case "global":
				vars, prefix = cfg.GlobalVariables, "GlobalVariables"
			case "table":
				vars, prefix = cfg.TableVariables, "TableVariables"
			case "row":
				vars, prefix = cfg.RowVariables, "RowVariables"
			case "column":
				vars, prefix = cfg.ColumnVariables, "ColumnVariables"
			}
			vars["a"] = config.Template(`{{if true}}{{.` + prefix + `.b}}{{end}}`)
			vars["b"] = config.Template(`{{.` + prefix + `.a}}`)
			if _, err := NewProcessor(cfg); err == nil || !strings.Contains(err.Error(), "cycle") {
				t.Fatalf("expected cycle error at construction, got %v", err)
			}
		})
	}
	t.Run("missing JSON variable", func(t *testing.T) {
		cfg := dependencyConfig()
		cfg.TableConfigs[0].Columns = []config.ColumnConfig{{ColumnName: "value", Operations: []config.ColumnOperation{
			{Type: "json", JsonFields: []config.JsonFieldConfig{{Path: "name", Template: `{{with .Row}}{{$.RowVariables.missing}}{{end}}`}}},
		}}}
		if _, err := NewProcessor(cfg); err == nil || !strings.Contains(err.Error(), ".RowVariables.missing") {
			t.Fatalf("expected missing dependency, got %v", err)
		}
	})
}

func TestVariableScopeValidation(t *testing.T) {
	for _, scope := range []string{"table", "row"} {
		t.Run(scope, func(t *testing.T) {
			cfg := dependencyConfig()
			if scope == "table" {
				cfg.TableVariables["invalid"] = `{{.RowVariables.prefix}}`
			} else {
				cfg.RowVariables["invalid"] = `{{.ColumnVariables.base}}`
			}
			if _, err := NewProcessor(cfg); err == nil || !strings.Contains(err.Error(), "later-scope") {
				t.Fatalf("expected invalid scope, got %v", err)
			}
		})
	}
}

func TestOperationDependenciesAndDottedColumnName(t *testing.T) {
	cfg := dependencyConfig()
	// Unreferenced variables are validated but must not be executed.
	cfg.ColumnVariables["unused"] = `{{fail "unused variable executed"}}`
	cfg.TableConfigs[0].Columns = []config.ColumnConfig{{ColumnName: "value.dotted", Operations: []config.ColumnOperation{
		{Type: "template", Template: `{{.ColumnVariables.final}}`},
		{Type: "template", Template: `{{.ColumnVariables.final}}`},
	}}}
	out, err := processSQL(t, cfg, "CREATE TABLE t (id int, `value.dotted` text); INSERT INTO t VALUES (1,'a');")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "safe-1-safe-1-a!!") {
		t.Fatal(out)
	}
}

func TestNestedGlobalVariableOrder(t *testing.T) {
	cfg := config.Config{GlobalVariables: map[string]config.Template{
		"a": `{{with .GlobalVariables}}{{.b}}{{end}}a`,
		"b": `{{if true}}{{.GlobalVariables.c}}{{end}}b`,
		"c": "c",
	}, TableConfigs: []config.TableConfig{{TableName: "t", Columns: []config.ColumnConfig{{ColumnName: "value", Templates: []config.Template{`{{.GlobalVariables.a}}`}}}}}}
	out, err := processSQL(t, cfg, "CREATE TABLE t (value text); INSERT INTO t VALUES ('private');")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "cba") || strings.Contains(out, "private") {
		t.Fatal(out)
	}
}
