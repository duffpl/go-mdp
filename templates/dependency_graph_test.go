package templates

import (
	"reflect"
	"strings"
	"testing"

	"github.com/duffpl/go-mdp/v2/config"
)

func TestDependencyReferences(t *testing.T) {
	r := NewRegistry(nil)
	for _, tc := range []struct {
		name, text string
		want       []string
	}{
		{"nested pipelines and branches", `{{if (eq .RowVariables.a "x")}}{{printf "%s" (.RowVariables.b)}}{{else}}{{.RowVariables.c}}{{end}}`, []string{".RowVariables.a", ".RowVariables.b", ".RowVariables.c"}},
		{"with root and else", `{{with .Row}}{{$.RowVariables.a}}{{else}}{{.RowVariables.b}}{{end}}`, []string{".RowVariables.a", ".RowVariables.b"}},
		{"rebound dot", `{{with .Row}}{{.RowVariables.fake}}{{end}}{{.RowVariables.a}}`, []string{".RowVariables.a"}},
		{"shadowed alias", `{{$x := .RowVariables}}{{with .Row}}{{$x := .}}{{$x.fake}}{{end}}{{$x.a}}`, []string{".RowVariables.a"}},
		{"assigned alias", `{{$x := .Row}}{{if true}}{{$x = $.RowVariables}}{{end}}{{$x.a}}`, []string{".RowVariables.a", ".RowVariables.b", ".RowVariables.c"}},
		{"loop assigned alias", `{{$x := .Row}}{{range list 1 2}}{{$x.a}}{{$x = $.RowVariables}}{{end}}`, []string{".RowVariables.a", ".RowVariables.b", ".RowVariables.c"}},
		{"literal index", `{{index .RowVariables "a"}}`, []string{".RowVariables.a"}},
		{"chained index", `{{(index $ "RowVariables").a}}`, []string{".RowVariables.a"}},
		{"dynamic index", `{{index .RowVariables .Row.key}}`, []string{".RowVariables.a", ".RowVariables.b", ".RowVariables.c"}},
		{"range with root", `{{range .Row.items}}{{$.RowVariables.a}}{{else}}{{.RowVariables.b}}{{end}}`, []string{".RowVariables.a", ".RowVariables.b"}},
		{"whole map", `{{range .RowVariables}}{{.}}{{end}}`, []string{".RowVariables.a", ".RowVariables.b", ".RowVariables.c"}},
		{"with map truthiness", `{{with .RowVariables}}present{{end}}`, []string{".RowVariables.a", ".RowVariables.b", ".RowVariables.c"}},
		{"associated template", `{{define "read"}}{{.a}}{{end}}{{template "read" .RowVariables}}`, []string{".RowVariables.a"}},
		{"exact names", `{{.Row.ColumnVariables.a}}{{.RowVariables.abc}}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := r.GetCompiledTemplate(tc.text, "test")
			if err != nil {
				t.Fatal(err)
			}
			refs, err := templateReferences(tmpl)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, name := range []string{".RowVariables.a", ".RowVariables.b", ".RowVariables.c"} {
				for _, ref := range refs {
					if len(matchingDependencies(ref, []string{name})) > 0 {
						got = append(got, name)
						break
					}
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v (refs %v)", got, tc.want, refs)
			}
		})
	}
}

func TestDependencyValidationAndOrdering(t *testing.T) {
	r := NewRegistry(nil)
	for _, source := range []map[string]config.Template{
		{".RowVariables.a": `{{.RowVariables.a}}`},
		{".RowVariables.a": `{{if true}}{{.RowVariables.b}}{{end}}`, ".RowVariables.b": `{{.RowVariables.a}}`},
		{".RowVariables.a": `{{.RowVariables.missing}}`},
		{".RowVariables.a": "ok", "Column.x": `{{.RowVariables.abc}}`},
		{".RowVariables.a.b": "ok", "Column.x": `{{.RowVariables.a}}`},
		{"Column.x": `{{define "loop"}}{{template "loop" .}}{{end}}{{template "loop" .}}`},
	} {
		if _, err := r.CompileAllTemplates(source); err == nil {
			t.Fatalf("expected error for %v", source)
		}
	}
	if _, err := r.CompileTemplates(map[string]config.Template{"a": `{{.Custom.missing}}`}, "Custom"); err == nil {
		t.Fatal("custom namespace accepted missing variable")
	}
	source := map[string]config.Template{
		".RowVariables.a": "a",
		".RowVariables.b": `{{.RowVariables.a}}b`,
		".RowVariables.c": `{{.RowVariables.a}}c`,
		"Column.x":        `{{.RowVariables.c}}{{.RowVariables.b}}`,
	}
	compiled, err := r.CompileAllTemplates(source)
	if err != nil {
		t.Fatal(err)
	}
	stack, err := GetDependencyStackForMultipleTemplates([]string{"Column.x", "Column.x"}, compiled)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tmpl := range stack {
		names = append(names, tmpl.Name)
	}
	if got := strings.Join(names, ","); got != ".RowVariables.a,.RowVariables.c,.RowVariables.b,Column.x" {
		t.Fatal(got)
	}
	// The exported traversal must validate hand-built graphs too.
	compiled[".RowVariables.a"].Dependencies = []*Template{compiled[".RowVariables.c"]}
	if _, err := GetDependencyStackForMultipleTemplates([]string{"Column.x"}, compiled); err == nil {
		t.Fatal("traversal accepted a cycle")
	}
}
