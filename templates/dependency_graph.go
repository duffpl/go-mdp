package templates

import (
	"fmt"
	"github.com/duffpl/go-mdp/v2/config"
	"sort"
	"strings"
	"text/template/parse"
)

func CompileTemplates(source map[string]config.Template, prefix string) (map[string]*Template, error) {
	return defaultReg().CompileTemplates(source, prefix)
}
func (r *Registry) CompileTemplates(source map[string]config.Template, prefix string) (map[string]*Template, error) {
	prefixed := make(map[string]config.Template, len(source))
	for name, text := range source {
		prefixed["."+prefix+"."+name] = text
	}
	return r.CompileAllTemplates(prefixed)
}
func CompileAllTemplates(source map[string]config.Template) (map[string]*Template, error) {
	return defaultReg().CompileAllTemplates(source)
}
func (r *Registry) CompileAllTemplates(source map[string]config.Template) (map[string]*Template, error) {
	compiled := make(map[string]*Template, len(source))
	names := make([]string, 0, len(source))
	for name := range source {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t, err := r.GetCompiledTemplate(string(source[name]), name)
		if err != nil {
			return nil, fmt.Errorf("cannot compile template %s: %w", name, err)
		}
		compiled[name] = &Template{Name: name, CompiledTemplate: t}
	}
	for _, name := range names {
		t := compiled[name]
		refs, err := templateReferences(t.CompiledTemplate)
		if err != nil {
			return nil, fmt.Errorf("template %s: %w", name, err)
		}
		seen := make(map[string]bool)
		for _, ref := range refs {
			matches := matchingDependencies(ref, names)
			if len(matches) == 0 && variableReference(ref, names) && strings.Count(ref, ".") >= 2 {
				return nil, fmt.Errorf("template %s depends on %s, but %s is not defined", name, ref, ref)
			}
			for _, dep := range matches {
				if !seen[dep] {
					t.Dependencies = append(t.Dependencies, compiled[dep])
					seen[dep] = true
				}
			}
		}
	}
	// Validate all configured variables, including those not currently used by a column.
	if _, err := GetOrderedTemplates(compiled); err != nil {
		return nil, err
	}
	return compiled, nil
}
func isVariableReference(path string) bool {
	for _, scope := range []string{"GlobalVariables", "TableVariables", "RowVariables", "ColumnVariables"} {
		if path == "."+scope || strings.HasPrefix(path, "."+scope+".") {
			return true
		}
	}
	return false
}

func variableReference(ref string, names []string) bool {
	if isVariableReference(ref) {
		return true
	}
	prefix, _, ok := strings.Cut(strings.TrimPrefix(ref, "."), ".")
	if !ok {
		return false
	}
	for _, name := range names {
		if strings.HasPrefix(name, "."+prefix+".") {
			return true
		}
	}
	return false
}
func matchingDependencies(ref string, names []string) []string {
	var result []string
	for _, name := range names {
		// Match complete path segments, never regex substrings. Reading a whole map
		// conservatively requires all its entries.
		if ref == name || (ref == "." && strings.HasPrefix(name, ".")) || (strings.Count(ref, ".") == 1 && strings.HasPrefix(name, ref+".")) {
			result = append(result, name)
		}
	}
	return result
}
func ExtractDependencies(root *parse.ListNode, names []string) []string {
	w := newReferenceWalker(nil)
	w.list(root, rootScope("."))
	seen := make(map[string]bool)
	var result []string
	for _, ref := range w.refs {
		for _, name := range matchingDependencies(ref, names) {
			if !seen[name] {
				result = append(result, name)
				seen[name] = true
			}
		}
	}
	return result
}
func ExtractVariables(root *parse.ListNode, prefix string) []string {
	w := newReferenceWalker(nil)
	w.list(root, rootScope("."))
	var result []string
	for _, ref := range w.refs {
		if strings.HasPrefix(ref, "."+prefix+".") {
			result = append(result, ref)
		}
	}
	return result
}
func GetDependencyStackForMultipleTemplates(names []string, templates map[string]*Template) ([]*Template, error) {
	var stack []*Template
	state := make(map[string]uint8)
	var path []string
	var visit func(string) error
	visit = func(name string) error {
		if state[name] == 1 {
			return fmt.Errorf("cycle detected: %s", strings.Join(append(path, name), " -> "))
		}
		if state[name] == 2 {
			return nil
		}
		t, ok := templates[name]
		if !ok {
			return fmt.Errorf("template %s does not exist", name)
		}
		state[name] = 1
		path = append(path, name)
		for _, dep := range t.Dependencies {
			if err := visit(dep.Name); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		state[name] = 2
		stack = append(stack, t)
		return nil
	}
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return stack, nil
}
func GetOrderedTemplates(templates map[string]*Template) ([]*Template, error) {
	names := make([]string, 0, len(templates))
	for name := range templates {
		names = append(names, name)
	}
	sort.Strings(names)
	return GetDependencyStackForMultipleTemplates(names, templates)
}
