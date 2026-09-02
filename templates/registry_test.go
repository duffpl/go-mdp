package templates

import (
	"bytes"
	"testing"
	"text/template"
)

func render(t *testing.T, r *Registry, templateData string) string {
	t.Helper()
	compiled, err := r.GetCompiledTemplate(templateData, "test")
	if err != nil {
		t.Fatalf("cannot compile template: %v", err)
	}
	output := &bytes.Buffer{}
	if err := compiled.Execute(output, nil); err != nil {
		t.Fatalf("cannot execute template: %v", err)
	}
	return output.String()
}

// Compiled templates are cached by template text, and a template resolves the
// funcs its registry held when it was parsed. Registries must therefore keep
// separate caches, or the second one is served a template bound to the first
// one's funcs - which is how dumps of one locale ended up with another
// locale's faker data.
func TestRegistriesDoNotShareCompiledTemplates(t *testing.T) {
	const templateData = `{{ transform "input" }}`

	first := NewRegistry(template.FuncMap{"transform": func(string) string { return "first" }})
	second := NewRegistry(template.FuncMap{"transform": func(string) string { return "second" }})

	if got := render(t, first, templateData); got != "first" {
		t.Errorf("first registry: got %q, want %q", got, "first")
	}
	if got := render(t, second, templateData); got != "second" {
		t.Errorf("second registry reused a template compiled by the first: got %q, want %q", got, "second")
	}
}

func TestRegistryCachesCompiledTemplates(t *testing.T) {
	registry := NewRegistry(nil)
	const templateData = `{{ "cached" }}`

	first, err := registry.GetCompiledTemplate(templateData, "test")
	if err != nil {
		t.Fatalf("cannot compile template: %v", err)
	}
	second, err := registry.GetCompiledTemplate(templateData, "test")
	if err != nil {
		t.Fatalf("cannot compile template: %v", err)
	}
	if first != second {
		t.Error("registry recompiled a template it had already compiled")
	}
}

func TestNewRegistryIncludesRegisteredFuncs(t *testing.T) {
	RegisterTemplateFuncs(template.FuncMap{"registryScoped": func() string { return "registered" }})

	if got := render(t, NewRegistry(nil), `{{ registryScoped }}`); got != "registered" {
		t.Errorf("got %q, want %q", got, "registered")
	}
}

// Registering funcs has to drop templates the package-level API already
// compiled, otherwise they keep calling the funcs they were parsed with.
func TestRegisterTemplateFuncsAppliesToAlreadyCompiledTemplates(t *testing.T) {
	const templateData = `{{ reRegistered }}`

	RegisterTemplateFuncs(template.FuncMap{"reRegistered": func() string { return "before" }})
	compiled, err := GetCompiledTemplate(templateData, "test")
	if err != nil {
		t.Fatalf("cannot compile template: %v", err)
	}
	output := &bytes.Buffer{}
	if err := compiled.Execute(output, nil); err != nil {
		t.Fatalf("cannot execute template: %v", err)
	}
	if output.String() != "before" {
		t.Fatalf("got %q, want %q", output.String(), "before")
	}

	RegisterTemplateFuncs(template.FuncMap{"reRegistered": func() string { return "after" }})
	compiled, err = GetCompiledTemplate(templateData, "test")
	if err != nil {
		t.Fatalf("cannot compile template: %v", err)
	}
	output.Reset()
	if err := compiled.Execute(output, nil); err != nil {
		t.Fatalf("cannot execute template: %v", err)
	}
	if output.String() != "after" {
		t.Errorf("stale compiled template kept the previously registered func: got %q, want %q", output.String(), "after")
	}
}
