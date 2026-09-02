package templates

import (
	"crypto/md5"
	"fmt"
	"github.com/Masterminds/sprig/v3"
	"sync"
	"text/template"
)

type Template struct {
	CompiledTemplate *template.Template
	Dependencies     []*Template
	Name             string
}

func Md5(input string) string {
	hasher := md5.New()
	hasher.Write([]byte(input))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// builtinFuncs returns a fresh map of the funcs every registry provides.
func builtinFuncs() template.FuncMap {
	funcs := sprig.FuncMap()
	funcs["md5"] = func(input string) string {
		return Md5(input)
	}
	funcs["argon2Hash"] = func(input string) (string, error) {
		return argon2Hash(input)
	}
	funcs["bcryptHash"] = func(input string) (string, error) {
		return bcryptHash(input)
	}
	return funcs
}

// Registry holds a set of template funcs together with the templates compiled
// against them.
//
// text/template resolves func names when a template is parsed, so a compiled
// template stays bound to the funcs of the registry that compiled it. Faker
// funcs carry a locale, which is why the cache lives here instead of in a
// package-level map: one shared cache hands the second caller whatever locale
// the first caller compiled with, for any template text the two have in common.
type Registry struct {
	funcs template.FuncMap
	cache sync.Map // md5(template text) -> *template.Template
}

// NewRegistry returns a registry providing the builtin funcs, the funcs passed
// to RegisterTemplateFuncs and funcs, in that order of precedence.
func NewRegistry(funcs template.FuncMap) *Registry {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return newRegistry(funcs)
}

// newRegistry builds a registry. Callers must hold globalMu.
func newRegistry(funcs template.FuncMap) *Registry {
	merged := builtinFuncs()
	for name, fn := range globalFuncs {
		merged[name] = fn
	}
	for name, fn := range funcs {
		merged[name] = fn
	}
	return &Registry{funcs: merged}
}

func (r *Registry) GetCompiledTemplate(templateData string, templateName string) (*template.Template, error) {
	templateId := Md5(templateData)
	if compiled, exists := r.cache.Load(templateId); exists {
		return compiled.(*template.Template), nil
	}
	compiledTemplate, err := template.New(templateName).Funcs(r.funcs).Parse(templateData)
	if err != nil {
		return nil, fmt.Errorf("cannot compile template: %w", err)
	}
	r.cache.Store(templateId, compiledTemplate)
	return compiledTemplate, nil
}

var (
	globalMu        sync.RWMutex
	globalFuncs     = template.FuncMap{}
	defaultRegistry = &Registry{funcs: builtinFuncs()}
)

// defaultReg returns the registry backing the package-level compile functions.
func defaultReg() *Registry {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return defaultRegistry
}

// RegisterTemplateFuncs adds funcs to the set that every registry created
// afterwards starts from.
//
// Prefer passing funcs to NewRegistry. These apply process-wide, so a caller
// processing more than one locale in a single process cannot use them to pick
// between locales.
func RegisterTemplateFuncs(funcs template.FuncMap) {
	globalMu.Lock()
	defer globalMu.Unlock()
	for name, fn := range funcs {
		globalFuncs[name] = fn
	}
	// Templates the package-level API already compiled resolved the funcs they
	// were parsed with and would keep calling the ones just replaced.
	defaultRegistry = newRegistry(nil)
}

// GetCompiledTemplate compiles templateData using the funcs registered with
// RegisterTemplateFuncs.
func GetCompiledTemplate(templateData string, templateName string) (*template.Template, error) {
	return defaultReg().GetCompiledTemplate(templateData, templateName)
}
