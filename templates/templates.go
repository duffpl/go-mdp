package templates

import (
	"crypto/md5"
	"fmt"
	"github.com/Masterminds/sprig/v3"
	"sync"
	"text/template"
)

var compiledTemplates = sync.Map{}

type Template struct {
	CompiledTemplate *template.Template
	Dependencies     []*Template
	Name             string
}

var templateFuncs = template.FuncMap{}

func Md5(input string) string {
	hasher := md5.New()
	hasher.Write([]byte(input))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func init() {
	templateFuncs = sprig.FuncMap()
	templateFuncs["md5"] = func(input string) string {
		return Md5(input)
	}
	templateFuncs["argon2Hash"] = func(input string) string {
		result, err := argon2Hash(input)
		if err != nil {
			panic(fmt.Errorf("cannot hash password: %w", err))
		}
		return result
	}
	templateFuncs["bcryptHash"] = func(input string) string {
		result, err := bcryptHash(input)
		if err != nil {
			panic(fmt.Errorf("cannot hash password: %w", err))
		}
		return result
	}
}

func GetCompiledTemplate(templateData string, templateName string) (*template.Template, error) {
	templateId := Md5(templateData)
	compiled, exists := compiledTemplates.Load(templateId)
	if !exists {
		compiledTemplate, err := template.New(templateName).Funcs(templateFuncs).Parse(templateData)
		if err != nil {
			return nil, fmt.Errorf("cannot compile template: %w", err)
		}
		compiledTemplates.Store(templateId, compiledTemplate)
		return compiledTemplate, nil
	}
	return compiled.(*template.Template), nil
}
func RegisterTemplateFuncs(funcs template.FuncMap) {
	for name, fn := range funcs {
		templateFuncs[name] = fn
	}
}
