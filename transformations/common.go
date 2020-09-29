package transformations

import (
	"fmt"
	"html/template"
	"sync"
)

var compiledTemplates = sync.Map{}

func getCompiledTemplate(templateData string) (*template.Template, error) {
	templateId := stringMd5(templateData)
	compiled, exists := compiledTemplates.Load(templateId)
	if !exists {
		compiledTemplate, err := template.New("template").Funcs(templateFuncs).Parse(templateData)
		if err != nil {
			return nil, fmt.Errorf("cannot compile template: %w", err)
		}
		compiledTemplates.Store(templateId, compiledTemplate)
		return compiledTemplate, nil
	}
	return compiled.(*template.Template), nil
}
