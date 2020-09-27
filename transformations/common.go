package transformations

import (
	"html/template"
	"sync"
)

var compiledTemplates = sync.Map{}

func getCompiledTemplate(templateData string) *template.Template {
	templateId := stringMd5(templateData)
	compiled, exists := compiledTemplates.Load(templateId)
	if !exists {
		compiledTemplate, _ := template.New("template").Funcs(templateFuncs).Parse(templateData)
		compiledTemplates.Store(templateId, compiledTemplate)
		return compiledTemplate
	}
	return compiled.(*template.Template)
}
