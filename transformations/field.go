package transformations

import (
	"bytes"
	"github.com/pingcap/parser/ast"
)

func TransformFieldValue(field ast.ValueExpr, options TransformationOptions) interface{} {
	return options["value"]
}

func TransformFieldTemplate(field ast.ValueExpr, options TransformationOptions) interface{} {
	transformationTemplate := options["template"].(string)
	output := new(bytes.Buffer)
	e := getCompiledTemplate(transformationTemplate).Execute(output, struct {
		FieldValue string
	}{
		field.GetString(),
	})
	if e != nil {
		panic(e)
	}
	return output.String()
}