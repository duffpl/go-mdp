package transformations

import (
	"bytes"
	"github.com/pingcap/parser/ast"
)

var templateTransformationType ColumnTransformationType = "template"
var valueTransformationType ColumnTransformationType = "value"

type TemplateOptions struct {
	Template string `json:"template"`
}

type ValueOptions struct {
	Value interface{} `json:"template"`
}

type transformData struct {
	Row        MappedRow
	FieldValue string
}

func TransformRowTemplate(row MappedRow, fieldValue ast.ValueExpr, options TemplateOptions) interface{} {
	transformationTemplate := options.Template
	output := new(bytes.Buffer)
	e := getCompiledTemplate(transformationTemplate).Execute(output, transformData{
		Row:        row,
		FieldValue: fieldValue.GetString(),
	})
	if e != nil {
		panic(e)
	}
	return output.String()
}
