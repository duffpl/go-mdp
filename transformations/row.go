package transformations

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/Masterminds/sprig"
	"github.com/pingcap/parser/ast"
)

type TemplateTransformationOptions struct {
	Template string `json:"template"`
}

type ValueTransformationOptions struct {
	Value interface{} `json:"value"`
}

type templateTransformationData struct {
	Row        MappedRow
	FieldValue string
}

func stringMd5(input string) string {
	h := md5.New()
	h.Write([]byte(input))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func init() {
	templateFuncs = sprig.FuncMap()
	templateFuncs["md5"] = func(input string) string {
		hasher := md5.New()
		hasher.Write([]byte(input))
		return fmt.Sprintf("%x", hasher.Sum(nil))
	}

	RegisterColumnTransformation(TransformationTypeTemplate, func(rawOptions json.RawMessage) (ColumnTransformationFunction, error) {
		options := &TemplateTransformationOptions{}
		err := json.Unmarshal(rawOptions, options)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal template options: %w", err)
		}
		return func(row MappedRow, fieldValue ast.ValueExpr) (interface{}, error) {
			return TransformColumnTemplate(row, fieldValue, *options)
		}, nil
	})
	RegisterColumnTransformation(TransformationTypeValue, func(rawOptions json.RawMessage) (ColumnTransformationFunction, error) {
		templateOptions := &ValueTransformationOptions{}
		err := json.Unmarshal(rawOptions, templateOptions)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal value options: %w", err)

		}
		return func(row MappedRow, fieldValue ast.ValueExpr) (interface{}, error) {
			return TransformColumnValue(*templateOptions)
		}, nil
	})
}

func TransformColumnTemplate(row MappedRow, fieldValue ast.ValueExpr, options TemplateTransformationOptions) (interface{}, error) {
	transformationTemplate := options.Template
	output := new(bytes.Buffer)
	tmpl, err := getCompiledTemplate(transformationTemplate)
	if err != nil {
		return nil, fmt.Errorf("cannot get compiled template: %w", err)
	}
	err = tmpl.Execute(output, templateTransformationData{
		Row:        row,
		FieldValue: fieldValue.GetString(),
	})
	if err != nil {
		return nil, fmt.Errorf("cannot execute template: %w", err)
	}
	return output.String(), nil
}

func TransformColumnValue(options ValueTransformationOptions) (interface{}, error) {
	return options.Value, nil
}