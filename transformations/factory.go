package transformations

import (
	"encoding/json"
	"github.com/pingcap/parser/ast"
)

type transformationFactory func(rawOptions json.RawMessage) TranformationFunction
type TranformationFunction func(row MappedRow, fieldValue ast.ValueExpr) interface{}

var factories = make(map[ColumnTransformationType]transformationFactory)

func init() {
	factories[templateTransformationType] = func(rawOptions json.RawMessage) TranformationFunction {
		templateOptions := &TemplateOptions{}
		json.Unmarshal(rawOptions, templateOptions)
		return func(row MappedRow, fieldValue ast.ValueExpr) interface{} {
			return TransformRowTemplate(row, fieldValue, *templateOptions)
		}
	}
	factories[valueTransformationType] = func(rawOptions json.RawMessage) TranformationFunction {
		options := &ValueOptions{}
		json.Unmarshal(rawOptions, options)
		return func(row MappedRow, fieldValue ast.ValueExpr) interface{} {
			return TransformFieldValue(fieldValue, *options)
		}
	}
}

func GetTransformation(t ColumnTransformationType, opts json.RawMessage) TranformationFunction {
	return factories[t](opts)
}
