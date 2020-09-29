package transformations

import (
	"encoding/json"
	"fmt"
	"github.com/pingcap/parser/ast"
)

type ColumnTransformationInitializer func(rawOptions json.RawMessage) (ColumnTransformationFunction, error)
type ColumnTransformationFunction func(row MappedRow, fieldValue ast.ValueExpr) (interface{}, error)

var columnTransformationInitializers = make(map[ColumnTransformationType]ColumnTransformationInitializer)

func RegisterColumnTransformation(t ColumnTransformationType, c ColumnTransformationInitializer) {
	columnTransformationInitializers[t] = c
}

func GetColumnTransformation(t ColumnTransformationType, opts json.RawMessage) (ColumnTransformationFunction, error) {
	transformFunctionInitializer, transformFound := columnTransformationInitializers[t]
	if !transformFound {
		return nil, fmt.Errorf("transformation initializer for '%s' doesn't exist", t)
	}
	transformFunction, err := transformFunctionInitializer(opts)
	if err != nil {
		return nil, fmt.Errorf("cannot initialize '%s': %w", t, err)
	}
	return transformFunction, nil
}
