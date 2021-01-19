package transformations

import (
	"encoding/json"
	"github.com/pingcap/parser/ast"
)

type ColumnTransformationInitializer func(rawOptions json.RawMessage, extraOptions any) (ColumnTransformationFunction, error)
type ColumnTransformationFunction func(row MappedRow, rowMeta RowMeta, variables ColumnTemplateVariables, fieldValue ast.ValueExpr) (interface{}, error)

type TransformationOptions map[string]interface{}
type MappedRow map[string]interface{}
type RowMeta struct {
	Index int
}
type ColumnTransformationType string
