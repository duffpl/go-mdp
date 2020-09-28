package transformations

import (
	"github.com/pingcap/parser/ast"
)

func TransformFieldValue(_ ast.ValueExpr, options ValueOptions) interface{} {
	return options.Value
}
