package processor

import (
	"errors"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/types"
)

type ColumnSchema struct {
	Type  *types.FieldType
	Index int
	Name  string
}

type columnMap map[int]ColumnSchema

func (c columnMap) GetByName(name string) (ColumnSchema, error) {
	for i := range c {
		if strings.EqualFold(c[i].Name, name) {
			return c[i], nil
		}
	}
	return ColumnSchema{}, errors.New("not found")
}

type TableSchema struct {
	Columns columnMap
	Name    string
}

type statementType string

const statementTypeInsert statementType = "insert"
const statementTypeCreateTable statementType = "createTable"
