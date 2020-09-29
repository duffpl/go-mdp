package processor

import (
	"errors"
	"github.com/pingcap/parser/types"
)

type ColumnSchema struct {
	Type  *types.FieldType
	Index int
	Name  string
}

type columnMap map[int]ColumnSchema

func (c columnMap) GetByName(name string) (ColumnSchema, error) {
	for i := range c {
		if c[i].Name == name {
			return c[i], nil
		}
	}
	return ColumnSchema{}, errors.New("not found")
}

type TableSchema struct {
	Columns columnMap
	Name    string
}

type OrderedLine struct {
	Order int
	Line  string
}

type statementType string

const statementTypeInsert statementType = "insert"
const statementTypeCreateTable statementType = "createTable"
