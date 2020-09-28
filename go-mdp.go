package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/duffpl/go-mdp/transformations"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type TableConfig struct {
	Name    string         `json:"name"`
	Columns []ColumnConfig `json:"columns"`
}

type ColumnConfig struct {
	Name            string                       `json:"name"`
	Transformations []ColumnTransformationConfig `json:"transformations"`
}

func (t TableConfig) GetColumnConfigByName(name string) ColumnConfig {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return t.Columns[i]
		}
	}
	return ColumnConfig{}
}

type ColumnTransformationConfig struct {
	Function transformations.TranformationFunction
}

func (c *ColumnTransformationConfig) UnmarshalJSON(i []byte) error {
	configData := &struct {
		Type    transformations.ColumnTransformationType `json:"type"`
		Options json.RawMessage                          `json:"options"`
	}{}
	json.Unmarshal(i, configData)
	c.Function = transformations.GetTransformation(configData.Type, configData.Options)
	return nil
}

type Config struct {
	TableConfigs []TableConfig `json:"tables"`
	PostSQL      string        `json:"postSql,omitempty"`
}

func (c Config) GetTableConfig(name string) TableConfig {
	for i := range c.TableConfigs {
		currentConfig := c.TableConfigs[i]
		if currentConfig.Name != name {
			continue
		}
		return currentConfig
	}
	return TableConfig{}
}

type DumpOptions struct {
	InputFile  *string
	OutputFile *string
}

// Many thanks to https://stackoverflow.com/a/47515580/1454045
func init() {
	lvl, ok := os.LookupEnv("LOG_LEVEL")
	// LOG_LEVEL not set, let's default to info
	if !ok {
		lvl = "info"
	}
	// parse string, this is built-in feature of logrus
	ll, err := logrus.ParseLevel(lvl)
	if err != nil {
		ll = logrus.InfoLevel
	}
	// set global log level
	logrus.SetLevel(ll)
}

func main() {
	then := time.Now()
	config, dumpOptions := parseArgs()
	var input io.Reader
	var output io.Writer

	if *dumpOptions.InputFile != "" {
		f, err := os.Open(*dumpOptions.InputFile)
		if err != nil {
			panic(err)
		}
		input = bufio.NewReaderSize(f, 16*4096)
	} else {
		input = os.Stdin
	}

	if *dumpOptions.OutputFile != "" {
		f, _ := os.Create(*dumpOptions.OutputFile)
		output = bufio.NewWriterSize(f, 16*4096)
	} else {
		output = os.Stdout
	}
	lines := processInput(input, config)
	lineBuffer := make(map[int]string)
	currentLineNumber := 0
	var processBuffer func()
	processBuffer = func() {
		if line, found := lineBuffer[currentLineNumber]; found {
			output.Write([]byte(line))
			delete(lineBuffer, currentLineNumber)
			currentLineNumber++
			processBuffer()
		}
	}

	for line := range lines {
		lineBuffer[line.order] = line.line
		if currentLineNumber == line.order {
			processBuffer()
		}
	}
	processBuffer()
	fmt.Println(time.Since(then))
}

func parseArgs() (Config, DumpOptions) {
	argParser := argparse.NewParser("anonymize-mysqldump", "Transforms MySQL database dump data using provided configuration file")
	configFilePath := argParser.String("c", "config", &argparse.Options{Required: false, Help: "Path to config.json", Default: "config.json"})
	dumpFilePath := argParser.String("i", "input", &argparse.Options{Required: false, Help: "Path to dump file. If not set STDIN is used as input"})
	outputFilePath := argParser.String("o", "output", &argparse.Options{Required: false, Help: "Path to output file. If not set STDOUT is used"})

	dumpOptions := DumpOptions{
		InputFile:  dumpFilePath,
		OutputFile: outputFilePath,
	}

	err := argParser.Parse(os.Args)
	if err != nil {
		fmt.Print(argParser.Usage(err))
		os.Exit(1)
	}
	return readConfigFile(*configFilePath), dumpOptions
}

func readConfigFile(filepath string) Config {
	jsonConfig, err := ioutil.ReadFile(filepath)
	if err != nil {
		logrus.Fatal(err)
	}

	var decoded Config
	jsonReader := strings.NewReader(string(jsonConfig))
	jsonParser := json.NewDecoder(jsonReader)
	jsonParser.Decode(&decoded)
	return decoded
}

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

var schemaMapLock = sync.Mutex{}
var tableSchemas = make(map[string]TableSchema)

func processCreateTableStatement(stmt *ast.CreateTableStmt) {
	tableName := stmt.Table.Name.String()
	schema := TableSchema{
		Columns: make(columnMap),
		Name:    tableName,
	}
	for i := range stmt.Cols {
		col := stmt.Cols[i]
		schema.Columns[i] = ColumnSchema{
			Type:  col.Tp,
			Index: i,
			Name:  col.Name.String(),
		}
	}
	schemaMapLock.Lock()
	tableSchemas[tableName] = schema
	schemaMapLock.Unlock()
}

func mapInsertRowToColumns(insertRow []ast.ExprNode, tableSchema TableSchema) transformations.MappedRow {
	result := make(transformations.MappedRow)
	for i := range insertRow {
		field, ok := insertRow[i].(ast.ValueExpr)
		if !ok {
			fmt.Println("haa")
		}
		column := tableSchema.Columns[i]
		result[column.Name] = field.GetValue()
	}
	return result
}

func processInsertStatement(stmt *ast.InsertStmt, tableConfig TableConfig) string {
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	schema, schemaFound := tableSchemas[tableName]
	if !schemaFound {
		panic(tableName)
	}
	allInsertRows := stmt.Lists
	for currentRowIndex := range allInsertRows {
		currentRow := allInsertRows[currentRowIndex]
		mappedRow := mapInsertRowToColumns(currentRow, schema)
		for columnIdx := range currentRow {
			columnSchema, _ := schema.Columns[columnIdx]
			currentColumn := currentRow[columnIdx]
			columnConfig := tableConfig.GetColumnConfigByName(columnSchema.Name)
			for i := range columnConfig.Transformations {
				currentTransform := columnConfig.Transformations[i]
				var transformedValue interface{}
				transformedValue = currentTransform.Function(mappedRow, currentColumn.(ast.ValueExpr))
				currentRow[columnIdx] = ast.NewValueExpr(transformedValue, mysql.UTF8Charset, mysql.UTF8Charset)
			}
		}
	}
	buf := new(bytes.Buffer)
	err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, buf))
	if err != nil {
		panic(err)
	}
	return buf.String() + ";"
}

type orderedLine struct {
	order int
	line  string
}

type statementType string

const statementTypeInsert statementType = "insert"
const statementTypeCreateTable statementType = "createTable"

type preparsedStatement interface {
	GetType() string
}

type preparsedStatementWithTable interface {
	GetTableName() string
}

type preparsedCreateStmt struct {
	tableName string
}

func (p preparsedCreateStmt) GetTableName() string {
	return p.tableName
}

func (p preparsedCreateStmt) GetType() string {
	return "create"
}

type preparsedInsertStmt struct {
	tableName string
}

func (p preparsedInsertStmt) GetTableName() string {
	return p.tableName
}

func (p preparsedInsertStmt) GetType() string {
	return "insert"
}

var preparseRegexps = map[statementType]*regexp.Regexp{
	statementTypeInsert:      regexp.MustCompile(`(?m)^(?:/\*!\d+ )?INSERT(?: (?:LOWPRIORITY|DELAYED|HIGH_PRIORITY))?(?: IGNORE)? INTO \x60?(\w+)\x60?`),
	statementTypeCreateTable: regexp.MustCompile(`(?m)^(?:/\*!\d+ )?CREATE(?: TEMPORARY)? TABLE(?: IF NOT EXISTS)? \x60?(\w+)\x60?`),
}

func preparse(line string) preparsedStatement {
	for statementType := range preparseRegexps {
		expression := preparseRegexps[statementType]
		matches := expression.FindStringSubmatch(line)
		if len(matches) > 0 {
			switch statementType {
			case statementTypeCreateTable:
				return preparsedCreateStmt{tableName: matches[1]}
			case statementTypeInsert:
				return preparsedInsertStmt{tableName: matches[1]}
			}
		}
	}
	return nil
}

func processInput(input io.Reader, config Config) chan orderedLine {

	outputLines := make(chan orderedLine)
	r := bufio.NewReaderSize(input, 2*1024*1024)
	linesForProcessing := make(chan orderedLine)
	lineProcessorsWg := sync.WaitGroup{}
	for i := 0; i < 16; i++ {
		go func() {
			lineProcessorsWg.Add(1)
			p := parser.New()
			for currentLine := range linesForProcessing {
				currentStatementLine := currentLine.line
				currentLine.line = func() string {
					var tableName string
					preparseResult := preparse(currentStatementLine)
					switch preparseResult.(type) {
					case nil:
						return currentStatementLine
					case preparsedStatementWithTable:
						tableName = preparseResult.(preparsedStatementWithTable).GetTableName()
						if len(config.GetTableConfig(tableName).Columns) == 0 {
							return currentStatementLine
						}
					}
					parseResult, warnings, err := p.Parse(currentStatementLine, mysql.UTF8Charset, mysql.UTF8Charset)
					if err != nil {
						return currentStatementLine
					}
					if len(warnings) > 0 {
						fmt.Println(warnings)
					}
					if len(parseResult) == 0 {
						fmt.Println("BAD")
					}
					statement := parseResult[0]
					switch statement.(type) {
					case *ast.InsertStmt:
						return processInsertStatement(statement.(*ast.InsertStmt), config.GetTableConfig(tableName))
					case *ast.CreateTableStmt:
						processCreateTableStatement(statement.(*ast.CreateTableStmt))
					}
					return currentStatementLine
				}()
				outputLines <- currentLine
				currentStatementLine = ""
			}
			lineProcessorsWg.Done()
		}()
	}
	lineOrder := 0

	pushLine := func(line string) {
		linesForProcessing <- orderedLine{
			order: lineOrder,
			line:  line,
		}
		lineOrder++
	}

	// line reader
	go func() {
		currentStatementLine := ""
		defer func() {
			close(linesForProcessing)
			lineProcessorsWg.Wait()
			close(outputLines)
		}()
		for {
			line, err := r.ReadString('\n')
			if err == io.EOF {
				return
			} else if err != nil {
				logrus.Error(err.Error())
				break
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				pushLine(line)
				continue
			}
			currentStatementLine += line + "\n"
			lastCharacter := line[len(line)-1:]
			if lastCharacter == ";" {
				pushLine(currentStatementLine)
				currentStatementLine = ""
			} else {
				continue
			}
		}
	}()
	return outputLines
}
