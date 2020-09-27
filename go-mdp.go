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
)

type TableTransform struct {
	Type    string                      `json:"type"`
	Field   string                      `json:"field"`
	Options transformations.TransformationOptions `json:"options"`
}

type Config struct {
	TableTransforms map[string][]TableTransform
	Patterns []ConfigPattern `json:"patterns"`
	PostSQL string `json:"postSql,omitempty"`
}

type DumpOptions struct {
	InputFile *string
	OutputFile *string
}

type ConfigPattern struct {
	TableName string         `json:"tableName"`
	Fields    []PatternField `json:"fields"`
	Skip bool `json:"skip,omitempty"`
}

type PatternField struct {
	Field       string                      `json:"field"`
	Position    int                         `json:"position"`
	Type        string                      `json:"type"`
	Constraints []PatternFieldConstraint    `json:"constraints"`
	Options     transformations.TransformationOptions `json:"options,omitempty"`
}

type PatternFieldConstraint struct {
	Field    string `json:"field"`
	Position int    `json:"position"`
	Value    string `json:"value"`
}

var (
	fieldTransformationsMap = map[string]func(expr ast.ValueExpr, options transformations.TransformationOptions) interface{} {
		"value":    transformations.TransformFieldValue,
	}
	rowTransformationsMap = map[string]func(row transformations.MappedRow, options transformations.TransformationOptions) interface{} {
		"template": transformations.TransformRowTemplate,
	}
)

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
	config, dumpOptions := parseArgs()
	var input io.Reader
	var output io.Writer

	if *dumpOptions.InputFile != "" {
		f, err := os.Open(*dumpOptions.InputFile)
		if err != nil {
			panic(err)
		}
		input = bufio.NewReaderSize(f, 4096)
	} else {
		input = os.Stdin
	}

	if *dumpOptions.OutputFile != "" {
		f, _ := os.Create(*dumpOptions.OutputFile)
		output = bufio.NewWriterSize(f, 4096)
	} else {
		output = os.Stdout
	}
	lines := processInput(input, config)
	lineBuffer := make(map[int]string)
	currentLineNumber := 0
	var processBuffer func()
	processBuffer = func(){
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
	Type *types.FieldType
	Index int
	Name string
}

type columnMap map[int]ColumnSchema

func (c columnMap) GetByName(name string) (ColumnSchema, error)  {
	for i:= range c {
		if c[i].Name == name {
			return c[i], nil
		}
	}
	return ColumnSchema{}, errors.New("not found")
}

type TableSchema struct {
	Columns columnMap
	Name string
}

var schemaMapLock = sync.Mutex{}
var tableSchemas = make(map[string]TableSchema)

func processCreateTableStatement(stmt *ast.CreateTableStmt) {
	tableName := stmt.Table.Name.String()
	schema := TableSchema{
		Columns: make(columnMap),
		Name: tableName,
	}
	for i:= range stmt.Cols {
		col := stmt.Cols[i]
		schema.Columns[i] = ColumnSchema{
			Type: col.Tp,
			Index: i,
			Name: col.Name.String(),
		}
	}
	schemaMapLock.Lock()
	tableSchemas[tableName] = schema
	schemaMapLock.Unlock()
}

func mapInsertRowToColumns(insertRow []ast.ExprNode, tableSchema TableSchema) transformations.MappedRow {
	result := make(transformations.MappedRow)
	for i := range insertRow {
		field := insertRow[i].(ast.ValueExpr)
		column := tableSchema.Columns[i]
		result[column.Name] = field.GetValue()
	}
	return result
}

type wrappedRow struct {
	rowIndex int
	rowData []ast.ExprNode
}

type wrappedColumn struct {
	columnIndex int
	columnData ast.ExprNode
	mappedRow transformations.MappedRow
	columnSchema ColumnSchema
}

func processInsertStatement(stmt *ast.InsertStmt,  originalSql string, allTransforms map[string][]TableTransform) (string) {
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	schema := tableSchemas[tableName]
	tableTransforms, ok := allTransforms[tableName]
	if !ok {
		return originalSql
	}
	allInsertRows := stmt.Lists
	todoRows := make(chan wrappedRow, 16)
	readyRows := make(chan wrappedRow, 16)
	rowMutex := sync.RWMutex{}
	rowProcessors := 4
	processorWg := sync.WaitGroup{}

	for i := 0; i<rowProcessors; i++ {
		go func() {
			processorWg.Add(1)
			for currentRow := range todoRows {
				mappedRow := mapInsertRowToColumns(currentRow.rowData, schema)
				unwrappedRow := currentRow.rowData
				for columnIdx := range unwrappedRow {
					columnSchema, _ := schema.Columns[columnIdx]
					currentColumn := unwrappedRow[columnIdx]
					for i:=range tableTransforms {
						currentTransform := tableTransforms[i]
						rowTransformationFunc, rowTransformationFound := rowTransformationsMap[currentTransform.Type]
						fieldTransformationFunc, fieldTransformationFound := fieldTransformationsMap[currentTransform.Type]
						if !rowTransformationFound && !fieldTransformationFound {
							panic("unknown transformation")
						}
						if columnSchema.Name != currentTransform.Field {
							continue
						}
						var transformedValue interface{}
						if rowTransformationFound {
							transformedValue = rowTransformationFunc(mappedRow, currentTransform.Options)
						}
						if fieldTransformationFound {
							transformedValue = fieldTransformationFunc(currentColumn.(ast.ValueExpr), currentTransform.Options)
						}
						unwrappedRow[columnIdx] = ast.NewValueExpr(transformedValue, mysql.UTF8Charset, mysql.UTF8Charset)
					}
				}
				readyRows <- currentRow
			}
			processorWg.Done()
		}()
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for row := range readyRows {
			rowMutex.Lock()
			allInsertRows[row.rowIndex] = row.rowData
			rowMutex.Unlock()
		}
		wg.Done()
	}()

	// ALL ROWS PROCESSOR
	for i := range allInsertRows {
		rowMutex.RLock()
		currentRow := allInsertRows[i]
		rowMutex.RUnlock()
		rowForTrans := wrappedRow{
			rowIndex: i,
			rowData:  currentRow,
		}
		todoRows <- rowForTrans
	}
	close(todoRows)
	processorWg.Wait()
	close(readyRows)
	wg.Wait()
	buf := new(bytes.Buffer)
	err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, buf))
	if err != nil {
		panic(err)
	}
	return buf.String()+";"
}

type orderedLine struct {
	order int
	line string
}

type statementType string

const statementTypeInsert statementType = "insert"
const statementTypeCreateTable statementType = "createTable"
const statementTypeComment statementType = "comment"

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

type preparsedCommentStmt struct {}

func (p preparsedCommentStmt) GetType() string {
	return "comment"
}

type preparseResult struct {
	tableName string
	statementType statementType
}

var preparseRegexps = map[statementType]*regexp.Regexp{
	statementTypeComment: regexp.MustCompile(`^--`),
	statementTypeInsert: regexp.MustCompile(`(?m)^(?:\/\*!\d+ )?INSERT(?: (?:LOWPRIORITY|DELAYED|HIGH_PRIORITY))?(?: IGNORE)? INTO \x60?(\w+)\x60?`),
	statementTypeCreateTable: regexp.MustCompile(`(?m)^(?:\/\*!\d+ )?CREATE(?: TEMPORARY)? TABLE (?: TEMPORARY)? \x60?(\w+)\x60?`),
}

func preparse(line string) preparsedStatement {
	for statementType := range preparseRegexps {
		expression := preparseRegexps[statementType]
		matches := expression.FindStringSubmatch(line)
		if len(matches) > 0  {
			switch statementType {
			case statementTypeComment:
				return preparsedCommentStmt{}
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
	commentRegex := regexp.MustCompile("^--")
	outputLines := make(chan orderedLine)
	r := bufio.NewReaderSize(input, 2*1024*1024)
	statementsForProcessing := make(chan orderedLine)
	wg := sync.WaitGroup{}
	for i:=0;i<16;i++ {
		go func() {
			wg.Add(1)
			p := parser.New()
			for currentOrderedLine := range statementsForProcessing {
				currentStatementLine := currentOrderedLine.line
				processedLine := func() orderedLine {
					preparseResult := preparse(currentStatementLine)
					switch preparseResult.(type) {
					case nil:
					case preparsedCommentStmt:
						return currentOrderedLine
					case preparsedInsertStmt:
					case preparsedCreateStmt:
						if _, transformExists := config.TableTransforms[preparseResult.(preparsedStatementWithTable).GetTableName()]; !transformExists {
							return currentOrderedLine
						}
					}
					parseResult, warnings, err := p.Parse(currentStatementLine, mysql.UTF8Charset, mysql.UTF8Charset)
					if err != nil {
						return currentOrderedLine
					}
					if len(warnings) >0 {
						fmt.Println(warnings)
					}
					statement := parseResult[0]
					result := currentStatementLine
					switch statement.(type) {
					case *ast.InsertStmt:
						result = processInsertStatement(statement.(*ast.InsertStmt), currentStatementLine, config.TableTransforms)
					case *ast.CreateTableStmt:
						processCreateTableStatement(statement.(*ast.CreateTableStmt))
					}
					return orderedLine{
						line: result + "\n",
						order: currentOrderedLine.order,
					}
				}()
				outputLines <- processedLine
				currentStatementLine = ""
			}
			wg.Done()
		}()
	}
	lineOrder := 0
	pushLine := func(line string, output chan orderedLine) {
		output <- orderedLine{
			order: lineOrder,
			line: line,
		}
		lineOrder++
	}
	go func() {
		currentStatementLine := ""
		defer func() {
			close(statementsForProcessing)
			wg.Wait()
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
			if commentRegex.MatchString(line) || len(line) == 0 {
				pushLine(line+"\n", outputLines)
				continue
			}
			currentStatementLine += line+"\n"
			lastCharacter := line[len(line)-1:]
			if lastCharacter == ";" {
				pushLine(currentStatementLine, statementsForProcessing)
				currentStatementLine = ""
			} else {
				continue
			}
		}
	}()
	return outputLines
}
