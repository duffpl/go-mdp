package processor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/duffpl/go-mdp/transformations"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"sync"
)

type Processor struct {
	config Config
}

func NewProcessorWithConfigFile(path string) (*Processor, error) {
	configData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read config file: %w", err)
	}
	config := &Config{}
	err = json.Unmarshal(configData, config)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal config: %w", err)
	}
	return &Processor{
		config: *config,
	}, nil
}

func NewProcessor(config Config) Processor {
	return Processor{
		config: config,
	}
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

func mapInsertRowToColumns(insertRow []ast.ExprNode, tableSchema TableSchema) (transformations.MappedRow, error) {
	result := make(transformations.MappedRow)
	for i := range insertRow {
		field, ok := insertRow[i].(ast.ValueExpr)
		if !ok {
			return nil, fmt.Errorf("cannot cast column value (%T)", insertRow[i])
		}
		column := tableSchema.Columns[i]
		result[column.Name] = field.GetValue()
	}
	return result, nil
}

func processInsertStatement(stmt *ast.InsertStmt, tableConfig TableConfig) (string, error) {
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	schema, schemaFound := tableSchemas[tableName]
	if !schemaFound {
		panic(tableName)
	}
	allInsertRows := stmt.Lists
	for currentRowIndex := range allInsertRows {
		currentRow := allInsertRows[currentRowIndex]
		mappedRow, err := mapInsertRowToColumns(currentRow, schema)
		if err != nil {
			return "", fmt.Errorf("cannot map row: %w", err)
		}
		for columnIdx := range currentRow {
			columnSchema, _ := schema.Columns[columnIdx]
			currentColumn := currentRow[columnIdx]
			columnConfig := tableConfig.GetColumnConfigByName(columnSchema.Name)
			for i := range columnConfig.Transformations {
				currentTransformation := columnConfig.Transformations[i]
				transformedValue, err := currentTransformation.TransformationFunction(mappedRow, currentColumn.(ast.ValueExpr))
				if err != nil {
					return "", fmt.Errorf("cannot apply transform: %w", err)
				}
				currentRow[columnIdx] = ast.NewValueExpr(transformedValue, mysql.UTF8Charset, mysql.UTF8Charset)
			}
		}
	}
	buf := new(bytes.Buffer)
	err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, buf))
	if err != nil {
		return "", fmt.Errorf("cannot restore insert statement: %w", err)
	}
	return buf.String() + ";", nil
}

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
	statementTypeInsert:      regexp.MustCompile(`(?m)^(?:\/\*!\d+ )?INSERT(?: (?:LOWPRIORITY|DELAYED|HIGH_PRIORITY))?(?: IGNORE)? INTO \x60?(\w+)\x60?`),
	statementTypeCreateTable: regexp.MustCompile(`(?m)^(?:\/\*!\d+ )?CREATE(?: TEMPORARY)? TABLE(?: IF NOT EXISTS)? \x60?(\w+)\x60?`),
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
func readStatements(input io.Reader, ctx context.Context) (chan OrderedLine, chan error) {
	linesCh := make(chan OrderedLine)
	bufferedInput := bufio.NewReader(input)
	errCh := make(chan error)
	currentLineOrder := 0
	writeLine := func(line string) {
		linesCh <- OrderedLine{
			Order: currentLineOrder,
			Line:  line,
		}
		currentLineOrder++
	}
	go func() {
		currentStatementLine := ""
		defer func() {
			close(linesCh)
			close(errCh)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			line, err := bufferedInput.ReadString('\n')
			if err == io.EOF {
				return
			} else if err != nil {
				errCh <- err
				return
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				writeLine(line)
				continue
			}
			currentStatementLine += line + "\n"
			lastCharacter := line[len(line)-1:]
			if lastCharacter == ";" {
				writeLine(currentStatementLine)
				currentStatementLine = ""
			} else {
				continue
			}
		}
	}()
	return linesCh, errCh
}

func (p Processor) processLine(line string, parser *parser.Parser) (string, error) {
	var tableName string
	config := p.config
	preparseResult := preparse(line)
	switch preparseResult.(type) {
	case nil:
		return line, nil
	case preparsedStatementWithTable:
		tableName = preparseResult.(preparsedStatementWithTable).GetTableName()
		if len(config.GetTableConfigByName(tableName).Columns) == 0 {
			return line, nil
		}
	}
	parseResult, _, err := parser.Parse(line, mysql.UTF8Charset, mysql.UTF8Charset)
	if err != nil {
		return line, fmt.Errorf("cannot parse statement: %w", err)
	}
	statement := parseResult[0]
	switch statement.(type) {
	case *ast.InsertStmt:
		return processInsertStatement(statement.(*ast.InsertStmt), config.GetTableConfigByName(tableName))
	case *ast.CreateTableStmt:
		processCreateTableStatement(statement.(*ast.CreateTableStmt))
	}
	return line, nil
}

func (p Processor) processLines(linesForProcessing chan OrderedLine, ctx context.Context) (chan OrderedLine, chan error) {
	outputLines := make(chan OrderedLine)
	errCh := make(chan error)
	lineProcessorsWg := sync.WaitGroup{}
	for i := 0; i < 16; i++ {
		go func() {
			defer func() {
				lineProcessorsWg.Done()
			}()
			lineProcessorsWg.Add(1)
			stmtParser := parser.New()
			for currentLine := range linesForProcessing {
				select {
				case <-ctx.Done():
					return
				default:
				}
				currentStatementLine := currentLine.Line
				processedLine, err := p.processLine(currentStatementLine, stmtParser)
				if err != nil {
					errCh <- err
					return
				}
				currentLine.Line = processedLine
				outputLines <- currentLine
				currentStatementLine = ""
			}
		}()
	}
	go func() {
		lineProcessorsWg.Wait()
		close(outputLines)
		close(errCh)
	}()
	return outputLines, errCh
}

func (p Processor) ProcessInput(input io.Reader, ctx context.Context) (chan OrderedLine, chan error) {
	errCh := make(chan error)
	linesForProcessing, inputErrors := readStatements(input, ctx)
	processedLines, processingErrors := p.processLines(linesForProcessing, ctx)
	go func() {
		select {
		case err := <-inputErrors:
			if err != nil {
				errCh <- fmt.Errorf("input error: %w", err)
			}
		case err := <-processingErrors:
			if err != nil {
				errCh <- fmt.Errorf("processing error: %w", err)
			}
		}
	}()
	return processedLines, errCh
}
