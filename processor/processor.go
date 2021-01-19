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
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

type Processor struct {
	config Config
}

func NewProcessorWithConfigJSON(configJson []byte) (*Processor, error) {
	config := &Config{}
	err := json.Unmarshal(configJson, config)
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
func readStatements(input io.Reader, ctx context.Context) (chan string, chan error) {
	outputCh := make(chan string, 100)
	bufferedInput := bufio.NewReader(input)
	errCh := make(chan error)
	go func() {
		currentStatementLine := ""
		defer func() {
			close(outputCh)
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
				outputCh <- line
				continue
			}
			currentStatementLine += line + "\n"
			lastCharacter := line[len(line)-1:]
			if lastCharacter == ";" {
				outputCh <- currentStatementLine
				currentStatementLine = ""
			} else {
				continue
			}
		}
	}()
	return outputCh, errCh
}

var kibel, _ = os.Create("/tmp/kibel2.sql")

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

type lineWithOutputChannel struct {
	line          string
	outputChannel chan string
}

func (p Processor) processLines(input chan string, ctx context.Context) (chan chan string, chan error) {
	outputCh := make(chan chan string, 100)
	errCh := make(chan error)
	linesForProcessing := make(chan lineWithOutputChannel, 100)
	processorCount := runtime.NumCPU()
	lineProcessorsWg := sync.WaitGroup{}
	go func() {
		for line := range input {
			processedCh := make(chan string)
			outputCh <- processedCh
			linesForProcessing <- lineWithOutputChannel{
				line:          line,
				outputChannel: processedCh,
			}
		}
		close(linesForProcessing)
	}()
	for i := 0; i < processorCount; i++ {
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
				processedLine, err := p.processLine(currentLine.line, stmtParser)
				if err != nil {
					errCh <- err
					return
				}
				currentLine.outputChannel <- processedLine
			}
		}()
	}
	go func() {
		lineProcessorsWg.Wait()
		close(outputCh)
		close(errCh)
	}()
	return outputCh, errCh
}

var log logrus.FieldLogger = logrus.New()

func SetLogger(logger logrus.FieldLogger) {
	log = logger
}

func (p Processor) Process(input io.Reader, output io.Writer, pCtx context.Context) error {
	ctx, cancelCtx := context.WithCancel(pCtx)
	linesForProcessing, inputErrors := readStatements(input, ctx)
	processedLines, processingErrors := p.processLines(linesForProcessing, ctx)
	var err error
	go func() {
		defer func() {
			cancelCtx()
		}()
		for {
			select {
			case processedLineCh, ok := <-processedLines:
				if !ok {
					return
				}
				processedLine := <-processedLineCh
				_, err = output.Write([]byte(processedLine))
				if err != nil {
					return
				}
			case err = <-inputErrors:
				if err != nil {
					err = fmt.Errorf("input error: %w", err)
					return
				}
			case err := <-processingErrors:
				if err != nil {
					err = fmt.Errorf("processing error: %w", err)
					return
				}
			}
		}
	}()
	<-ctx.Done()
	if p.config.PostSQL != "" {
		_, err = output.Write([]byte(p.config.PostSQL))
	}
	return err
}
