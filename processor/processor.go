package processor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/bobg/go-generics/v2/slices"
	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/templates"
	"github.com/duffpl/go-mdp/v2/transformations"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/sirupsen/logrus"
	"io"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type Processor struct {
	Config               config.Config
	tableTransformations map[string]*PreparedTableConfig
	globalVariables      map[string]string
}

var tableRowCounterMutex = &sync.Mutex{}
var tableRowCounterMap = make(map[string]int)

func NewProcessorWithConfig(configData config.Config) (*Processor, error) {
	return NewProcessor(configData)
}

func incrementTableCounter(tableName string) int {
	tableRowCounterMutex.Lock()
	defer tableRowCounterMutex.Unlock()
	counter := tableRowCounterMap[tableName]
	counter++
	tableRowCounterMap[tableName] = counter
	return counter
}

func NewProcessor(config config.Config) (*Processor, error) {
	//transformations.RegisterColumnTransformations()
	tableTransformations, err := prepareTableConfigs(config)
	if err != nil {
		return nil, fmt.Errorf("unable to prepare transformations: %w", err)
	}
	p := &Processor{
		Config:               config,
		tableTransformations: tableTransformations,
	}
	globalVariables, err := p.renderGlobalVariables()
	if err != nil {
		return nil, fmt.Errorf("cannot render global variables: %w", err)
	}
	p.globalVariables = globalVariables
	return p, nil
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

type rowTemplateData struct {
	Row             transformations.MappedRow
	RowMeta         transformations.RowMeta
	RowVariables    map[string]string
	GlobalVariables map[string]string
	TableVariables  map[string]string
}

type columnTemplateData struct {
	rowTemplateData
	ColumnVariables map[string]string
	FieldValue      interface{}
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

func processInsertStatement(stmt *ast.InsertStmt, tableConfig *PreparedTableConfig) (string, error) {
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	schema, schemaFound := tableSchemas[tableName]
	if !schemaFound {
		panic(tableName)
	}
	allInsertRows := stmt.Lists
	for currentRowIndex := range allInsertRows {
		tableRowIndex := incrementTableCounter(tableName)
		currentRow := allInsertRows[currentRowIndex]
		mappedRow, err := mapInsertRowToColumns(currentRow, schema)
		if err != nil {
			return "", fmt.Errorf("cannot map row: %w", err)
		}
		rowMeta := transformations.RowMeta{
			Index: tableRowIndex,
		}
		rowData := &rowTemplateData{
			Row:             mappedRow,
			RowMeta:         rowMeta,
			RowVariables:    make(map[string]string),
			GlobalVariables: tableConfig.GlobalVariables,
			TableVariables:  tableConfig.TableVariables,
		}
		err = renderRowVariables(
			tableConfig.RowVariableTemplates,
			rowData,
		)

		if err != nil {
			return "", fmt.Errorf("cannot render row variables: %w", err)
		}

		for columnIdx := range currentRow {
			columnSchema, _ := schema.Columns[columnIdx]
			currentColumn := currentRow[columnIdx]
			columnTemplates, ok := tableConfig.ColumnTemplates[columnSchema.Name]

			if !ok {
				continue
			}
			// render column templates
			for _, tmpl := range columnTemplates {
				if err != nil {
					return "", fmt.Errorf("cannot get transformation function: %w", err)
				}
				columnVariablesTemplates := slices.Filter(tmpl.Dependencies, func(tmpl *templates.Template) bool {
					return strings.HasPrefix(tmpl.Name, ".ColumnVariables")
				})
				columnData := &columnTemplateData{
					rowTemplateData: *rowData,
					FieldValue:      currentColumn.(ast.ValueExpr).GetString(),
					ColumnVariables: make(map[string]string),
				}
				err = renderColumnVariables(columnVariablesTemplates, columnData)
				if err != nil {
					return "", fmt.Errorf("cannot render column variables: %w", err)
				}
				transformedValue := new(bytes.Buffer)
				err := tmpl.CompiledTemplate.Execute(transformedValue, columnData)
				if err != nil {
					return "", fmt.Errorf("cannot apply transform: %w", err)
				}
				currentRow[columnIdx] = ast.NewValueExpr(transformedValue.String(), mysql.UTF8Charset, mysql.UTF8Charset)
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

func renderRowVariables(
	templates []*templates.Template,
	data *rowTemplateData,
) error {
	result := make(map[string]string)
	for _, tmpl := range templates {
		output := new(bytes.Buffer)
		err := tmpl.CompiledTemplate.Execute(output, data)
		if err != nil {
			return fmt.Errorf("cannot render variable '%s' template: %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".RowVariables.")
		result[shortName] = output.String()
		data.RowVariables = result
	}
	return nil
}

func renderColumnVariables(
	templates []*templates.Template,
	data *columnTemplateData,
) error {
	for _, tmpl := range templates {
		output := new(bytes.Buffer)
		err := tmpl.CompiledTemplate.Execute(output, data)
		if err != nil {
			return fmt.Errorf("cannot render variable '%s' template: %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".ColumnVariables.")
		data.ColumnVariables[shortName] = output.String()
	}
	return nil
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

func (p Processor) processLine(line string, parser *parser.Parser) (string, error) {
	var tableName string
	preparseResult := preparse(line)
	switch preparseResult.(type) {
	case nil:
		return line, nil
	case preparsedStatementWithTable:
		tableName = preparseResult.(preparsedStatementWithTable).GetTableName()
	}
	tableTransformations, ok := p.tableTransformations[tableName]
	if !ok {
		return line, nil
	}
	parseResult, _, err := parser.Parse(line, mysql.UTF8Charset, mysql.UTF8Charset)
	if err != nil {
		return line, fmt.Errorf("cannot parse statement for table %s: %w", tableName, err)
	}
	statement := parseResult[0]
	switch statement.(type) {
	case *ast.InsertStmt:
		line, err = processInsertStatement(statement.(*ast.InsertStmt), tableTransformations)
		if err != nil {
			return line, fmt.Errorf("cannot process insert statement for table %s: %w", tableName, err)
		}
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
			for {
				select {
				case <-ctx.Done():
					return
				case currentLine, ok := <-linesForProcessing:
					if !ok {
						return
					}
					processedLine, err := p.processLine(currentLine.line, stmtParser)
					if err != nil {
						currentLine.outputChannel <- fmt.Sprintf("/* error: %s */\n%s", err.Error(), currentLine.line)
						errCh <- err
						return
					}
					currentLine.outputChannel <- processedLine
				}
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

func (p Processor) Process(input io.Reader, output io.Writer, pCtx context.Context) (err error) {
	ctx, cancelCtx := context.WithCancel(pCtx)
	readLines, inputErrors := readStatements(input, ctx)
	processedLinesChans, processingErrors := p.processLines(readLines, ctx)
	go func() {
		defer cancelCtx()
		for {
			select {
			case processedLineCh, ok := <-processedLinesChans:
				if !ok {
					return
				}
				processedLine := <-processedLineCh
				_, err = output.Write([]byte(processedLine))
				if err != nil {
					err = fmt.Errorf("output error: %w", err)
					return
				}
			case err = <-inputErrors:
				if err != nil {
					err = fmt.Errorf("input error: %w", err)
					return
				}
			case err = <-processingErrors:
				if err != nil {
					err = fmt.Errorf("processing error: %w", err)
					return
				}
			}
		}
	}()
	<-ctx.Done()
	if p.Config.PostSQL != "" {
		_, err = output.Write([]byte(p.Config.PostSQL))
	}
	return err
}

func (p Processor) renderGlobalVariables() (map[string]string, error) {
	compiled, err := templates.CompileTemplates(p.Config.GlobalVariables, "TableVariables")
	if err != nil {
		return nil, fmt.Errorf("cannot compile global variables templates: %w", err)
	}
	ordered, err := templates.GetOrderedTemplates(compiled)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve global variables order: %w", err)
	}
	result := make(map[string]string, len(ordered))
	for _, tmpl := range ordered {
		output := new(bytes.Buffer)
		err := tmpl.CompiledTemplate.Execute(output, struct {
			GlobalVariables map[string]string
		}{
			GlobalVariables: result,
		})
		if err != nil {
			return nil, fmt.Errorf("cannot render global variables template '%w': %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".GlobalVariables.")
		result[shortName] = output.String()
	}
	return result, nil
}

type PreparedTableConfig struct {
	ColumnTemplates         map[string][]*templates.Template
	ColumnVariableTemplates map[string]*templates.Template
	GlobalVariables         map[string]string
	TableVariables          map[string]string
	RowVariableTemplates    []*templates.Template
}

func prepareTableConfigs(configData config.Config) (map[string]*PreparedTableConfig, error) {
	result := make(map[string]*PreparedTableConfig)
	renderedGlobalVariables, err := renderGlobalVariables(configData)
	if err != nil {
		return nil, fmt.Errorf("cannot render global variables: %w", err)
	}
	for _, tableConfig := range configData.TableConfigs {
		preparedTableConfig, err := func() (*PreparedTableConfig, error) {
			allTemplates := make(map[string]config.Template)
			for name, tmpl := range configData.TableVariables {
				allTemplates[".TableVariables."+name] = tmpl
			}
			for name, tmpl := range tableConfig.TableVariables {
				allTemplates[".TableVariables."+name] = tmpl
			}
			for name, tmpl := range configData.RowVariables {
				allTemplates[".RowVariables."+name] = tmpl
			}
			for name, tmpl := range tableConfig.RowVariables {
				allTemplates[".RowVariables."+name] = tmpl
			}
			for name, tmpl := range configData.ColumnVariables {
				allTemplates[".ColumnVariables."+name] = tmpl
			}
			for name, tmpl := range tableConfig.ColumnVariables {
				allTemplates[".ColumnVariables."+name] = tmpl
			}
			var templatesForDependencyStack []string
			columnTemplates := make(map[string][]*templates.Template)
			for _, columnConfig := range tableConfig.Columns {
				colName := columnConfig.ColumnName
				templateCount := 0
				for i, tmpl := range columnConfig.Templates {
					columnTemplateName := "Column." + colName + "." + strconv.Itoa(i)
					allTemplates[columnTemplateName] = tmpl
					templatesForDependencyStack = append(templatesForDependencyStack, columnTemplateName)
					templateCount++
				}
				columnTemplates[colName] = make([]*templates.Template, templateCount)
			}
			allCompiledTemplates, err := templates.CompileAllTemplates(allTemplates)
			if err != nil {
				return nil, fmt.Errorf("cannot compile all templates: %w", err)
			}
			dependencyStack, err := templates.GetDependencyStackForMultipleTemplates(templatesForDependencyStack, allCompiledTemplates)
			if err != nil {
				return nil, fmt.Errorf("cannot get dependency stack: %w", err)
			}
			var tableVariablesTemplates []*templates.Template
			var rowVariableTemplates []*templates.Template
			columnVariableTemplates := make(map[string]*templates.Template)
			for _, tmpl := range dependencyStack {
				switch {
				case strings.HasPrefix(tmpl.Name, ".TableVariables."):
					tableVariablesTemplates = append(tableVariablesTemplates, tmpl)
				case strings.HasPrefix(tmpl.Name, ".RowVariables."):
					rowVariableTemplates = append(rowVariableTemplates, tmpl)
				case strings.HasPrefix(tmpl.Name, ".ColumnVariables."):
					columnVariableTemplates[tmpl.Name] = tmpl
				case strings.HasPrefix(tmpl.Name, "Column"):
					splitted := strings.Split(tmpl.Name, ".")
					colName := splitted[1]
					colIndex, _ := strconv.Atoi(splitted[2])
					columnTemplates[colName][colIndex] = tmpl
				default:
					return nil, fmt.Errorf("unable to resolve dependency type for: %s", tmpl.Name)
				}
			}
			rendererTableVariables, err := renderTableVariables(tableVariablesTemplates, renderedGlobalVariables)
			if err != nil {
				return nil, fmt.Errorf("cannot render table variables: %w", err)
			}
			preparedTableConfig := &PreparedTableConfig{
				GlobalVariables:         renderedGlobalVariables,
				TableVariables:          rendererTableVariables,
				RowVariableTemplates:    rowVariableTemplates,
				ColumnVariableTemplates: columnVariableTemplates,
				ColumnTemplates:         columnTemplates,
			}
			return preparedTableConfig, nil
		}()
		if err != nil {
			return nil, fmt.Errorf("cannot prepare table config for '%s': %w", tableConfig.TableName, err)
		}
		result[tableConfig.TableName] = preparedTableConfig
	}
	return result, nil
}

func renderTableVariables(
	tableVariableTemplates []*templates.Template,
	globalVariables map[string]string,
) (map[string]string, error) {
	tableVariables := make(map[string]string, len(tableVariableTemplates))
	for _, tmpl := range tableVariableTemplates {
		output := new(bytes.Buffer)
		err := tmpl.CompiledTemplate.Execute(output, struct {
			GlobalVariables map[string]string
			TableVariables  map[string]string
		}{
			GlobalVariables: globalVariables,
			TableVariables:  tableVariables,
		})
		if err != nil {
			return nil, fmt.Errorf("cannot render table variables template '%w': %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".TableVariables.")
		tableVariables[shortName] = output.String()
	}
	return tableVariables, nil
}

func renderGlobalVariables(configData config.Config) (map[string]string, error) {
	compiledGlobalTemplates, err := templates.CompileTemplates(configData.GlobalVariables, "GlobalVariables")
	if err != nil {
		return nil, fmt.Errorf("cannot compile global variables templates: %w", err)
	}
	orderedGlobalTemplates, err := templates.GetOrderedTemplates(compiledGlobalTemplates)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve global variables order: %w", err)
	}
	globalVariables := make(map[string]string, len(orderedGlobalTemplates))
	for _, tmpl := range orderedGlobalTemplates {
		output := new(bytes.Buffer)
		err := tmpl.CompiledTemplate.Execute(output, struct {
			GlobalVariables map[string]string
		}{
			GlobalVariables: globalVariables,
		})
		if err != nil {
			return nil, fmt.Errorf("cannot render global variables template '%w': %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".GlobalVariables.")
		globalVariables[shortName] = output.String()
	}

	return globalVariables, nil
}
