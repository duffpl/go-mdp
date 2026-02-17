package processor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/bobg/go-generics/v3/slices"
	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/templates"
	"github.com/duffpl/go-mdp/v2/transformations"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

type Processor struct {
	Config               config.Config
	tableTransformations map[string]*PreparedTableConfig
	globalVariables map[string]string
	schemaMapLock   *sync.Mutex
	tableSchemas         map[string]TableSchema
	schemaReadyCond      *sync.Cond // condition variable to wait for schema
}

func NewProcessorWithConfig(configData config.Config) (*Processor, error) {
	return NewProcessor(configData)
}


func NewProcessor(config config.Config) (*Processor, error) {
	tableTransformations, err := prepareTableConfigs(config)
	if err != nil {
		return nil, fmt.Errorf("unable to prepare transformations: %w", err)
	}
	schemaLock := &sync.Mutex{}
	p := &Processor{
		Config:               config,
		tableTransformations: tableTransformations,
		tableSchemas:         make(map[string]TableSchema),
		schemaMapLock:        schemaLock,
		schemaReadyCond:      sync.NewCond(schemaLock),
	}
	globalVariables, err := renderGlobalVariables(config)
	if err != nil {
		return nil, fmt.Errorf("cannot render global variables: %w", err)
	}
	p.globalVariables = globalVariables
	return p, nil
}

func (p *Processor) processCreateTableStatement(stmt *ast.CreateTableStmt) {
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
	p.schemaMapLock.Lock()
	p.tableSchemas[tableName] = schema
	p.schemaReadyCond.Broadcast() // wake up any goroutines waiting for this schema
	p.schemaMapLock.Unlock()
}

// waitForSchema blocks until the schema for tableName is available or context is cancelled
func (p *Processor) waitForSchema(ctx context.Context, tableName string) (TableSchema, error) {
	p.schemaMapLock.Lock()
	defer p.schemaMapLock.Unlock()
	for {
		if schema, ok := p.tableSchemas[tableName]; ok {
			return schema, nil
		}
		if ctx.Err() != nil {
			return TableSchema{}, fmt.Errorf("context cancelled while waiting for schema of table %s: %w", tableName, ctx.Err())
		}
		// Wake up when context is cancelled so we don't block forever
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				p.schemaReadyCond.Broadcast()
			case <-done:
			}
		}()
		p.schemaReadyCond.Wait()
		close(done)
	}
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

func (p *Processor) processInsertStatement(ctx context.Context, stmt *ast.InsertStmt, tableConfig *PreparedTableConfig, startRowIndex int) (string, error) {
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	// Wait for schema to be available (CREATE TABLE must be processed first)
	schema, err := p.waitForSchema(ctx, tableName)
	if err != nil {
		return "", err
	}
	allInsertRows := stmt.Lists
	for currentRowIndex := range allInsertRows {
		// Use pre-computed row index (no lock needed)
		tableRowIndex := startRowIndex + currentRowIndex
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

			// Check if we have ColumnOps (new path)
			if columnOps, ok := tableConfig.ColumnOps[columnSchema.Name]; ok {
				for _, op := range columnOps {
					switch op.Type {
					case "template":
						currentColumn := currentRow[columnIdx]
						columnVariablesTemplates := slices.Filter(op.CompiledTemplate.Dependencies, func(tmpl *templates.Template) bool {
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
						err := op.CompiledTemplate.CompiledTemplate.Execute(transformedValue, columnData)
						if err != nil {
							return "", fmt.Errorf("cannot apply transform: %w", err)
						}
						currentRow[columnIdx] = ast.NewValueExpr(transformedValue.String(), mysql.UTF8Charset, mysql.UTF8Charset)
					case "json":
						currentValue := currentRow[columnIdx].(ast.ValueExpr).GetString()
						columnData := &columnTemplateData{
							rowTemplateData: *rowData,
							FieldValue:      currentValue,
							ColumnVariables: make(map[string]string),
						}
						transformedJSON, err := applyJsonTransform(currentValue, op.JsonFields, columnData)
						if err != nil {
							return "", fmt.Errorf("JSON transform failed for column '%s': %w", columnSchema.Name, err)
						}
						currentRow[columnIdx] = ast.NewValueExpr(transformedJSON, mysql.UTF8Charset, mysql.UTF8Charset)
					}
				}
				continue
			}

			// Legacy path: use ColumnTemplates
			currentColumn := currentRow[columnIdx]
			columnTemplates, ok := tableConfig.ColumnTemplates[columnSchema.Name]
			if !ok {
				continue
			}
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
	err = stmt.Restore(format.NewRestoreCtx(restoreFlags, buf))
	if err != nil {
		return "", fmt.Errorf("cannot restore insert statement: %w", err)
	}
	return buf.String() + ";\n", nil
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
		currentStatementLine := strings.Builder{}
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
			if err != nil && err != io.EOF {
				errCh <- err
				return
			}
			isEOF := err == io.EOF
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				if isEOF {
					return
				}
				outputCh <- line
				continue
			}
			_, err = currentStatementLine.Write([]byte(line + "\n"))
			if err != nil {
				errCh <- err
				return
			}
			lastCharacter := line[len(line)-1:]
			if lastCharacter == ";" {
				outputCh <- currentStatementLine.String()
				currentStatementLine = strings.Builder{}
			}
			if isEOF {
				remaining := strings.TrimSpace(currentStatementLine.String())
				if len(remaining) > 0 {
					outputCh <- remaining + "\n"
				}
				return
			}
		}
	}()
	return outputCh, errCh
}

func (p Processor) processLine(ctx context.Context, line string, parser *parser.Parser, startRowIndex int) (string, error) {
	var tableName string
	preparseResult := preparse(line)
	switch preparseResult.(type) {
	case nil:
		return line, nil // passthrough: not a CREATE/INSERT
	case preparsedStatementWithTable:
		tableName = preparseResult.(preparsedStatementWithTable).GetTableName()
	}
	tableTransformations, ok := p.tableTransformations[tableName]
	if !ok {
		return line, nil // passthrough: table not configured
	}
	parseResult, _, err := parser.Parse(line, mysql.UTF8Charset, mysql.UTF8Charset)
	if err != nil {
		return line, fmt.Errorf("cannot parse statement for table %s: %w", tableName, err)
	}
	statement := parseResult[0]
	switch statement.(type) {
	case *ast.InsertStmt:
		line, err = p.processInsertStatement(ctx, statement.(*ast.InsertStmt), tableTransformations, startRowIndex)
		if err != nil {
			return line, fmt.Errorf("cannot process insert statement for table %s: %w", tableName, err)
		}
	case *ast.CreateTableStmt:
		p.processCreateTableStatement(statement.(*ast.CreateTableStmt))
	}
	return line, nil
}

type lineWithOutputChannel struct {
	line          string
	outputChannel chan string
	startRowIndex int // pre-computed starting row index for this statement
}

// countInsertRows quickly counts the number of value tuples in an INSERT statement
// by counting occurrences of "),(" plus 1 for the first tuple
var rowCountRegex = regexp.MustCompile(`\),\s*\(`)

func countInsertRows(line string) int {
	matches := rowCountRegex.FindAllStringIndex(line, -1)
	return len(matches) + 1 // +1 for the first tuple
}

func (p Processor) processLines(input chan string, ctx context.Context) (chan chan string, chan error) {
	outputCh := make(chan chan string, 100)
	errCh := make(chan error, 1) // buffered to ensure error is never dropped
	linesForProcessing := make(chan lineWithOutputChannel, 100)
	processorCount := 8
	lineProcessorsWg := sync.WaitGroup{}

	// Create a cancellable context for coordinated shutdown
	processingCtx, cancelProcessing := context.WithCancel(ctx)

	// Row counters per table - only accessed by dispatcher goroutine (no lock needed)
	tableRowCounters := make(map[string]int)

	go func() {
		defer close(linesForProcessing)
		for line := range input {
			select {
			case <-processingCtx.Done():
				// Stop creating new work if processing is cancelled
				return
			default:
			}

			// Pre-compute row index for INSERT statements
			var startRowIndex int
			preparsed := preparse(line)
			if insertStmt, ok := preparsed.(preparsedInsertStmt); ok {
				name := insertStmt.GetTableName()
				rowCount := countInsertRows(line)
				startRowIndex = tableRowCounters[name] + 1 // 1-based indexing
				tableRowCounters[name] += rowCount
			}

			processedCh := make(chan string, 1) // buffered to prevent blocking
			select {
			case outputCh <- processedCh:
			case <-processingCtx.Done():
				close(processedCh)
				return
			}
			select {
			case linesForProcessing <- lineWithOutputChannel{
				line:          line,
				outputChannel: processedCh,
				startRowIndex: startRowIndex,
			}:
			case <-processingCtx.Done():
				close(processedCh)
				return
			}
		}
	}()

	for i := 0; i < processorCount; i++ {
		lineProcessorsWg.Add(1)
		go func() {
			defer lineProcessorsWg.Done()
			stmtParser := parser.New()
			for {
				select {
				case <-processingCtx.Done():
					// Drain remaining work and close orphaned channels
					for work := range linesForProcessing {
						close(work.outputChannel)
					}
					return
				case currentLine, ok := <-linesForProcessing:
					if !ok {
						return
					}
					processedLine, err := p.processLine(processingCtx, currentLine.line, stmtParser, currentLine.startRowIndex)
					if err != nil {
						currentLine.outputChannel <- fmt.Sprintf("/* error: %s */\n%s", err.Error(), currentLine.line)
						// Cancel processing context to stop all goroutines
						cancelProcessing()
						// Drain remaining work and close orphaned channels FIRST
						// This allows the main loop to finish reading from those channels
						for work := range linesForProcessing {
							close(work.outputChannel)
						}
						// Now send the error - main loop can receive it
						select {
						case errCh <- err:
						default:
						}
						return
					}
					currentLine.outputChannel <- processedLine
				}
			}
		}()
	}
	go func() {
		lineProcessorsWg.Wait()
		cancelProcessing() // Ensure context is cancelled when workers finish
		close(outputCh)
		close(errCh)
	}()
	return outputCh, errCh
}

var restoreFlags = format.RestoreStringSingleQuotes |
	format.RestoreKeyWordLowercase |
	format.RestoreNameBackQuotes |
	format.RestoreStringEscapeBackslash


func (p Processor) Process(input io.Reader, output io.Writer, pCtx context.Context) (err error) {
	readLines, inputErrors := readStatements(input, pCtx)
	processedLinesChans, processingErrors := p.processLines(readLines, pCtx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for {
			select {
			case <-pCtx.Done():
				done <- pCtx.Err()
				return
			case processedLineCh, ok := <-processedLinesChans:
				if !ok {
					return
				}
				processedLine, ok := <-processedLineCh
				if !ok {
					// Channel was closed (orphaned work), continue to check for errors
					continue
				}

				if output != nil && pCtx.Err() == nil {
					_, err = output.Write([]byte(processedLine))
					if err != nil {
						done <- fmt.Errorf("output error: %w", err)
						return
					}
				}
			case err = <-inputErrors:
				if err != nil {
					done <- fmt.Errorf("input error: %w", err)
					return
				}
			case err = <-processingErrors:
				if err != nil {
					done <- fmt.Errorf("processing error: %w", err)
					return
				}
			}
		}
	}()
	select {
	case <-pCtx.Done():
		return pCtx.Err()
	case err = <-done:
		if err == nil && p.Config.PostSQL != "" {
			_, err = output.Write([]byte(p.Config.PostSQL))
		}
		return err
	}
}


type PreparedColumnOp struct {
	Type             string             // "template" or "json"
	CompiledTemplate *templates.Template // for type "template"
	JsonFields       []jsonFieldOp      // for type "json"
}

type PreparedTableConfig struct {
	ColumnOps               map[string][]PreparedColumnOp
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
			columnOps := make(map[string][]PreparedColumnOp)
			// Track which template indices map to which operation for later assembly
			type templateOpMapping struct {
				colName  string
				opIndex  int
				tmplIdx  int
			}
			var templateOpMappings []templateOpMapping
			for _, columnConfig := range tableConfig.Columns {
				colName := columnConfig.ColumnName
				if len(columnConfig.Operations) > 0 {
					// New path: build ops from Operations
					ops := make([]PreparedColumnOp, len(columnConfig.Operations))
					templateCount := 0
					for opIdx, op := range columnConfig.Operations {
						switch op.Type {
						case "template":
							tmplName := "Column." + colName + "." + strconv.Itoa(templateCount)
							allTemplates[tmplName] = op.Template
							templatesForDependencyStack = append(templatesForDependencyStack, tmplName)
							templateOpMappings = append(templateOpMappings, templateOpMapping{colName, opIdx, templateCount})
							templateCount++
							ops[opIdx] = PreparedColumnOp{Type: "template"}
						case "json":
							jsonFields := make([]jsonFieldOp, len(op.JsonFields))
							for j, f := range op.JsonFields {
								compiled, err := templates.GetCompiledTemplate(string(f.Template), fmt.Sprintf("json.%s.%s", colName, f.Path))
								if err != nil {
									return nil, fmt.Errorf("cannot compile JSON field template for path '%s': %w", f.Path, err)
								}
								jsonFields[j] = jsonFieldOp{
									path:     f.Path,
									template: compiled,
								}
							}
							ops[opIdx] = PreparedColumnOp{Type: "json", JsonFields: jsonFields}
						}
					}
					columnOps[colName] = ops
					columnTemplates[colName] = make([]*templates.Template, templateCount)
				} else {
					// Legacy path: build from Templates
					templateCount := 0
					for i, tmpl := range columnConfig.Templates {
						columnTemplateName := "Column." + colName + "." + strconv.Itoa(i)
						allTemplates[columnTemplateName] = tmpl
						templatesForDependencyStack = append(templatesForDependencyStack, columnTemplateName)
						templateCount++
					}
					columnTemplates[colName] = make([]*templates.Template, templateCount)
				}
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
			// Wire up compiled templates into ColumnOps for "template" operations
			for _, mapping := range templateOpMappings {
				compiledTmpl := columnTemplates[mapping.colName][mapping.tmplIdx]
				if ops, ok := columnOps[mapping.colName]; ok {
					ops[mapping.opIndex].CompiledTemplate = compiledTmpl
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
				ColumnOps:               columnOps,
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
			return nil, fmt.Errorf("cannot render table variables template '%s': %w", tmpl.Name, err)
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
			return nil, fmt.Errorf("cannot render global variables template '%s': %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".GlobalVariables.")
		globalVariables[shortName] = output.String()
	}

	return globalVariables, nil
}
