package processor

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/faker"
	"github.com/duffpl/go-mdp/v2/templates"
	"github.com/duffpl/go-mdp/v2/transformations"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	// Do not retain extended INSERT buffers for small field transformations.
	if buf.Cap() <= 64<<10 {
		bufferPool.Put(buf)
	}
}

type Processor struct {
	Config               config.Config
	tableTransformations map[string]*PreparedTableConfig
	processMu            sync.Mutex
	processedTables      []string
	progress             atomic.Pointer[runProgress]
}

func NewProcessorWithConfig(configData config.Config) (*Processor, error) {
	return NewProcessor(configData)
}

func NewProcessor(config config.Config) (*Processor, error) {
	if config.Settings.Workers < 0 || config.Settings.MaxInFlightBytes < 0 || config.Settings.MaxStatementBytes < 0 {
		return nil, fmt.Errorf("worker and memory limits cannot be negative")
	}
	// Faker funcs are bound to the configured locale and text/template resolves
	// them at parse time, so every processor compiles against its own registry.
	// A process-wide cache would hand this processor the templates whichever
	// locale ran first had already compiled.
	registry := templates.NewRegistry(faker.NewWithLocale(config.Settings.Locale).FuncMap())
	tableTransformations, err := prepareTableConfigs(config, registry)
	if err != nil {
		return nil, fmt.Errorf("unable to prepare transformations: %w", err)
	}
	// Expand SkipTables shorthand into tableTransformations
	for _, tableName := range config.SkipTables {
		if existing, ok := tableTransformations[tableName]; ok {
			existing.Skip = true
		} else {
			tableTransformations[tableName] = &PreparedTableConfig{Skip: true}
		}
	}
	p := &Processor{Config: config, tableTransformations: tableTransformations}
	return p, nil
}

// ProcessedTables returns a sorted list of all table names encountered in
// CREATE TABLE statements during the most recent Process() call.
// Must be called after Process() returns.
func (p *Processor) ProcessedTables() []string {
	p.processMu.Lock()
	defer p.processMu.Unlock()
	result := make([]string, len(p.processedTables))
	copy(result, p.processedTables)
	sort.Strings(result)
	return result
}

func schemaFromCreate(stmt *ast.CreateTableStmt) (TableSchema, error) {
	if len(stmt.Cols) == 0 {
		return TableSchema{}, fmt.Errorf("table %s: CREATE TABLE has no column definitions", stmt.Table.Name.O)
	}
	schema := TableSchema{Columns: make(columnMap), Name: stmt.Table.Name.O}
	for i, col := range stmt.Cols {
		schema.Columns[i] = ColumnSchema{Type: col.Tp, Index: i, Name: col.Name.Name.O}
	}
	return schema, nil
}

// Resolve explicit INSERT column order against this statement's immutable schema.
func insertSchema(stmt *ast.InsertStmt, schema TableSchema) (TableSchema, error) {
	if len(stmt.Columns) == 0 {
		return schema, nil
	}
	columns := make(columnMap, len(stmt.Columns))
	seen := make(map[string]bool, len(stmt.Columns))
	for i, name := range stmt.Columns {
		key := strings.ToLower(name.Name.O)
		if seen[key] {
			return TableSchema{}, fmt.Errorf("duplicate INSERT column %s", name.Name.O)
		}
		seen[key] = true
		col, err := schema.Columns.GetByName(name.Name.O)
		if err != nil {
			return TableSchema{}, fmt.Errorf("unknown INSERT column %s", name.Name.O)
		}
		col.Index = i
		columns[i] = col
	}
	schema.Columns = columns
	return schema, nil
}

type rowTemplateData struct {
	Row             transformations.MappedRow
	RowMeta         transformations.RowMeta
	RowVariables    map[string]string
	GlobalVariables map[string]string
	TableVariables  map[string]string
}

type columnTemplateData struct {
	Row             transformations.MappedRow
	RowMeta         transformations.RowMeta
	RowVariables    map[string]string
	GlobalVariables map[string]string
	TableVariables  map[string]string
	ColumnVariables map[string]string
	FieldValue      interface{}
}

func (c *columnTemplateData) setRow(r *rowTemplateData) {
	c.Row, c.RowMeta = r.Row, r.RowMeta
	c.RowVariables, c.GlobalVariables, c.TableVariables = r.RowVariables, r.GlobalVariables, r.TableVariables
}

func mapInsertRowToColumns(insertRow []ast.ExprNode, tableSchema TableSchema, result transformations.MappedRow) error {
	clear(result)
	if len(insertRow) != len(tableSchema.Columns) {
		return fmt.Errorf("INSERT has %d values for %d columns", len(insertRow), len(tableSchema.Columns))
	}
	for i := range insertRow {
		field, ok := insertRow[i].(ast.ValueExpr)
		if !ok {
			return fmt.Errorf("cannot cast column value (%T)", insertRow[i])
		}
		column := tableSchema.Columns[i]
		result[column.Name] = field.GetValue()
	}
	return nil
}

func (p *Processor) processInsertStatement(ctx context.Context, stmt *ast.InsertStmt, tableConfig *PreparedTableConfig, startRowIndex int, schema TableSchema) (string, error) {
	schema, err := insertSchema(stmt, schema)
	if err != nil {
		return "", err
	}
	if len(stmt.Lists) == 0 {
		return "", fmt.Errorf("only INSERT VALUES statements are supported for transformed tables")
	}
	allInsertRows := stmt.Lists
	reusableRow := make(transformations.MappedRow, len(schema.Columns))
	rowData := &rowTemplateData{
		RowVariables:    make(map[string]string),
		GlobalVariables: tableConfig.GlobalVariables,
		TableVariables:  tableConfig.TableVariables,
	}
	columnData := &columnTemplateData{
		ColumnVariables: make(map[string]string),
	}
	for currentRowIndex := range allInsertRows {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		// Use pre-computed row index (no lock needed)
		tableRowIndex := startRowIndex + currentRowIndex
		currentRow := allInsertRows[currentRowIndex]
		err := mapInsertRowToColumns(currentRow, schema, reusableRow)
		if err != nil {
			return "", fmt.Errorf("cannot map row: %w", err)
		}
		clear(rowData.RowVariables)
		rowData.Row = reusableRow
		rowData.RowMeta = transformations.RowMeta{Index: tableRowIndex}

		err = renderRowVariables(
			tableConfig.RowVariableTemplates,
			rowData,
		)

		if err != nil {
			return "", fmt.Errorf("cannot render row variables: %w", err)
		}

		for columnIdx := range currentRow {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			columnSchema := schema.Columns[columnIdx]

			// Check if we have ColumnOps (new path)
			if columnOps, ok := tableConfig.ColumnOps[columnSchema.Name]; ok {
				for _, op := range columnOps {
					switch op.Type {
					case "template":
						columnVariablesTemplates := tableConfig.ColumnVariableDepsPerTemplate[op.CompiledTemplate]
						columnData.setRow(rowData)
						columnData.FieldValue = currentRow[columnIdx].(ast.ValueExpr).GetString()
						clear(columnData.ColumnVariables)
						err = renderColumnVariables(columnVariablesTemplates, columnData)
						if err != nil {
							return "", fmt.Errorf("cannot render column variables: %w", err)
						}
						transformedValue := getBuffer()
						err := op.CompiledTemplate.CompiledTemplate.Execute(transformedValue, columnData)
						if err != nil {
							putBuffer(transformedValue)
							return "", fmt.Errorf("cannot apply transform: %w", err)
						}
						result := transformedValue.String()
						putBuffer(transformedValue)
						currentRow[columnIdx] = ast.NewValueExpr(result, mysql.UTF8Charset, mysql.UTF8Charset)
					case "json":
						currentValue := currentRow[columnIdx].(ast.ValueExpr).GetString()
						columnData.setRow(rowData)
						columnData.FieldValue = currentValue
						clear(columnData.ColumnVariables)
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
			columnTemplates, ok := tableConfig.ColumnTemplates[columnSchema.Name]
			if !ok {
				continue
			}
			for _, tmpl := range columnTemplates {
				if err != nil {
					return "", fmt.Errorf("cannot get transformation function: %w", err)
				}
				columnVariablesTemplates := tableConfig.ColumnVariableDepsPerTemplate[tmpl]
				columnData.setRow(rowData)
				columnData.FieldValue = currentRow[columnIdx].(ast.ValueExpr).GetString()
				clear(columnData.ColumnVariables)
				err = renderColumnVariables(columnVariablesTemplates, columnData)
				if err != nil {
					return "", fmt.Errorf("cannot render column variables: %w", err)
				}
				transformedValue := getBuffer()
				err := tmpl.CompiledTemplate.Execute(transformedValue, columnData)
				if err != nil {
					putBuffer(transformedValue)
					return "", fmt.Errorf("cannot apply transform: %w", err)
				}
				result := transformedValue.String()
				putBuffer(transformedValue)
				currentRow[columnIdx] = ast.NewValueExpr(result, mysql.UTF8Charset, mysql.UTF8Charset)
			}
		}
	}
	buf := getBuffer()
	err = stmt.Restore(format.NewRestoreCtx(restoreFlags, buf))
	if err != nil {
		putBuffer(buf)
		return "", fmt.Errorf("cannot restore insert statement: %w", err)
	}
	buf.WriteString(";\n")
	result := buf.String()
	putBuffer(buf)
	return result, nil
}

func renderRowVariables(
	templates []*templates.Template,
	data *rowTemplateData,
) error {
	for _, tmpl := range templates {
		output := getBuffer()
		err := tmpl.CompiledTemplate.Execute(output, data)
		if err != nil {
			putBuffer(output)
			return fmt.Errorf("cannot render variable '%s' template: %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".RowVariables.")
		data.RowVariables[shortName] = output.String()
		putBuffer(output)
	}
	return nil
}

func renderColumnVariables(
	templates []*templates.Template,
	data *columnTemplateData,
) error {
	for _, tmpl := range templates {
		output := getBuffer()
		err := tmpl.CompiledTemplate.Execute(output, data)
		if err != nil {
			putBuffer(output)
			return fmt.Errorf("cannot render variable '%s' template: %w", tmpl.Name, err)
		}
		shortName, _ := strings.CutPrefix(tmpl.Name, ".ColumnVariables.")
		data.ColumnVariables[shortName] = output.String()
		putBuffer(output)
	}
	return nil
}

var restoreFlags = format.RestoreStringSingleQuotes |
	format.RestoreKeyWordLowercase |
	format.RestoreNameBackQuotes |
	format.RestoreStringEscapeBackslash

type PreparedColumnOp struct {
	Type             string              // "template" or "json"
	CompiledTemplate *templates.Template // for type "template"
	JsonFields       []jsonFieldOp       // for type "json"
}

type PreparedTableConfig struct {
	Skip                          bool
	ColumnOps                     map[string][]PreparedColumnOp
	ColumnTemplates               map[string][]*templates.Template
	ColumnVariableTemplates       map[string]*templates.Template
	ColumnVariableDepsPerTemplate map[*templates.Template][]*templates.Template
	GlobalVariables               map[string]string
	TableVariables                map[string]string
	RowVariableTemplates          []*templates.Template
}

func prepareTableConfigs(configData config.Config, registry *templates.Registry) (map[string]*PreparedTableConfig, error) {
	result := make(map[string]*PreparedTableConfig)
	renderedGlobalVariables, err := renderGlobalVariables(configData, registry)
	if err != nil {
		return nil, fmt.Errorf("cannot render global variables: %w", err)
	}
	for _, tableConfig := range configData.TableConfigs {
		preparedTableConfig, err := func() (*PreparedTableConfig, error) {
			allTemplates := make(map[string]config.Template)
			for name, tmpl := range configData.GlobalVariables {
				allTemplates[".GlobalVariables."+name] = tmpl
			}
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
				colName string
				opIndex int
				tmplIdx int
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
								name := fmt.Sprintf("JSON.%q.%d.%d", colName, opIdx, j)
								allTemplates[name] = f.Template
								templatesForDependencyStack = append(templatesForDependencyStack, name)
								jsonFields[j] = jsonFieldOp{path: f.Path, templateName: name}
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
			allCompiledTemplates, err := registry.CompileAllTemplates(allTemplates)
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
				case strings.HasPrefix(tmpl.Name, ".GlobalVariables."), strings.HasPrefix(tmpl.Name, "Column."), strings.HasPrefix(tmpl.Name, "JSON."):
					// Global values were rendered once; operation roots are wired below.
				default:
					return nil, fmt.Errorf("unable to resolve dependency type for: %s", tmpl.Name)
				}
			}
			// Resolve by the original column key instead of splitting template
			// names: SQL column names can themselves contain dots.
			for colName, list := range columnTemplates {
				for i := range list {
					list[i] = allCompiledTemplates["Column."+colName+"."+strconv.Itoa(i)]
				}
			}
			// Wire up compiled templates into ColumnOps for "template" operations
			for _, mapping := range templateOpMappings {
				compiledTmpl := columnTemplates[mapping.colName][mapping.tmplIdx]
				if ops, ok := columnOps[mapping.colName]; ok {
					ops[mapping.opIndex].CompiledTemplate = compiledTmpl
				}
			}
			// A variable cannot depend on a value evaluated at a later scope.
			scope := func(name string) int {
				for i, prefix := range []string{".GlobalVariables.", ".TableVariables.", ".RowVariables.", ".ColumnVariables."} {
					if strings.HasPrefix(name, prefix) {
						return i
					}
				}
				return 4
			}
			for _, tmpl := range allCompiledTemplates {
				for _, dep := range tmpl.Dependencies {
					if scope(dep.Name) > scope(tmpl.Name) {
						return nil, fmt.Errorf("template %s cannot depend on later-scope variable %s", tmpl.Name, dep.Name)
					}
				}
			}
			// Precompute the transitive, topologically ordered column variables
			// separately for each operation, including each JSON field.
			columnVariableDeps := make(map[*templates.Template][]*templates.Template)
			for _, name := range templatesForDependencyStack {
				stack, err := templates.GetDependencyStackForMultipleTemplates([]string{name}, allCompiledTemplates)
				if err != nil {
					return nil, err
				}
				var deps []*templates.Template
				for _, dep := range stack {
					if strings.HasPrefix(dep.Name, ".ColumnVariables.") {
						deps = append(deps, dep)
					}
				}
				columnVariableDeps[allCompiledTemplates[name]] = deps
			}
			for _, ops := range columnOps {
				for i := range ops {
					for j := range ops[i].JsonFields {
						field := &ops[i].JsonFields[j]
						tmpl := allCompiledTemplates[field.templateName]
						field.template = tmpl.CompiledTemplate
						field.columnVariables = columnVariableDeps[tmpl]
					}
				}
			}
			rendererTableVariables, err := renderTableVariables(tableVariablesTemplates, renderedGlobalVariables)
			if err != nil {
				return nil, fmt.Errorf("cannot render table variables: %w", err)
			}
			preparedTableConfig := &PreparedTableConfig{
				GlobalVariables:               renderedGlobalVariables,
				TableVariables:                rendererTableVariables,
				RowVariableTemplates:          rowVariableTemplates,
				ColumnVariableTemplates:       columnVariableTemplates,
				ColumnVariableDepsPerTemplate: columnVariableDeps,
				ColumnTemplates:               columnTemplates,
				ColumnOps:                     columnOps,
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

func renderGlobalVariables(configData config.Config, registry *templates.Registry) (map[string]string, error) {
	compiledGlobalTemplates, err := registry.CompileTemplates(configData.GlobalVariables, "GlobalVariables")
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
