package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/duffpl/go-mdp/v2/config"
)

// loadFixture loads a SQL fixture file from testdata/fixtures
func loadFixture(t testing.TB, name string) string {
	t.Helper()
	path := filepath.Join("testdata", "fixtures", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to load fixture %s: %v", name, err)
	}
	return string(data)
}

// loadBenchmarkData loads a SQL benchmark file from testdata/benchmark
func loadBenchmarkData(b *testing.B, name string) string {
	b.Helper()
	path := filepath.Join("testdata", "benchmark", name)
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatalf("Failed to load benchmark data %s: %v", name, err)
	}
	return string(data)
}

// processSQL is a helper that creates a processor and processes the input SQL
func processSQL(t *testing.T, cfg config.Config, input string) (string, error) {
	t.Helper()

	processor, err := NewProcessor(cfg)
	if err != nil {
		return "", err
	}

	inputReader := strings.NewReader(input)
	outputBuffer := &bytes.Buffer{}
	ctx := context.Background()

	err = processor.Process(inputReader, outputBuffer, ctx)
	if err != nil {
		return "", err
	}

	return outputBuffer.String(), nil
}

func TestProcessor_AnonymizeEmail(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"user-{{ .Row.id }}@anonymized.test"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "user-1@anonymized.test") {
		t.Errorf("Expected anonymized email 'user-1@anonymized.test' in output, got:\n%s", output)
	}

	if strings.Contains(output, "john.doe@example.com") {
		t.Errorf("Original email should not be present in output")
	}
}

func TestProcessor_AnonymizeMultipleColumns(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"anon-{{ .Row.id }}@test.com"},
					},
					{
						ColumnName: "first_name",
						Templates:  []config.Template{"FirstName{{ .Row.id }}"},
					},
					{
						ColumnName: "last_name",
						Templates:  []config.Template{"LastName{{ .Row.id }}"},
					},
					{
						ColumnName: "password",
						Templates:  []config.Template{"hashed_password"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	checks := []struct {
		expected    string
		notExpected string
		desc        string
	}{
		{"anon-1@test.com", "john.doe@example.com", "email"},
		{"FirstName1", "John", "first_name"},
		{"LastName1", "Doe", "last_name"},
		{"hashed_password", "secret123", "password"},
	}

	for _, c := range checks {
		if !strings.Contains(output, c.expected) {
			t.Errorf("Expected anonymized %s '%s' in output", c.desc, c.expected)
		}
		if strings.Contains(output, c.notExpected) {
			t.Errorf("Original %s '%s' should not be present in output", c.desc, c.notExpected)
		}
	}
}

func TestProcessor_MD5Template(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"{{ md5 .FieldValue }}@hashed.test"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "@hashed.test") {
		t.Errorf("Expected MD5 hashed email in output, got:\n%s", output)
	}

	if strings.Contains(output, "john.doe@example.com") {
		t.Errorf("Original email should not be present in output")
	}
}

func TestProcessor_PreserveNonConfiguredColumns(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"anonymized@test.com"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "'John'") {
		t.Errorf("Non-configured column 'first_name' should be preserved, got:\n%s", output)
	}
}

func TestProcessor_NonConfiguredTablePassthrough(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"anonymized@test.com"},
					},
				},
			},
		},
	}

	usersSQL := loadFixture(t, "users.sql")
	ordersSQL := loadFixture(t, "orders.sql")
	input := usersSQL + ordersSQL

	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "99.99") {
		t.Errorf("Non-configured table 'orders' should pass through unchanged")
	}
	if !strings.Contains(output, "'completed'") {
		t.Errorf("Non-configured table 'orders' should pass through unchanged")
	}
}

func TestProcessor_MultipleRows(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "members",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "name",
						Templates:  []config.Template{"Member{{ .RowMeta.Index }}"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "members.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "Member1") {
		t.Errorf("Expected 'Member1' in output")
	}
	if !strings.Contains(output, "Member2") {
		t.Errorf("Expected 'Member2' in output")
	}
	if !strings.Contains(output, "Member3") {
		t.Errorf("Expected 'Member3' in output")
	}
}

func TestProcessor_RowVariables(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "contacts",
				RowVariables: map[string]config.Template{
					"anon_id": "anon_{{ .Row.id }}",
				},
				Columns: []config.ColumnConfig{
					{
						ColumnName: "full_name",
						Templates:  []config.Template{"{{ .RowVariables.anon_id }}_name"},
					},
					{
						ColumnName: "email",
						Templates:  []config.Template{"{{ .RowVariables.anon_id }}@anonymized.test"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "contacts.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "anon_42_name") {
		t.Errorf("Expected row variable to be used in full_name, got:\n%s", output)
	}
	if !strings.Contains(output, "anon_42@anonymized.test") {
		t.Errorf("Expected row variable to be used in email, got:\n%s", output)
	}
}

func TestProcessor_DeterministicOutput(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"{{ md5 .FieldValue }}@test.com"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")

	output1, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("First process failed: %v", err)
	}

	output2, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Second process failed: %v", err)
	}

	if !strings.Contains(output1, "@test.com") {
		t.Errorf("First output should contain hashed email, got:\n%s", output1)
	}

	if !strings.Contains(output2, "@test.com") {
		t.Errorf("Second output should contain hashed email, got:\n%s", output2)
	}

	if output1 != output2 {
		t.Errorf("Processor output should be deterministic.\nFirst:\n%s\nSecond:\n%s", output1, output2)
	}
}

func TestProcessor_SpecialCharacters(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "texts",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "content",
						Templates:  []config.Template{"Anonymized content {{ .Row.id }}"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "texts.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL with special characters: %v", err)
	}

	if !strings.Contains(output, "Anonymized content 1") {
		t.Errorf("Expected anonymized content in output, got:\n%s", output)
	}
}

func TestProcessor_EmptyValue(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "profiles",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "bio",
						Templates:  []config.Template{"anon_bio_{{ .Row.id }}"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "profiles.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL with empty value: %v", err)
	}

	if !strings.Contains(output, "anon_bio_1") {
		t.Errorf("Expected anonymized bio in output, got:\n%s", output)
	}
}

func TestProcessor_PostSQL(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"anon@test.com"},
					},
				},
			},
		},
		PostSQL: "\n-- Anonymization complete\nSELECT 'done';\n",
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.HasSuffix(output, "SELECT 'done';\n") {
		t.Errorf("Expected PostSQL at end of output, got:\n%s", output)
	}
}

func TestProcessor_GlobalVariables(t *testing.T) {
	cfg := config.Config{
		GlobalVariables: map[string]config.Template{
			"domain": "anonymized-domain.test",
		},
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"user{{ .Row.id }}@{{ .GlobalVariables.domain }}"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "user1@anonymized-domain.test") {
		t.Errorf("Expected global variable to be used in email, got:\n%s", output)
	}
}

func TestProcessor_TableVariables(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				TableVariables: map[string]config.Template{
					"table_prefix": "users_anon",
				},
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"{{ .TableVariables.table_prefix }}_{{ .Row.id }}@test.com"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "users_anon_1@test.com") {
		t.Errorf("Expected table variable to be used in email, got:\n%s", output)
	}
}

func TestProcessor_MultiValueInsert(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "items",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "name",
						Templates:  []config.Template{"Anon Item {{ .RowMeta.Index }}"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "items.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "Anon Item 1") {
		t.Errorf("Expected 'Anon Item 1' in output")
	}
	if !strings.Contains(output, "Anon Item 2") {
		t.Errorf("Expected 'Anon Item 2' in output")
	}
	if !strings.Contains(output, "Anon Item 3") {
		t.Errorf("Expected 'Anon Item 3' in output")
	}

	if strings.Contains(output, "Item One") {
		t.Errorf("Original 'Item One' should not be present")
	}
}

func TestProcessor_SprigFunctions(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"{{ lower .FieldValue | trunc 10 }}@sprig.test"},
					},
					{
						ColumnName: "first_name",
						Templates:  []config.Template{"{{ upper .FieldValue }}"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "@sprig.test") {
		t.Errorf("Expected Sprig truncated email in output, got:\n%s", output)
	}

	if !strings.Contains(output, "'JOHN'") {
		t.Errorf("Expected Sprig uppercased first_name in output, got:\n%s", output)
	}
}

func TestProcessor_JsonTransform_SimpleField(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "events",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "metadata",
						Operations: []config.ColumnOperation{
							{
								Type: "json",
								JsonFields: []config.JsonFieldConfig{
									{Path: "user.firstName", Template: "REDACTED"},
									{Path: "user.lastName", Template: "REDACTED"},
								},
							},
						},
					},
				},
			},
		},
	}

	input := loadFixture(t, "events.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if strings.Contains(output, "John") {
		t.Errorf("Original firstName 'John' should not be present in output, got:\n%s", output)
	}
	if strings.Contains(output, "Doe") {
		t.Errorf("Original lastName 'Doe' should not be present in output, got:\n%s", output)
	}
	if !strings.Contains(output, "REDACTED") {
		t.Errorf("Expected 'REDACTED' in output, got:\n%s", output)
	}
}

func TestProcessor_JsonTransform_ArrayWildcard(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "events",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "metadata",
						Operations: []config.ColumnOperation{
							{
								Type: "json",
								JsonFields: []config.JsonFieldConfig{
									{Path: "tags.#.value", Template: "anon-tag"},
								},
							},
						},
					},
				},
			},
		},
	}

	input := loadFixture(t, "events_with_arrays.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "anon-tag") {
		t.Errorf("Expected 'anon-tag' in output, got:\n%s", output)
	}
}

func TestProcessor_JsonTransform_WithFieldValue(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "events",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "metadata",
						Operations: []config.ColumnOperation{
							{
								Type: "json",
								JsonFields: []config.JsonFieldConfig{
									{Path: "user.email", Template: "{{ md5 .FieldValue }}@anon.test"},
								},
							},
						},
					},
				},
			},
		},
	}

	input := loadFixture(t, "events.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if strings.Contains(output, "john@real.com") {
		t.Errorf("Original email should not be present in output")
	}
	if !strings.Contains(output, "@anon.test") {
		t.Errorf("Expected anonymized email in output, got:\n%s", output)
	}
}

func TestProcessor_JsonTransform_MixedWithTemplate(t *testing.T) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "events",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "metadata",
						Operations: []config.ColumnOperation{
							{
								Type: "json",
								JsonFields: []config.JsonFieldConfig{
									{Path: "user.firstName", Template: "ANON"},
								},
							},
						},
					},
					{
						ColumnName: "event_type",
						Operations: []config.ColumnOperation{
							{
								Type:     "template",
								Template: "anonymized_event",
							},
						},
					},
				},
			},
		},
	}

	input := loadFixture(t, "events.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "ANON") {
		t.Errorf("Expected JSON field anonymized in output")
	}
	if !strings.Contains(output, "anonymized_event") {
		t.Errorf("Expected template-transformed event_type in output")
	}
}

func TestProcessor_JsonTransform_FromJSONConfig(t *testing.T) {
	configJSON := `{
		"tables": [{
			"name": "events",
			"columns": [{
				"name": "metadata",
				"transformations": [{
					"type": "json",
					"options": {
						"fields": [
							{"path": "user.firstName", "template": "ANON_FIRST"},
							{"path": "user.lastName", "template": "ANON_LAST"}
						]
					}
				}]
			}]
		}]
	}`

	var cfg config.Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	input := loadFixture(t, "events.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if strings.Contains(output, "John") {
		t.Errorf("Original firstName should be anonymized, got:\n%s", output)
	}
	if !strings.Contains(output, "ANON_FIRST") {
		t.Errorf("Expected 'ANON_FIRST' in output, got:\n%s", output)
	}
	if !strings.Contains(output, "ANON_LAST") {
		t.Errorf("Expected 'ANON_LAST' in output, got:\n%s", output)
	}
}

func TestProcessor_SkipTables_Shorthand(t *testing.T) {
	cfg := config.Config{
		SkipTables: []string{"audit_log"},
	}

	input := loadFixture(t, "audit_log.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "CREATE TABLE") {
		t.Error("CREATE TABLE should be preserved")
	}
	lowered := strings.ToLower(output)
	if strings.Contains(lowered, "insert into") {
		t.Error("INSERT should be dropped for tables in SkipTables")
	}
}

func TestProcessor_SkipTables_MergesWithExistingTableConfig(t *testing.T) {
	cfg := config.Config{
		SkipTables: []string{"users"},
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"anon@test.com"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "users.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	lowered := strings.ToLower(output)
	// Skip takes precedence — INSERT should be dropped entirely, not transformed
	if strings.Contains(lowered, "insert into") {
		t.Error("INSERT should be dropped when SkipTables overrides TableConfig")
	}
	if strings.Contains(output, "anon@test.com") {
		t.Error("Transformations should not be applied when skip takes precedence")
	}
}

func TestProcessor_ProcessedTables_EmptyInput(t *testing.T) {
	cfg := config.Config{}
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	input := strings.NewReader("")
	err = processor.Process(input, &bytes.Buffer{}, context.Background())
	if err != nil {
		t.Fatalf("Failed to process: %v", err)
	}

	tables := processor.ProcessedTables()
	if len(tables) != 0 {
		t.Errorf("Expected empty table list, got %v", tables)
	}
}

func TestProcessor_ProcessedTables_SingleTable(t *testing.T) {
	cfg := config.Config{}
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	input := strings.NewReader(loadFixture(t, "users.sql"))
	err = processor.Process(input, &bytes.Buffer{}, context.Background())
	if err != nil {
		t.Fatalf("Failed to process: %v", err)
	}

	tables := processor.ProcessedTables()
	if len(tables) != 1 {
		t.Fatalf("Expected 1 table, got %d: %v", len(tables), tables)
	}
	if tables[0] != "users" {
		t.Errorf("Expected 'users', got '%s'", tables[0])
	}
}

func TestProcessor_ProcessedTables_MultipleTables_Sorted(t *testing.T) {
	cfg := config.Config{}
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	fixture := loadFixture(t, "users.sql") + loadFixture(t, "orders.sql")
	input := strings.NewReader(fixture)
	err = processor.Process(input, &bytes.Buffer{}, context.Background())
	if err != nil {
		t.Fatalf("Failed to process: %v", err)
	}

	tables := processor.ProcessedTables()
	if len(tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d: %v", len(tables), tables)
	}
	if tables[0] != "orders" || tables[1] != "users" {
		t.Errorf("Expected [orders, users], got %v", tables)
	}
}

func TestProcessor_ProcessedTables_SkippedTablesStillAppear(t *testing.T) {
	cfg := config.Config{
		SkipTables: []string{"audit_log"},
	}
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	fixture := loadFixture(t, "audit_log.sql") + loadFixture(t, "users.sql")
	input := strings.NewReader(fixture)
	err = processor.Process(input, &bytes.Buffer{}, context.Background())
	if err != nil {
		t.Fatalf("Failed to process: %v", err)
	}

	tables := processor.ProcessedTables()
	if len(tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d: %v", len(tables), tables)
	}
	found := map[string]bool{}
	for _, name := range tables {
		found[name] = true
	}
	if !found["audit_log"] {
		t.Error("Skipped table 'audit_log' should still appear in ProcessedTables")
	}
	if !found["users"] {
		t.Error("Table 'users' should appear in ProcessedTables")
	}
}

func TestProcessor_ProcessedTables_BeforeProcess(t *testing.T) {
	cfg := config.Config{}
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	tables := processor.ProcessedTables()
	if len(tables) != 0 {
		t.Errorf("Expected empty list before Process(), got %v", tables)
	}
}

func TestProcessor_ProcessedTables_ReturnsNewSlice(t *testing.T) {
	cfg := config.Config{}
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	input := strings.NewReader(loadFixture(t, "users.sql"))
	err = processor.Process(input, &bytes.Buffer{}, context.Background())
	if err != nil {
		t.Fatalf("Failed to process: %v", err)
	}

	tables1 := processor.ProcessedTables()
	tables2 := processor.ProcessedTables()
	if len(tables1) > 0 {
		tables1[0] = "MUTATED"
	}
	if len(tables2) > 0 && tables2[0] == "MUTATED" {
		t.Error("ProcessedTables should return a new slice each call")
	}
}

func TestProcessor_ProcessedTables_ResetsOnSecondProcess(t *testing.T) {
	cfg := config.Config{}
	processor, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	// First Process call with users.sql
	input1 := strings.NewReader(loadFixture(t, "users.sql"))
	err = processor.Process(input1, &bytes.Buffer{}, context.Background())
	if err != nil {
		t.Fatalf("First Process failed: %v", err)
	}
	tables1 := processor.ProcessedTables()
	if len(tables1) != 1 || tables1[0] != "users" {
		t.Fatalf("Expected [users] after first Process, got %v", tables1)
	}

	// Second Process call with orders.sql
	input2 := strings.NewReader(loadFixture(t, "orders.sql"))
	err = processor.Process(input2, &bytes.Buffer{}, context.Background())
	if err != nil {
		t.Fatalf("Second Process failed: %v", err)
	}
	tables2 := processor.ProcessedTables()
	if len(tables2) != 1 || tables2[0] != "orders" {
		t.Errorf("Expected [orders] after second Process (reset), got %v", tables2)
	}
}

func TestProcessor_SkipTable_WithColumnConfigs_SkipTakesPrecedence(t *testing.T) {
	cfg := config.Config{
		SkipTables: []string{"audit_log"},
		TableConfigs: []config.TableConfig{
			{
				TableName: "audit_log",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "details",
						Templates:  []config.Template{"REDACTED"},
					},
				},
			},
		},
	}

	input := loadFixture(t, "audit_log.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	lowered := strings.ToLower(output)
	if strings.Contains(lowered, "insert into") {
		t.Error("INSERT should be dropped even when column configs exist (skip takes precedence)")
	}
	if strings.Contains(output, "REDACTED") {
		t.Error("Column transformations should not run when skip is true")
	}
	if !strings.Contains(output, "CREATE TABLE") {
		t.Error("CREATE TABLE should be preserved")
	}
}

func TestProcessor_SkipTable_Mixed_SkipAndTransformAndPassthrough(t *testing.T) {
	cfg := config.Config{
		SkipTables: []string{"audit_log"},
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"anon@test.com"},
					},
				},
			},
		},
	}

	// audit_log = skipped, users = transformed, orders = passthrough
	input := loadFixture(t, "audit_log.sql") + loadFixture(t, "users.sql") + loadFixture(t, "orders.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	// audit_log: CREATE preserved, INSERT dropped
	if !strings.Contains(output, "CREATE TABLE `audit_log`") {
		t.Error("audit_log CREATE TABLE should be preserved")
	}
	lowered := strings.ToLower(output)
	if strings.Contains(lowered, "'login'") {
		t.Error("audit_log INSERT data should be dropped")
	}

	// users: transformed
	if !strings.Contains(output, "anon@test.com") {
		t.Error("users email should be anonymized")
	}
	if strings.Contains(output, "john.doe@example.com") {
		t.Error("Original email should not be present")
	}

	// orders: passthrough
	if !strings.Contains(output, "99.99") {
		t.Error("orders should pass through unchanged")
	}
}

func TestProcessor_SkipTable_MultiValueInsert(t *testing.T) {
	cfg := config.Config{
		SkipTables: []string{"items"},
	}

	input := loadFixture(t, "items.sql")
	output, err := processSQL(t, cfg, input)
	if err != nil {
		t.Fatalf("Failed to process SQL: %v", err)
	}

	if !strings.Contains(output, "CREATE TABLE") {
		t.Error("CREATE TABLE should be preserved")
	}
	lowered := strings.ToLower(output)
	if strings.Contains(lowered, "insert into") {
		t.Error("Multi-value INSERT should be dropped entirely")
	}
	if strings.Contains(output, "Item One") {
		t.Error("INSERT data should not be present")
	}
}

// Benchmark configuration for anonymizing benchmark_users table
func benchmarkConfig() config.Config {
	return config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "benchmark_users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"user-{{ .Row.id }}@anonymized.test"},
					},
					{
						ColumnName: "first_name",
						Templates:  []config.Template{"{{ md5 .FieldValue }}"},
					},
					{
						ColumnName: "last_name",
						Templates:  []config.Template{"{{ md5 .FieldValue }}"},
					},
					{
						ColumnName: "phone",
						Templates:  []config.Template{"+1-555-{{ .RowMeta.Index }}"},
					},
					{
						ColumnName: "address",
						Templates:  []config.Template{"{{ .RowMeta.Index }} Anonymous Street"},
					},
					{
						ColumnName: "company",
						Templates:  []config.Template{"Company {{ .Row.id }}"},
					},
					{
						ColumnName: "notes",
						Templates:  []config.Template{"Anonymized notes for user {{ .Row.id }}"},
					},
				},
			},
		},
	}
}

func BenchmarkProcessor_Small_100rows(b *testing.B) {
	cfg := benchmarkConfig()
	input := loadBenchmarkData(b, "small.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}

func BenchmarkProcessor_Medium_1000rows(b *testing.B) {
	cfg := benchmarkConfig()
	input := loadBenchmarkData(b, "medium.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}

func BenchmarkProcessor_Large_10000rows(b *testing.B) {
	cfg := benchmarkConfig()
	input := loadBenchmarkData(b, "large.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}

func BenchmarkProcessor_XLarge_100000rows(b *testing.B) {
	cfg := benchmarkConfig()
	input := loadBenchmarkData(b, "xlarge.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}

func BenchmarkProcessor_XXLarge_500000rows(b *testing.B) {
	cfg := benchmarkConfig()
	input := loadBenchmarkData(b, "xxlarge.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}

func BenchmarkProcessor_Huge_2000000rows(b *testing.B) {
	cfg := benchmarkConfig()
	input := loadBenchmarkData(b, "huge.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}

func BenchmarkProcessor_MD5_Only(b *testing.B) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "benchmark_users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"{{ md5 .FieldValue }}@hashed.test"},
					},
				},
			},
		},
	}
	input := loadBenchmarkData(b, "medium.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}

func BenchmarkProcessor_SimpleTemplate(b *testing.B) {
	cfg := config.Config{
		TableConfigs: []config.TableConfig{
			{
				TableName: "benchmark_users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "email",
						Templates:  []config.Template{"user{{ .Row.id }}@test.com"},
					},
				},
			},
		},
	}
	input := loadBenchmarkData(b, "medium.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor, err := NewProcessor(cfg)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}
		inputReader := strings.NewReader(input)
		outputBuffer := &bytes.Buffer{}
		ctx := context.Background()
		_ = processor.Process(inputReader, outputBuffer, ctx)
	}
}
