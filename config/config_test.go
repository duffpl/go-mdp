package config

import (
	"encoding/json"
	"testing"
)

func TestColumnConfig_UnmarshalJSON_TemplateType(t *testing.T) {
	input := `{
		"name": "email",
		"transformations": [
			{"type": "template", "options": {"template": "{{ .FieldValue }}@anon.test"}}
		]
	}`
	var col ColumnConfig
	err := json.Unmarshal([]byte(input), &col)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if col.ColumnName != "email" {
		t.Errorf("Expected column name 'email', got '%s'", col.ColumnName)
	}
	if len(col.Operations) != 1 {
		t.Fatalf("Expected 1 operation, got %d", len(col.Operations))
	}
	op := col.Operations[0]
	if op.Type != "template" {
		t.Errorf("Expected type 'template', got '%s'", op.Type)
	}
	if op.Template != "{{ .FieldValue }}@anon.test" {
		t.Errorf("Expected template string, got '%s'", op.Template)
	}
}

func TestColumnConfig_UnmarshalJSON_JsonType(t *testing.T) {
	input := `{
		"name": "metadata",
		"transformations": [
			{
				"type": "json",
				"options": {
					"fields": [
						{"path": "user.firstName", "template": "{{ transformFirstName .FieldValue }}"},
						{"path": "contacts.#.email", "template": "anon-{{ .RowMeta.Index }}@test.com"}
					]
				}
			}
		]
	}`
	var col ColumnConfig
	err := json.Unmarshal([]byte(input), &col)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(col.Operations) != 1 {
		t.Fatalf("Expected 1 operation, got %d", len(col.Operations))
	}
	op := col.Operations[0]
	if op.Type != "json" {
		t.Errorf("Expected type 'json', got '%s'", op.Type)
	}
	if len(op.JsonFields) != 2 {
		t.Fatalf("Expected 2 json fields, got %d", len(op.JsonFields))
	}
	if op.JsonFields[0].Path != "user.firstName" {
		t.Errorf("Expected path 'user.firstName', got '%s'", op.JsonFields[0].Path)
	}
	if op.JsonFields[1].Path != "contacts.#.email" {
		t.Errorf("Expected path 'contacts.#.email', got '%s'", op.JsonFields[1].Path)
	}
}

func TestColumnConfig_UnmarshalJSON_MixedTypes(t *testing.T) {
	input := `{
		"name": "data",
		"transformations": [
			{"type": "template", "options": {"template": "prefix-{{ .FieldValue }}"}},
			{"type": "json", "options": {"fields": [{"path": "name", "template": "anon"}]}}
		]
	}`
	var col ColumnConfig
	err := json.Unmarshal([]byte(input), &col)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(col.Operations) != 2 {
		t.Fatalf("Expected 2 operations, got %d", len(col.Operations))
	}
	if col.Operations[0].Type != "template" {
		t.Errorf("Expected first op type 'template', got '%s'", col.Operations[0].Type)
	}
	if col.Operations[1].Type != "json" {
		t.Errorf("Expected second op type 'json', got '%s'", col.Operations[1].Type)
	}
}

func TestColumnConfig_UnmarshalJSON_DefaultType(t *testing.T) {
	// When type is omitted, it should default to "template"
	input := `{
		"name": "email",
		"transformations": [
			{"options": {"template": "{{ .FieldValue }}"}}
		]
	}`
	var col ColumnConfig
	err := json.Unmarshal([]byte(input), &col)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(col.Operations) != 1 {
		t.Fatalf("Expected 1 operation, got %d", len(col.Operations))
	}
	if col.Operations[0].Type != "template" {
		t.Errorf("Expected default type 'template', got '%s'", col.Operations[0].Type)
	}
}

func TestTableConfig_UnmarshalJSON_SkipField(t *testing.T) {
	input := `{
		"tables": [
			{"name": "audit_log", "skip": true},
			{"name": "users", "columns": []}
		]
	}`
	var cfg Config
	if err := json.Unmarshal([]byte(input), &cfg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(cfg.TableConfigs) != 2 {
		t.Fatalf("Expected 2 table configs, got %d", len(cfg.TableConfigs))
	}
	if !cfg.TableConfigs[0].Skip {
		t.Error("Expected audit_log to have Skip=true")
	}
	if cfg.TableConfigs[0].TableName != "audit_log" {
		t.Errorf("Expected table name 'audit_log', got '%s'", cfg.TableConfigs[0].TableName)
	}
	if cfg.TableConfigs[1].Skip {
		t.Error("Expected users to have Skip=false")
	}
}

func TestConfig_SkipTablesField(t *testing.T) {
	input := `{
		"skipTables": ["cache_entries", "job_queue"]
	}`
	var cfg Config
	if err := json.Unmarshal([]byte(input), &cfg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(cfg.SkipTables) != 2 {
		t.Fatalf("Expected 2 skip tables, got %d", len(cfg.SkipTables))
	}
	if cfg.SkipTables[0] != "cache_entries" {
		t.Errorf("Expected 'cache_entries', got '%s'", cfg.SkipTables[0])
	}
	if cfg.SkipTables[1] != "job_queue" {
		t.Errorf("Expected 'job_queue', got '%s'", cfg.SkipTables[1])
	}
}
