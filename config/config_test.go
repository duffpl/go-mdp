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
