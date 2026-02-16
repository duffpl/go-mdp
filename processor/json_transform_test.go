package processor

import (
	"strings"
	"testing"
	"text/template"
)

func compileTestTemplate(t *testing.T, tmplStr string) *template.Template {
	t.Helper()
	tmpl, err := template.New("test").Parse(tmplStr)
	if err != nil {
		t.Fatalf("Failed to compile template: %v", err)
	}
	return tmpl
}

func TestApplyJsonTransform_SimpleField(t *testing.T) {
	jsonStr := `{"user":{"firstName":"John","lastName":"Doe"}}`
	fields := []jsonFieldOp{
		{
			path:     "user.firstName",
			template: compileTestTemplate(t, "REDACTED"),
		},
	}

	data := &columnTemplateData{}
	data.GlobalVariables = map[string]string{}
	data.TableVariables = map[string]string{}
	data.RowVariables = map[string]string{}
	data.ColumnVariables = map[string]string{}

	result, err := applyJsonTransform(jsonStr, fields, data)
	if err != nil {
		t.Fatalf("applyJsonTransform failed: %v", err)
	}

	if result == jsonStr {
		t.Errorf("Expected JSON to be modified, got same string back")
	}

	if !strings.Contains(result, `"firstName":"REDACTED"`) && !strings.Contains(result, `"firstName": "REDACTED"`) {
		t.Errorf("Expected firstName to be 'REDACTED', got: %s", result)
	}

	if !strings.Contains(result, `"lastName":"Doe"`) && !strings.Contains(result, `"lastName": "Doe"`) {
		t.Errorf("Expected lastName to be preserved, got: %s", result)
	}
}

func TestApplyJsonTransform_FieldValueInTemplate(t *testing.T) {
	jsonStr := `{"email":"john@real.com"}`
	fields := []jsonFieldOp{
		{
			path:     "email",
			template: compileTestTemplate(t, "was-{{ .FieldValue }}"),
		},
	}

	data := &columnTemplateData{}
	data.GlobalVariables = map[string]string{}
	data.TableVariables = map[string]string{}
	data.RowVariables = map[string]string{}
	data.ColumnVariables = map[string]string{}

	result, err := applyJsonTransform(jsonStr, fields, data)
	if err != nil {
		t.Fatalf("applyJsonTransform failed: %v", err)
	}

	if !strings.Contains(result, `"was-john@real.com"`) {
		t.Errorf("Expected template to use FieldValue, got: %s", result)
	}
}

func TestApplyJsonTransform_ArrayWildcard(t *testing.T) {
	jsonStr := `{"users":[{"name":"Alice"},{"name":"Bob"},{"name":"Charlie"}]}`
	fields := []jsonFieldOp{
		{
			path:     "users.#.name",
			template: compileTestTemplate(t, "ANON"),
		},
	}

	data := &columnTemplateData{}
	data.GlobalVariables = map[string]string{}
	data.TableVariables = map[string]string{}
	data.RowVariables = map[string]string{}
	data.ColumnVariables = map[string]string{}

	result, err := applyJsonTransform(jsonStr, fields, data)
	if err != nil {
		t.Fatalf("applyJsonTransform failed: %v", err)
	}

	if strings.Contains(result, `"Alice"`) || strings.Contains(result, `"Bob"`) || strings.Contains(result, `"Charlie"`) {
		t.Errorf("Expected all names to be anonymized, got: %s", result)
	}
}

func TestApplyJsonTransform_MissingPath(t *testing.T) {
	jsonStr := `{"user":{"firstName":"John"}}`
	fields := []jsonFieldOp{
		{
			path:     "user.middleName",
			template: compileTestTemplate(t, "REDACTED"),
		},
	}

	data := &columnTemplateData{}
	data.GlobalVariables = map[string]string{}
	data.TableVariables = map[string]string{}
	data.RowVariables = map[string]string{}
	data.ColumnVariables = map[string]string{}

	_, err := applyJsonTransform(jsonStr, fields, data)
	if err == nil {
		t.Fatalf("Expected warning/error for missing path, got nil")
	}
}

func TestApplyJsonTransform_InvalidJSON(t *testing.T) {
	jsonStr := `not-valid-json{`
	fields := []jsonFieldOp{
		{
			path:     "user.name",
			template: compileTestTemplate(t, "REDACTED"),
		},
	}

	data := &columnTemplateData{}
	data.GlobalVariables = map[string]string{}
	data.TableVariables = map[string]string{}
	data.RowVariables = map[string]string{}
	data.ColumnVariables = map[string]string{}

	_, err := applyJsonTransform(jsonStr, fields, data)
	if err == nil {
		t.Fatalf("Expected error for invalid JSON, got nil")
	}
}
