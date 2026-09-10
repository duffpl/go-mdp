package processor

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/duffpl/go-mdp/v2/templates"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type jsonFieldOp struct {
	path            string
	template        *template.Template
	templateName    string
	columnVariables []*templates.Template
}

// JSON column variables are scoped to each field/array element, whose current
// value becomes .FieldValue. Never carry a previous field's variables forward.
func (field jsonFieldOp) execute(buf *bytes.Buffer, data *columnTemplateData) error {
	clear(data.ColumnVariables)
	if len(field.columnVariables) > 0 && data.ColumnVariables == nil {
		data.ColumnVariables = make(map[string]string)
	}
	if err := renderColumnVariables(field.columnVariables, data); err != nil {
		return err
	}
	return field.template.Execute(buf, data)
}

// applyJsonTransform applies JSON field transformations to a JSON string.
// For each field, it reads the value at the gjson path, executes the template
// with .FieldValue set to the current value, and writes the result back.
func applyJsonTransform(jsonStr string, fields []jsonFieldOp, data *columnTemplateData) (string, error) {
	if !gjson.Valid(jsonStr) {
		return "", fmt.Errorf("invalid JSON: %.50s", jsonStr)
	}

	result := jsonStr

	for _, field := range fields {
		if strings.Contains(field.path, ".#.") || strings.HasPrefix(field.path, "#.") {
			var err error
			result, err = applyArrayWildcard(result, field, data)
			if err != nil {
				return "", err
			}
		} else {
			val := gjson.Get(result, field.path)
			if !val.Exists() {
				return "", fmt.Errorf("JSON path '%s' not found", field.path)
			}

			originalFieldValue := data.FieldValue
			data.FieldValue = val.String()

			var buf bytes.Buffer
			if err := field.execute(&buf, data); err != nil {
				data.FieldValue = originalFieldValue
				return "", fmt.Errorf("template execution failed for path '%s': %w", field.path, err)
			}

			var err error
			result, err = sjson.Set(result, field.path, buf.String())
			if err != nil {
				data.FieldValue = originalFieldValue
				return "", fmt.Errorf("sjson.Set failed for path '%s': %w", field.path, err)
			}

			data.FieldValue = originalFieldValue
		}
	}

	return result, nil
}

// applyArrayWildcard handles paths containing # (array wildcard).
func applyArrayWildcard(jsonStr string, field jsonFieldOp, data *columnTemplateData) (string, error) {
	parts := strings.SplitN(field.path, ".#.", 2)
	if len(parts) != 2 {
		parts = strings.SplitN(field.path, "#.", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid array wildcard path: %s", field.path)
		}
		parts[0] = ""
	}

	arrayPath := parts[0]
	fieldSuffix := parts[1]

	var arrayResult gjson.Result
	if arrayPath == "" {
		arrayResult = gjson.Parse(jsonStr)
	} else {
		arrayResult = gjson.Get(jsonStr, arrayPath)
	}

	if !arrayResult.Exists() || !arrayResult.IsArray() {
		return "", fmt.Errorf("JSON path '%s' is not an array", arrayPath)
	}

	result := jsonStr
	elements := arrayResult.Array()

	if len(elements) == 0 {
		return "", fmt.Errorf("JSON array at path '%s' is empty", arrayPath)
	}

	originalFieldValue := data.FieldValue

	for i, elem := range elements {
		val := elem.Get(fieldSuffix)
		if !val.Exists() {
			data.FieldValue = originalFieldValue
			return "", fmt.Errorf("JSON path '%s' not found in array element %d", fieldSuffix, i)
		}

		data.FieldValue = val.String()

		var buf bytes.Buffer
		if err := field.execute(&buf, data); err != nil {
			data.FieldValue = originalFieldValue
			return "", fmt.Errorf("template execution failed for path '%s[%d].%s': %w", arrayPath, i, fieldSuffix, err)
		}

		var indexedPath string
		if arrayPath == "" {
			indexedPath = fmt.Sprintf("%d.%s", i, fieldSuffix)
		} else {
			indexedPath = fmt.Sprintf("%s.%d.%s", arrayPath, i, fieldSuffix)
		}

		var err error
		result, err = sjson.Set(result, indexedPath, buf.String())
		if err != nil {
			data.FieldValue = originalFieldValue
			return "", fmt.Errorf("sjson.Set failed for indexed path '%s': %w", indexedPath, err)
		}
	}

	data.FieldValue = originalFieldValue
	return result, nil
}
