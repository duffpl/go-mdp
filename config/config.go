package config

import (
	"encoding/json"
	"fmt"
)

type Config struct {
	TableConfigs    []TableConfig       `json:"tables"`
	SkipTables      []string            `json:"skipTables,omitempty"`
	RowVariables    map[string]Template `json:"rowVariables,omitempty"`
	ColumnVariables map[string]Template `json:"columnVariables,omitempty"`
	GlobalVariables map[string]Template `json:"globalVariables,omitempty"`
	TableVariables  map[string]Template `json:"tableVariables,omitempty"`
	PostSQL         string              `json:"postSql,omitempty"`
	Settings        Settings            `json:"settings"`
}

type Settings struct {
	Locale string `json:"locale"`
}

type Template string

type TableConfig struct {
	TableName       string              `json:"name"`
	Skip            bool                `json:"skip,omitempty"`
	Columns         []ColumnConfig      `json:"columns"`
	RowVariables    map[string]Template `json:"rowVariables,omitempty"`
	ColumnVariables map[string]Template `json:"columnVariables,omitempty"`
	TableVariables  map[string]Template `json:"tableVariables,omitempty"`
}

type ColumnOperation struct {
	Type       string            // "template" or "json"
	Template   Template          // used when Type == "template"
	JsonFields []JsonFieldConfig // used when Type == "json"
}

type JsonFieldConfig struct {
	Path     string   `json:"path"`
	Template Template `json:"template"`
}

type ColumnConfig struct {
	ColumnName string
	Operations []ColumnOperation
	Templates  []Template
}

type ColumnTransformation interface {
	GetType() string
}

func (c *ColumnConfig) UnmarshalJSON(bytes []byte) error {
	type jsonFieldRaw struct {
		Path     string `json:"path"`
		Template string `json:"template"`
	}
	type optionsRaw struct {
		Template string         `json:"template"`
		Fields   []jsonFieldRaw `json:"fields"`
	}
	type transformationRaw struct {
		Type    string     `json:"type"`
		Options optionsRaw `json:"options"`
	}
	type columnRaw struct {
		Name            string              `json:"name"`
		Transformations []transformationRaw `json:"transformations"`
	}

	var raw columnRaw
	if err := json.Unmarshal(bytes, &raw); err != nil {
		return err
	}

	c.ColumnName = raw.Name
	c.Operations = make([]ColumnOperation, len(raw.Transformations))
	c.Templates = nil

	for i, t := range raw.Transformations {
		opType := t.Type
		if opType == "" {
			opType = "template"
		}

		switch opType {
		case "template":
			c.Operations[i] = ColumnOperation{
				Type:     "template",
				Template: Template(t.Options.Template),
			}
			c.Templates = append(c.Templates, Template(t.Options.Template))
		case "json":
			fields := make([]JsonFieldConfig, len(t.Options.Fields))
			for j, f := range t.Options.Fields {
				fields[j] = JsonFieldConfig{
					Path:     f.Path,
					Template: Template(f.Template),
				}
			}
			c.Operations[i] = ColumnOperation{
				Type:       "json",
				JsonFields: fields,
			}
		default:
			return fmt.Errorf("unknown transformation type: %s", opType)
		}
	}

	return nil
}

func (t TableConfig) GetAllColumnConfigsByName(name string) []ColumnConfig {
	var result []ColumnConfig
	for i := range t.Columns {
		if t.Columns[i].ColumnName == name {
			result = append(result, t.Columns[i])
		}
	}
	return result
}

type ColumnTransformationConfig struct {
	Type    string          `json:"type"`
	Options json.RawMessage `json:"options"`
}

func (c Config) GetAllTableConfigsByName(name string) []TableConfig {
	var result []TableConfig
	for i := range c.TableConfigs {
		currentConfig := c.TableConfigs[i]
		if currentConfig.TableName != name {
			continue
		}
		result = append(result, currentConfig)
	}
	return result
}
