package config

import (
	"encoding/json"
)

type Config struct {
	TableConfigs    []TableConfig       `json:"tables"`
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
	Columns         []ColumnConfig      `json:"columns"`
	RowVariables    map[string]Template `json:"rowVariables,omitempty"`
	ColumnVariables map[string]Template `json:"columnVariables,omitempty"`
	TableVariables  map[string]Template `json:"tableVariables,omitempty"`
}

type ColumnConfig struct {
	ColumnName string
	Templates  []Template
}

type ColumnTransformation interface {
	GetType() string
}

func (c *ColumnConfig) UnmarshalJSON(bytes []byte) error {

	type tmpTransformation struct {
		Template string `json:"template"`
	}
	type tmpConfig struct {
		Name            string              `json:"name"`
		Transformations []tmpTransformation `json:"transformations"`
	}
	parsedTmpConfig := tmpConfig{}
	err := json.Unmarshal(bytes, &parsedTmpConfig)
	if err != nil {
		return err
	}
	c.ColumnName = parsedTmpConfig.Name
	c.Templates = make([]Template, len(parsedTmpConfig.Transformations))
	for i := range parsedTmpConfig.Transformations {
		c.Templates[i] = Template(parsedTmpConfig.Transformations[i].Template)
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
