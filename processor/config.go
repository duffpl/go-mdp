package processor

import (
	"encoding/json"
	"github.com/duffpl/go-mdp/transformations"
)

type Config struct {
	TableConfigs []TableConfig `json:"tables"`
	PostSQL      string              `json:"postSql,omitempty"`
}

type TableConfig struct {
	TableName string         `json:"name"`
	Columns   []ColumnConfig `json:"columns"`
}

type ColumnConfig struct {
	ColumnName      string                       `json:"name"`
	Transformations []ColumnTransformationConfig `json:"transformations"`
}

func (t TableConfig) GetColumnConfigByName(name string) ColumnConfig {
	for i := range t.Columns {
		if t.Columns[i].ColumnName == name {
			return t.Columns[i]
		}
	}
	return ColumnConfig{}
}

type ColumnTransformationConfig struct {
	TransformationFunction transformations.ColumnTransformationFunction
}

func (c *ColumnTransformationConfig) UnmarshalJSON(i []byte) error {
	configData := &struct {
		Type    transformations.ColumnTransformationType `json:"type"`
		Options json.RawMessage                          `json:"options"`
	}{}
	err := json.Unmarshal(i, configData)
	if err != nil {
		return err
	}
	c.TransformationFunction, err = transformations.GetColumnTransformation(configData.Type, configData.Options)
	return err
}

func (c Config) GetTableConfigByName(name string) TableConfig {
	for i := range c.TableConfigs {
		currentConfig := c.TableConfigs[i]
		if currentConfig.TableName != name {
			continue
		}
		return currentConfig
	}
	return TableConfig{}
}