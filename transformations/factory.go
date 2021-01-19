package transformations

import (
	"encoding/json"
	"fmt"
)

var columnTransformationInitializers = make(map[ColumnTransformationType]ColumnTransformationInitializer)

func RegisterColumnTransformation(t ColumnTransformationType, c ColumnTransformationInitializer) {
	columnTransformationInitializers[t] = c
}

func GetColumnTransformation(t ColumnTransformationType, opts json.RawMessage, additionalOptions any) (ColumnTransformationFunction, error) {
	transformFunctionInitializer, transformFound := columnTransformationInitializers[t]
	if !transformFound {
		return nil, fmt.Errorf("transformation initializer for '%s' doesn't exist", t)
	}
	transformFunction, err := transformFunctionInitializer(opts, additionalOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot initialize '%s': %w", t, err)
	}
	return transformFunction, nil
}
