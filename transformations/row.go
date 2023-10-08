package transformations

type TemplateTransformationOptions struct {
	Template string `json:"template,omitempty"`
}

type ValueTransformationOptions struct {
	Value interface{} `json:"value"`
}

type ColumnTemplateVariables struct {
	Row    map[string]string
	Table  map[string]string
	Global map[string]string
}
