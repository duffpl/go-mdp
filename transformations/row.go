package transformations

import "bytes"

func TransformRowTemplate(row MappedRow, options TransformationOptions) interface{} {
	transformationTemplate := options["template"].(string)
	output := new(bytes.Buffer)
	e := getCompiledTemplate(transformationTemplate).Execute(output, struct {
		Row MappedRow
	}{
		row,
	})
	if e != nil {
		panic(e)
	}
	return output.String()
}
