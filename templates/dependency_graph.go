package templates

import (
	"fmt"
	"github.com/duffpl/go-mdp/v2/config"
	"golang.org/x/exp/slices"
	"regexp"
	"text/template/parse"
)

func CompileTemplates(templates map[string]config.Template, namePrefix string) (map[string]*Template, error) {
	compiledTemplates := make(map[string]*Template)
	for name, _template := range templates {
		prefixedName := "." + namePrefix + "." + name
		compiledTemplate, err := GetCompiledTemplate(string(_template), prefixedName)
		if err != nil {
			return nil, fmt.Errorf("cannot compile template %s: %w", prefixedName, err)
		}
		compiledTemplates[prefixedName] = &Template{
			CompiledTemplate: compiledTemplate,
			Dependencies:     nil,
			Name:             prefixedName,
		}
	}
	// build dependency graph
	for name, compiledTemplate := range compiledTemplates {
		dependencies := ExtractVariables(compiledTemplate.CompiledTemplate.Tree.Root, namePrefix)
		for _, dependency := range dependencies {
			dependencyTemplate, ok := compiledTemplates[dependency]
			if !ok {
				return nil, fmt.Errorf("template %s depends on %s, but %s is not defined", name, dependency, dependency)
			}
			compiledTemplate.Dependencies = append(compiledTemplate.Dependencies, dependencyTemplate)
		}
	}
	return compiledTemplates, nil
}

func CompileAllTemplates(templates map[string]config.Template) (map[string]*Template, error) {
	compiledTemplates := make(map[string]*Template)
	allTemplateNames := []string{}
	for name, _template := range templates {
		allTemplateNames = append(allTemplateNames, name)
		compiledTemplate, err := GetCompiledTemplate(string(_template), name)
		if err != nil {
			return nil, fmt.Errorf("cannot compile template %s: %w", name, err)
		}
		compiledTemplates[name] = &Template{
			CompiledTemplate: compiledTemplate,
			Dependencies:     nil,
			Name:             name,
		}
	}
	// build dependency graph
	for name, compiledTemplate := range compiledTemplates {
		dependencies := ExtractDependencies(compiledTemplate.CompiledTemplate.Tree.Root, allTemplateNames)
		for _, dependency := range dependencies {
			dependencyTemplate, ok := compiledTemplates[dependency]
			if !ok {
				return nil, fmt.Errorf("template %s depends on %s, but %s is not defined", name, dependency, dependency)
			}
			compiledTemplate.Dependencies = append(compiledTemplate.Dependencies, dependencyTemplate)
		}
	}
	return compiledTemplates, nil
}

func ExtractDependencies(rootNode *parse.ListNode, templateNames []string) []string {
	var result []string
	var variableRegexes []*regexp.Regexp
	for _, templateName := range templateNames {
		variableRegexes = append(variableRegexes, regexp.MustCompile(templateName))
	}
	matchAnyRegex := func(input string) bool {
		for _, regex := range variableRegexes {
			if regex.MatchString(input) {
				return true
			}
		}
		return false
	}
	for _, node := range rootNode.Nodes {
		switch node.Type() {
		case parse.NodeAction:
			actionNode := node.(*parse.ActionNode)
			if actionNode.Pipe == nil {
				continue
			}
			for _, pipeCmd := range actionNode.Pipe.Cmds {
				if pipeCmd.NodeType != parse.NodeCommand {
					continue
				}
				for _, arg := range pipeCmd.Args {
					if arg.Type() != parse.NodeField {
						continue
					}
					variableName := arg.String()
					if !matchAnyRegex(variableName) {
						continue
					}
					// check if the dependency is already in the list
					if !slices.Contains(result, variableName) {
						result = append(result, variableName)
					}
				}
			}
		}
	}
	return result
}
func ExtractVariables(rootNode *parse.ListNode, prefix string) (result []string) {
	variableNameRegexp := regexp.MustCompile(`\.` + prefix + `\.`)
	for _, node := range rootNode.Nodes {
		switch node.Type() {
		case parse.NodeAction:
			actionNode := node.(*parse.ActionNode)
			if actionNode.Pipe == nil {
				continue
			}
			for _, pipeCmd := range actionNode.Pipe.Cmds {
				if pipeCmd.NodeType != parse.NodeCommand {
					continue
				}
				for _, arg := range pipeCmd.Args {
					if arg.Type() != parse.NodeField {
						continue
					}
					variableName := arg.String()
					if !variableNameRegexp.MatchString(variableName) {
						continue
					}
					// check if the variable is already in the list
					if !slices.Contains(result, variableName) {
						result = append(result, variableName)
					}
				}
			}
		}
	}
	return
}
func GetDependencyStackForMultipleTemplates(templateNames []string, templates map[string]*Template) ([]*Template, error) {
	var stack []*Template
	visited := make(map[string]bool)

	for _, templateName := range templateNames {
		// Check if the template exists
		template, exists := templates[templateName]
		if !exists {
			return nil, fmt.Errorf("template %s does not exist", templateName)
		}

		// Start depth-first search
		err := dfs(template, templates, &stack, visited)
		if err != nil {
			return nil, err
		}
	}

	return stack, nil
}

func dfs(template *Template, templates map[string]*Template, stack *[]*Template, visited map[string]bool) error {
	if visited[template.Name] {
		return nil
	}
	visited[template.Name] = true
	for _, dep := range template.Dependencies {
		depTemplate, exists := templates[dep.Name]
		if !exists {
			return fmt.Errorf("template %s depends on %s, but %s does not exist", template.Name, dep.Name, dep.Name)
		}
		err := dfs(depTemplate, templates, stack, visited)
		if err != nil {
			return err
		}
	}
	*stack = append(*stack, template)

	return nil
}
func GetOrderedTemplates(templates map[string]*Template) ([]*Template, error) {
	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)
	stack := []*Template{}

	var dfs func(node string) error
	dfs = func(name string) error {
		if _, ok := templates[name]; !ok {
			return fmt.Errorf("template %s not found", name)
		}
		if !visited[name] {
			visited[name] = true
			recursionStack[name] = true
			for _, dep := range templates[name].Dependencies {
				if recursionStack[dep.Name] {
					return fmt.Errorf("cycle detected: %s is part of a cycle", dep.Name)
				}
				if !visited[dep.Name] {
					if err := dfs(dep.Name); err != nil {
						return err
					}
				}
			}
			stack = append(stack, templates[name])
		}
		delete(recursionStack, name)
		return nil
	}

	for name := range templates {
		if !visited[name] {
			if err := dfs(name); err != nil {
				return nil, err
			}
		}
	}

	return stack, nil
}
