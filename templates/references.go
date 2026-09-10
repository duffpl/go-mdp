package templates

import (
	"fmt"
	"strings"
	"text/template"
	"text/template/parse"
)

// Bindings retain possible origins across assignments in conditional branches.
// Declarations are scoped; assignments update the shared outer binding.
type referenceBinding struct{ paths []string }
type referenceScope struct {
	dot  []string
	vars map[string]*referenceBinding
}

func rootScope(dot string) referenceScope {
	var paths []string
	if dot != "" {
		paths = []string{dot}
	}
	return referenceScope{paths, map[string]*referenceBinding{"$": {paths}}}
}
func (s referenceScope) child() referenceScope {
	vars := make(map[string]*referenceBinding, len(s.vars))
	for k, v := range s.vars {
		vars[k] = v
	}
	return referenceScope{s.dot, vars}
}

type referenceWalker struct {
	lookup *template.Template
	refs   []string
	seen   map[string]bool
	active map[string]bool
	err    error
}

func newReferenceWalker(t *template.Template) *referenceWalker {
	return &referenceWalker{lookup: t, seen: make(map[string]bool), active: make(map[string]bool)}
}
func templateReferences(t *template.Template) ([]string, error) {
	w := newReferenceWalker(t)
	w.active[t.Name()] = true
	w.list(t.Tree.Root, rootScope("."))
	return w.refs, w.err
}
func (w *referenceWalker) record(paths []string, wholeMap bool) {
	for _, path := range paths {
		if path == "" || (path == "." && !wholeMap) {
			continue
		}
		if !wholeMap && strings.Count(path, ".") < 2 {
			continue
		}
		if !w.seen[path] {
			w.refs = append(w.refs, path)
			w.seen[path] = true
		}
	}
}
func extendPaths(paths []string, fields []string) []string {
	if len(fields) == 0 {
		return paths
	}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		out = append(out, strings.TrimSuffix(path, ".")+"."+strings.Join(fields, "."))
	}
	return out
}
func (w *referenceWalker) expr(node parse.Node, s referenceScope) []string {
	switch n := node.(type) {
	case *parse.DotNode:
		return s.dot
	case *parse.FieldNode:
		return extendPaths(s.dot, n.Ident)
	case *parse.VariableNode:
		if b := s.vars[n.Ident[0]]; b != nil {
			return extendPaths(b.paths, n.Ident[1:])
		}
	case *parse.ChainNode:
		return extendPaths(w.expr(n.Node, s), n.Field)
	case *parse.PipeNode:
		return w.pipe(n, s)
	}
	return nil
}
func (w *referenceWalker) pipe(p *parse.PipeNode, s referenceScope) []string {
	if p == nil {
		return nil
	}
	var previous []string
	for i, cmd := range p.Cmds {
		if len(cmd.Args) == 1 {
			if _, function := cmd.Args[0].(*parse.IdentifierNode); !function && i == 0 {
				previous = w.expr(cmd.Args[0], s)
				continue
			}
		}
		ident, function := cmd.Args[0].(*parse.IdentifierNode)
		if function && ident.Ident == "index" && len(cmd.Args) >= 3 && i == 0 {
			base := w.expr(cmd.Args[1], s)
			for _, key := range cmd.Args[2:] {
				if literal, ok := key.(*parse.StringNode); ok {
					base = extendPaths(base, []string{literal.Text})
				} else {
					// A computed key requires the whole map. Its key expression can also
					// depend on another configured variable.
					w.record(base, true)
					w.record(w.expr(key, s), true)
					base = nil
				}
			}
			previous = base
			continue
		}
		w.record(previous, true)
		var origins []string
		for _, arg := range cmd.Args {
			paths := w.expr(arg, s)
			w.record(paths, true)
			origins = append(origins, paths...)
		}
		if function && (ident.Ident == "and" || ident.Ident == "or" || ident.Ident == "default" || ident.Ident == "coalesce") {
			previous = append(previous, origins...)
		} else {
			previous = nil
		}
	}
	return previous
}
func (w *referenceWalker) declare(p *parse.PipeNode, paths []string, s referenceScope) {
	if p == nil {
		return
	}
	for _, decl := range p.Decl {
		name := decl.Ident[0]
		if p.IsAssign && s.vars[name] != nil {
			s.vars[name].paths = append(s.vars[name].paths, paths...)
		} else {
			s.vars[name] = &referenceBinding{paths: paths}
		}
	}
}
func (w *referenceWalker) list(list *parse.ListNode, s referenceScope) {
	if list == nil || w.err != nil {
		return
	}
	for _, node := range list.Nodes {
		switch n := node.(type) {
		case *parse.ActionNode:
			paths := w.pipe(n.Pipe, s)
			// Reassigned aliases can be read on a later loop iteration. Treat
			// assignment of a whole map conservatively as a dependency on it.
			w.record(paths, len(n.Pipe.Decl) == 0 || n.Pipe.IsAssign)
			w.declare(n.Pipe, paths, s)
		case *parse.IfNode:
			child := s.child()
			paths := w.pipe(n.Pipe, child)
			w.record(paths, true)
			w.declare(n.Pipe, paths, child)
			w.list(n.List, child.child())
			w.list(n.ElseList, child.child())
		case *parse.WithNode:
			child := s.child()
			paths := w.pipe(n.Pipe, child)
			w.record(paths, false)
			w.declare(n.Pipe, paths, child)
			body := child.child()
			body.dot = paths
			w.list(n.List, body)
			w.list(n.ElseList, child.child())
			// A namespace used only for its truthiness still needs its entries.
			for _, path := range paths {
				if strings.Count(path, ".") != 1 {
					continue
				}
				used := false
				// Already discovered references also make the map available.
				for ref := range w.seen {
					if strings.HasPrefix(ref, path+".") {
						used = true
					}
				}
				if !used {
					w.record([]string{path}, true)
				}
			}
		case *parse.RangeNode:
			child := s.child()
			paths := w.pipe(n.Pipe, child)
			w.record(paths, true)
			// Range values have runtime-dependent keys; the collection access above
			// accounts for any dependency on an entire configured-variable map.
			w.declare(n.Pipe, nil, child)
			body := child.child()
			body.dot = nil
			w.list(n.List, body)
			w.list(n.ElseList, child.child())
		case *parse.TemplateNode:
			paths := w.pipe(n.Pipe, s)
			w.record(paths, false)
			if w.lookup == nil {
				continue
			}
			target := w.lookup.Lookup(n.Name)
			if target == nil {
				w.err = fmt.Errorf("template %s is not defined", n.Name)
				return
			}
			if w.active[n.Name] {
				w.err = fmt.Errorf("recursive template invocation: %s", n.Name)
				return
			}
			w.active[n.Name] = true
			child := rootScope("")
			child.dot = paths
			child.vars["$"].paths = paths
			w.list(target.Tree.Root, child)
			delete(w.active, n.Name)
		}
	}
}
