package plugin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// hasDeprecatedParagraph reports whether a doc comment has a paragraph starting "Deprecated: ".
// This is the form Go tools (staticcheck SA1019, gopls, pkgsite) read as a deprecation.
func hasDeprecatedParagraph(doc *ast.CommentGroup) bool {
	if doc == nil {
		return false
	}
	for _, para := range strings.Split(doc.Text(), "\n\n") {
		if strings.HasPrefix(para, "Deprecated: ") {
			return true
		}
	}
	return false
}

func TestHasDeprecatedParagraph(t *testing.T) {
	cases := map[string]struct {
		doc  string
		want bool
	}{
		"deprecated paragraph":       {"// Deprecated: use X", true},
		"after another paragraph":    {"// Does a thing.\n//\n// Deprecated: use X", true},
		"lower case, no colon":       {"// deprecated - use X", false},
		"capital, no colon":          {"// Deprecated", false},
		"in the middle of a section": {"// Does a thing.\n// Deprecated: use X", false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "x.go", "package p\n"+c.doc+"\ntype T int\n", parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			if got := hasDeprecatedParagraph(f.Decls[0].(*ast.GenDecl).Doc); got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

// the four concurrency settings the SDK does not read must be marked as deprecated, and point at the rate limiter
func TestConcurrencySettingsAreDeprecated(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	pkg := pkgs["plugin"]
	if pkg == nil {
		t.Fatal("package plugin not found")
	}

	type item struct{ typeName, field string }
	wantFields := []item{
		{"HydrateConfig", "MaxConcurrency"},
		{"GetConfig", "MaxConcurrency"},
		{"Plugin", "DefaultConcurrency"},
	}
	foundFields := map[item]bool{}
	foundType := false

	check := func(what string, doc *ast.CommentGroup) {
		t.Helper()
		if !hasDeprecatedParagraph(doc) {
			t.Errorf("%s has no 'Deprecated: ' doc paragraph", what)
			return
		}
		if !strings.Contains(doc.Text(), "rate_limiter.Definition") {
			t.Errorf("%s deprecation does not point at MaxConcurrency on a rate_limiter.Definition", what)
		}
	}

	for _, file := range pkg.Files {
		if strings.HasSuffix(file.Name.Name, "_test") {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			gd, ok := n.(*ast.GenDecl)
			if !ok {
				return true
			}
			for _, spec := range gd.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				if ts.Name.Name == "DefaultConcurrencyConfig" {
					foundType = true
					check("type DefaultConcurrencyConfig", gd.Doc)
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, w := range wantFields {
					if w.typeName != ts.Name.Name {
						continue
					}
					for _, f := range st.Fields.List {
						for _, name := range f.Names {
							if name.Name == w.field {
								foundFields[w] = true
								check(w.typeName+"."+w.field, f.Doc)
							}
						}
					}
				}
			}
			return true
		})
	}
	if !foundType {
		t.Error("type DefaultConcurrencyConfig not found")
	}
	for _, w := range wantFields {
		if !foundFields[w] {
			t.Errorf("field %s.%s not found", w.typeName, w.field)
		}
	}
}

// the docs of HydrateConfig, GetConfig and Plugin must not tell an author to set a setting that does nothing
func TestDocsDoNotAdviseConcurrencySettings(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, file := range pkgs["plugin"].Files {
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Doc == nil {
				continue
			}
			for _, spec := range gd.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				switch ts.Name.Name {
				case "HydrateConfig", "GetConfig", "Plugin":
					seen++
					doc := gd.Doc.Text()
					for _, bad := range []string{"MaxConcurrency: 50", "Plugin.DefaultConcurrency", "HydrateConfig.MaxConcurrency", "GetConfig.MaxConcurrency"} {
						if strings.Contains(doc, bad) {
							t.Errorf("doc of %s still mentions %q", ts.Name.Name, bad)
						}
					}
					if !strings.Contains(doc, "rate limiter") && !strings.Contains(doc, "RateLimiters") {
						t.Errorf("doc of %s does not say to limit concurrency with a rate limiter", ts.Name.Name)
					}
				}
			}
		}
	}
	if seen != 3 {
		t.Errorf("expected the docs of 3 types, found %d", seen)
	}
}
