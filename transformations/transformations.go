package transformations

import (
	"crypto/md5"
	"fmt"
	"github.com/Masterminds/sprig"
	"html/template"
)

var templateFuncs = template.FuncMap{}

func init() {
	templateFuncs = sprig.FuncMap()
	templateFuncs["md5"] = func(input string) string {
		hasher := md5.New()
		hasher.Write([]byte(input))
		return fmt.Sprintf("%x", hasher.Sum(nil))
	}
}

func stringMd5(input string) string {
	h := md5.New()
	h.Write([]byte(input))
	return fmt.Sprintf("%x", h.Sum(nil))
}

