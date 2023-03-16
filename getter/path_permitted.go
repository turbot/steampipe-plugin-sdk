package getter

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// EnvPermittedFileRoots specified a comma separate list of permitted folder roots
const EnvPermittedFileRoots = "STEAMPIPE_SDK_PERMITTED_ROOT_PATHS"

func pathPermitted(sourcePath string) bool {
	permittedFileRootEnv, ok := os.LookupEnv(EnvPermittedFileRoots)
	if !ok || permittedFileRootEnv == "" {
		return true
	}
	roots := strings.Split(permittedFileRootEnv, ",")
	for _, root := range roots {
		// TODO: is it ok to only support abs roots?
		if !path.IsAbs(root) {
			log.Printf("[WARN] permitted file root %s is not an absolute path - ignoring", roots)
			continue
		}
		isSubElement, _ := subElem(root, sourcePath)
		if isSubElement {
			return true
		}
	}
	return false
}

// from https://stackoverflow.com/questions/28024731/check-if-given-path-is-a-subdirectory-of-another-in-golang
func subElem(parent, sub string) (bool, error) {
	up := ".." + string(os.PathSeparator)

	// path-comparisons using filepath.Abs don't work reliably according to docs (no unique representation).
	rel, err := filepath.Rel(parent, sub)
	if err != nil {
		return false, err
	}
	if !strings.HasPrefix(rel, up) && rel != ".." {
		return true, nil
	}
	return false, nil
}
