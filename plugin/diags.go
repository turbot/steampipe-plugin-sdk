package plugin

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
)

// DiagsToError converts tfdiags diags into an error
func DiagsToError(prefix string, diags hcl.Diagnostics) error {
	if !diags.HasErrors() {
		return nil
	}
	errStrings := diagsToString(diags, hcl.DiagError)

	var res string
	if len(errStrings) > 0 {
		res = strings.Join(errStrings, "\n")
		if len(errStrings) > 1 {
			res += "\n"
		}
		return fmt.Errorf("%s\n%s", prefix, res)
	}

	return diags.Errs()[0]
}

// DiagsToWarning converts warning diags into a list of warning strings
func DiagsToWarnings(diags hcl.Diagnostics) []string {
	return diagsToString(diags, hcl.DiagWarning)
}

func diagsToString(diags hcl.Diagnostics, severity hcl.DiagnosticSeverity) []string { // convert the first diag into an error
	// store list of messages (without the range) and use for deduping (we may get the same message for multiple ranges)
	var msgMap = make(map[string]struct{})
	var strs []string
	for _, diag := range diags {
		if diag.Severity == severity {
			str := fmt.Sprintf("%s", diag.Summary)
			if diag.Detail != "" {
				str += fmt.Sprintf(": %s", diag.Detail)
			}

			if _, ok := msgMap[str]; !ok {
				msgMap[str] = struct{}{}
				// now add in the subject and add to the output array
				if diag.Subject != nil && len(diag.Subject.Filename) > 0 {
					str += fmt.Sprintf("\n(%s)", diag.Subject.String())
				}

				strs = append(strs, str)
			}
		}
	}

	return strs
}
