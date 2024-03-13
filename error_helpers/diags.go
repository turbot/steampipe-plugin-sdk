package error_helpers

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/sperr"
	"strings"

	"github.com/hashicorp/hcl/v2"
)

// TODO this is duplicated from pipe-fittings - only exists here until AWS plugin is updated to latest sdk so we can reference pipe-fittings
func HclDiagsToError(prefix string, diags hcl.Diagnostics) error {
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
		return sperr.New(fmt.Sprintf("%s: %s", prefix, res))
	}

	return diags.Errs()[0]
}

func diagsToString(diags hcl.Diagnostics, severity hcl.DiagnosticSeverity) []string { // convert the first diag into an error
	// store list of messages (without the range) and use for de-duping (we may get the same message for multiple ranges)
	var msgMap = make(map[string]struct{})
	var strs []string
	for _, diag := range diags {
		if diag.Severity == severity {
			str := diag.Summary
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

// HclDiagsToWarnings converts warning diags into a list of warning strings
func HclDiagsToWarnings(diags hcl.Diagnostics) []string {
	return diagsToString(diags, hcl.DiagWarning)
}
