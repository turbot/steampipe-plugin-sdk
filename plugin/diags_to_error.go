package plugin

import (
	"errors"
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
	"strings"
)

// DiagsToError converts hcl diags into an error
func DiagsToError(prefix string, diags hcl.Diagnostics) error {
	// convert the first diag into an error
	if !diags.HasErrors() {
		return nil
	}
	errorStrings := []string{fmt.Sprintf("%s", prefix)}
	// store list of messages (without the range) and use for deduping (we may get the same message for multiple ranges)
	errorMessages := []string{}
	for _, diag := range diags {
		if diag.Severity == hcl.DiagError {
			errorString := fmt.Sprintf("%s", diag.Summary)
			if diag.Detail != "" {
				errorString += fmt.Sprintf(": %s", diag.Detail)
			}

			if !helpers.StringSliceContains(errorMessages, errorString) {
				errorMessages = append(errorMessages, errorString)
				// now add in the subject and add to the output array
				if diag.Subject != nil && len(diag.Subject.Filename) > 0 {
					errorString += fmt.Sprintf("\n(%s)", diag.Subject.String())
				}
				errorStrings = append(errorStrings, errorString)

			}
		}
	}
	if len(errorStrings) > 0 {
		errorString := strings.Join(errorStrings, "\n")
		if len(errorStrings) > 1 {
			errorString += "\n"
		}
		return errors.New(errorString)
	}
	return diags.Errs()[0]
}
