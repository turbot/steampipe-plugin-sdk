package logging

import (
	"bytes"
	"strings"
	"testing"
)

// the table is logged line by line, byte by byte, so it must be pure ASCII
func TestDisplayTableIsASCII(t *testing.T) {
	var b bytes.Buffer
	displayTable(&b, [][]string{
		{"first operation", "Jan  1 00:00:00.000", "< 1s", "2s"},
		{"a very long operation name that goes on and on and on and on and on and on and on", "x", "y", "z"},
	})
	out := b.String()
	for _, r := range out {
		if r > 127 {
			t.Fatalf("non-ASCII character %q in table:\n%s", r, out)
		}
	}
	for _, want := range []string{"OPERATION", "CUMULATIVE", "first operation", "a very long operation name that goes on and on and on and on and on and on and on", "+-", "| "} {
		if !strings.Contains(out, want) {
			t.Errorf("table does not contain %q:\n%s", want, out)
		}
	}
}
