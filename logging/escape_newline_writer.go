package logging

import (
	"bytes"
	"io"
)

// EscapeNewlineWriter represents a io.Writer that can escape newlines
type EscapeNewlineWriter struct {
	wr io.Writer
}

// NewEscapeNewlineWriter returns an object that conform to the io.Writer interface
// and can be used to escape newline ('\n') with the string literal ("\n")
// The param is the underlying io.Writer that this object will write to
func NewEscapeNewlineWriter(writer io.Writer) EscapeNewlineWriter {
	return EscapeNewlineWriter{
		wr: writer,
	}
}

func (m EscapeNewlineWriter) Write(in []byte) (n int, err error) {
	escaped := make([]byte, len(in))
	copy(escaped, in)

	// the number of newlines to replace
	numReplacements := bytes.Count(escaped, newLine)

	// we shouldn't replace a trailing new line
	if bytes.HasSuffix(in, newLine) {
		numReplacements--
	}

	// do the replacement
	escaped = bytes.Replace(escaped, newLine, escapedNewLine, numReplacements)

	// write out the escaped bytes
	if _, err := m.wr.Write(escaped); err != nil {
		return 0, err
	}

	// send back the length of the data passed to us
	// NOT the number of bytes we have written
	// upstream code may check the count returned by this function equals the bytes sent
	return len(in), nil
}
