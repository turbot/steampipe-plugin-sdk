package logging

import (
	"bytes"
	"io"
)

type UnescapeNewlineWriter struct {
	wr io.Writer
}

// NewUnescapeNewlineWriter
func NewUnescapeNewlineWriter(writer io.Writer) UnescapeNewlineWriter {
	return UnescapeNewlineWriter{
		wr: writer,
	}
}

func (m UnescapeNewlineWriter) Write(in []byte) (n int, err error) {
	escaped := bytes.ReplaceAll(in, escapedNewLine, newLine)
	if _, err := m.wr.Write(escaped); err != nil {
		return 0, err
	}

	// send back the length of the data passed to us
	// NOT the number of bytes we have written
	// upstream code may check the count returned by this function equals the bytes sent
	return len(in), nil
}
