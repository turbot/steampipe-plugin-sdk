package logging

import (
	"bytes"
	"io"
)

type newLineEscapeWriter struct {
	wr io.Writer
}

func NewLineEscapeWriter(writer io.Writer) newLineEscapeWriter {
	return newLineEscapeWriter{
		wr: writer,
	}
}

func (m newLineEscapeWriter) Write(in []byte) (n int, err error) {
	hasSuffixNewLine := bytes.HasSuffix(in, []byte("\n"))
	escaped := []byte{}
	if hasSuffixNewLine {
		escaped = bytes.TrimSuffix(in, []byte("\n"))
	}
	escaped = bytes.ReplaceAll(escaped, []byte("\n"), []byte("\\n"))
	if hasSuffixNewLine {
		escaped = append(escaped, '\n')
	}
	if _, err := m.wr.Write(escaped); err != nil {
		return 0, err
	}
	// send back the length of the sent in buffer
	return len(in), nil
}

type newLineUnescapeWriter struct {
	wr io.Writer
}

func NewLineUnescapeWriter(writer io.Writer) newLineUnescapeWriter {
	return newLineUnescapeWriter{
		wr: writer,
	}
}
func (m newLineUnescapeWriter) Write(in []byte) (n int, err error) {
	escaped := bytes.ReplaceAll(in, []byte("\\n"), []byte("\n"))
	if _, err := m.wr.Write(escaped); err != nil {
		return 0, err
	}
	// send back the length of the sent in buffer
	return len(in), nil
}
