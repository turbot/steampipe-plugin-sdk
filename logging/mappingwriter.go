package logging

import (
	"bytes"
	"io"
)

type Mapper map[string]string

// Transposes
// [k => v] becomes [v => k]
func (m Mapper) Reverse() Mapper {
	new := Mapper{}
	for k, v := range m {
		new[v] = k
	}
	return new
}

var LogMapping Mapper = Mapper{
	"\n": "$$SPLF$$",
	"\r": "$$SPCR$$",
}

type MappingWriter struct {
	wr     io.Writer
	mapper Mapper
}

// creates and returns an io.Writer which writes to the underlying io.Writer after replacing string tokens according to the given map
func NewMappingWriter(writer io.Writer, mapper Mapper) MappingWriter {
	wr := MappingWriter{
		wr:     writer,
		mapper: mapper,
	}
	return wr
}

func (mwr MappingWriter) Write(p []byte) (n int, err error) {
	mapped := make([]byte, len(p))
	copy(mapped, p)
	for old, new := range mwr.mapper {
		replaceCount := bytes.Count(mapped, []byte(old))
		if replaceCount > 0 && old == "\n" {
			// if we are replaceing a newline, we can't replace the last one
			// since the reader on the other end reads by line
			replaceCount--
		}

		for bytes.Count(mapped, []byte(old)) > 1 {
			mapped = bytes.Replace(mapped, []byte(old), []byte(new), replaceCount)
		}
	}
	if _, err := mwr.wr.Write(mapped); err != nil {
		return 0, err
	}
	// send back the length of the sent in buffer
	return len(p), nil
}
