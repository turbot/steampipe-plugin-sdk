package logging

import (
	"bytes"
	"testing"
)

// the expected text in these tests is what tablewriter v0.0.5 printed for the same rows

func TestDisplayTableWrapsLongOperations(t *testing.T) {
	var b bytes.Buffer
	displayTable(&b, [][]string{
		{"first operation", "Jan  1 00:00:00.000", "< 1s", "2s"},
		{"second", "Jan  1 00:00:02.500", "2.5s", "4.5s"},
		{"a very long operation name that goes on and on and on and on", "Jan  1 00:00:03.000", "500ms", "5s"},
	})
	want := `+----------------------------------------------------+---------------------+---------+------------+
|                     OPERATION                      |        TIME         | ELAPSED | CUMULATIVE |
+----------------------------------------------------+---------------------+---------+------------+
| first operation                                    | Jan  1 00:00:00.000 | < 1s    | 2s         |
| second                                             | Jan  1 00:00:02.500 | 2.5s    | 4.5s       |
| a very long operation name that goes on and on and | Jan  1 00:00:03.000 | 500ms   | 5s         |
| on and on                                          |                     |         |            |
+----------------------------------------------------+---------------------+---------+------------+
`
	if got := b.String(); got != want {
		t.Errorf("table output changed.\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestDisplayTableKeepsLongWordsNewlinesAndNumbers(t *testing.T) {
	var b bytes.Buffer
	displayTable(&b, [][]string{
		{"refresh_connections", "Nov 23 18:01:02.345", "< 100ms", "< 100ms"},
		{"build schema\nfor 512 tables", "Nov 23 18:01:02.500", "155ms", "155ms"},
		{"getPluginDefinitionsFromAnUnusuallyLongOperationNameWithNoSpacesAtAll", "Nov 23 18:01:03.000", "500ms", "655ms"},
		{"42", "Nov 23 18:01:04.000", "1s", "1.655s"},
	})
	want := `+-----------------------------------------------------------------------+---------------------+---------+------------+
|                               OPERATION                               |        TIME         | ELAPSED | CUMULATIVE |
+-----------------------------------------------------------------------+---------------------+---------+------------+
| refresh_connections                                                   | Nov 23 18:01:02.345 | < 100ms | < 100ms    |
| build schema                                                          | Nov 23 18:01:02.500 | 155ms   | 155ms      |
| for 512 tables                                                        |                     |         |            |
| getPluginDefinitionsFromAnUnusuallyLongOperationNameWithNoSpacesAtAll | Nov 23 18:01:03.000 | 500ms   | 655ms      |
|                                                                    42 | Nov 23 18:01:04.000 | 1s      | 1.655s     |
+-----------------------------------------------------------------------+---------------------+---------+------------+
`
	if got := b.String(); got != want {
		t.Errorf("table output changed.\nwant:\n%s\ngot:\n%s", want, got)
	}
}
