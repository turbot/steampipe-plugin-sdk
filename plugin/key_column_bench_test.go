package plugin

import "testing"

func BenchmarkKeyColumnString(b *testing.B) {
	k := &KeyColumn{Name: "id", Operators: []string{"=", "!="}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = k.String()
	}
}
