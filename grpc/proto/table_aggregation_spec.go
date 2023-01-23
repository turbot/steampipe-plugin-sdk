package proto

import "github.com/gertd/wild"

func (x *TableAggregationSpec) MatchesConnection(connectionName string) bool {
	for _, connectionMatch := range x.Connections {
		if wild.Match(connectionMatch, connectionName, false) {
			return true
		}
	}
	return false
}
