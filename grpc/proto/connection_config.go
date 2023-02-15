package proto

import (
	"github.com/turbot/go-kit/helpers"
)

func (x *ConnectionConfig) Equals(other *ConnectionConfig) bool {
	res := x.Connection == other.Connection &&
		x.Plugin == other.Plugin &&
		x.PluginShortName == other.PluginShortName &&
		x.Config == other.Config &&
		//x.TableAggregationSpecsEqual(other) &&
		x.childConnectionsEqual(other)

	return res
}

//func (x *ConnectionConfig) TableAggregationSpecsEqual(other *ConnectionConfig) bool {
//	if len(x.TableAggregationSpecs) != len(other.TableAggregationSpecs) {
//		return false
//	}
//	for i, s := range x.TableAggregationSpecs {
//		if !other.TableAggregationSpecs[i].Equals(s) {
//			return false
//		}
//	}
//	return true
//}

func (x *ConnectionConfig) childConnectionsEqual(other *ConnectionConfig) bool {
	if len(x.ChildConnections) != len(other.ChildConnections) {
		return false
	}

	// ignore ordering
	for _, c := range x.ChildConnections {
		if !helpers.StringSliceContains(other.ChildConnections, c) {
			return false
		}
	}
	return true
}

//func (x *ConnectionConfig) GetAggregationSpecForTable(tableName string) *TableAggregationSpec {
//	for _, spec := range x.TableAggregationSpecs {
//		if wild.Match(spec.Match, tableName, false) {
//			return spec
//		}
//	}
//	return nil
//}
