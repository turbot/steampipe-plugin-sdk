package proto

import "slices"

func (x *ConnectionConfig) Equals(other *ConnectionConfig) bool {
	res := x.Connection == other.Connection &&
		x.Plugin == other.Plugin &&
		x.PluginShortName == other.PluginShortName &&
		x.Config == other.Config &&
		x.childConnectionsEqual(other)

	return res
}

func (x *ConnectionConfig) childConnectionsEqual(other *ConnectionConfig) bool {
	if len(x.ChildConnections) != len(other.ChildConnections) {
		return false
	}

	// ignore ordering
	for _, c := range x.ChildConnections {
		if !slices.Contains(other.ChildConnections, c) {
			return false
		}
	}
	return true
}

func (x *ConnectionConfig) IsAggregator() bool {
	// keep len(d.config.ChildConnections) > 0 for legacy steampipe verisons which do not support type
	return len(x.ChildConnections) > 0 || x.Type == "aggregator"
}
