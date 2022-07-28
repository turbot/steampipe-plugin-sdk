package proto

import "github.com/turbot/go-kit/helpers"

func (c *ConnectionConfig) Equals(other *ConnectionConfig) bool {
	res := c.Connection == other.Connection &&
		c.Plugin == other.Plugin &&
		c.PluginShortName == other.PluginShortName &&
		c.Config == other.Config &&
		c.childConnectionsEqual(other)
	return res
}

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
