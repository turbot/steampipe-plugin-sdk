package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// create a connectionData for the aggregator connection but DO NOT set the schem yet
// we build all aggregator schemas in a separate phase at the end after all connection updates
// have been processed
func (p *Plugin) setAggregatorConnectionData(aggregatorConfig *proto.ConnectionConfig) {
	c := aggregatorConfig.Connection
	connectionData := NewConnectionData(&Connection{Name: c}, p, aggregatorConfig)
	p.setConnectionData(connectionData, c)
}
