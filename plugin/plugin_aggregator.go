package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
)

func (p *Plugin) setAggregatorConnectionData(failedConnections map[string]error, aggregatorConfig *proto.ConnectionConfig) {
	if p.SchemaMode == SchemaModeDynamic {
		logMessages, err := p.setDynamicAggregatorConnectionData(aggregatorConfig)
		if err != nil {
			log.Printf("[WARN] setDynamicAggregatorConnectionData failed for aggregator connection %s: %s", aggregatorConfig.Connection, err)
			failedConnections[aggregatorConfig.Connection] = err
		}
		// TODO notify someone of log messages
		if len(logMessages) > 0 {
			log.Printf("[WARN] %v", logMessages)
		}
	} else {
		firstChild := p.ConnectionMap[aggregatorConfig.ChildConnections[0]]

		// add to connection map using the first child's schema
		p.ConnectionMap[aggregatorConfig.Connection] = NewConnectionData(
			&Connection{Name: aggregatorConfig.Connection},
			firstChild.TableMap,
			firstChild.Schema,
			p,
		)
	}
}

// build the schem for the aggregator connection - find the common subset schema
func (p *Plugin) setDynamicAggregatorConnectionData(aggregatorConfig *proto.ConnectionConfig) (map[string][]string, error) {
	log.Printf("[TRACE] setDynamicAggregatorConnectionData for connection '%s'", aggregatorConfig.Connection)

	var res = NewConnectionData(
		&Connection{Name: aggregatorConfig.Connection},
		make(map[string]*Table),
		&grpc.PluginSchema{
			Schema: make(map[string]*proto.TableSchema),
			Mode:   SchemaModeDynamic,
		},
		p)

	// get child connection schemas

	// initialise the ConnectionConfig table map
	logMessages, err := res.initAggregatorSchema(aggregatorConfig)
	if err != nil {
		return nil, err
	}

	// log messages by connection

	log.Printf("[TRACE] setDynamicAggregatorConnectionData resolving tables to include in aggregator schema")

	// now store connection data
	p.ConnectionMap[aggregatorConfig.Connection] = res
	return logMessages, nil
}
