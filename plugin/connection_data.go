package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ConnectionData is the data stored by the plugin which is connection dependent
type ConnectionData struct {
	// TableMap is a map of all the tables in the plugin, keyed by the table name
	TableMap map[string]*Table
	// connection this plugin is instantiated for
	Connection *Connection
	// schema - this may be connection specific for dynamic schemas
	Schema map[string]*proto.TableSchema
}
