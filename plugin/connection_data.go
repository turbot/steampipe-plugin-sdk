package plugin

import (
	connection_manager "github.com/turbot/steampipe-plugin-sdk/v3/connection"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

// ConnectionData is the data stored by the plugin which is connection dependent
type ConnectionData struct {
	// TableMap is a map of all the tables in the plugin, keyed by the table name
	TableMap map[string]*Table
	// connection this plugin is instantiated for
	Connection *Connection
	// object to handle caching of connection specific data
	ConnectionManager *connection_manager.Manager
	// schema - this may be connection specific for dynamic schemas
	Schema map[string]*proto.TableSchema
}
