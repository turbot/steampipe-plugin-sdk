package plugin

import (
	connection_manager "github.com/turbot/steampipe-plugin-sdk/v3/connection"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"log"
)

// ConnectionData is the data stored by the plugin which is connection dependent
type ConnectionData struct {
	// TableMap is a map of all the tables in the plugin, keyed by the table name
	TableMap map[string]*Table
	// connection this plugin is instantiated for
	Connection *Connection
	// schema - this may be connection specific for dynamic schemas
	Schema map[string]*proto.TableSchema
	// object to handle caching of connection specific data
	connectionManager *connection_manager.Manager
}

// ConnectionManager lazy loads the connection manager
func (d *ConnectionData) ConnectionManager() *connection_manager.Manager {
	if d.connectionManager == nil {
		log.Printf("[WARN] creating connection manager for %s", d.Connection.Name)
		d.connectionManager = connection_manager.NewManager()
	}
	return d.connectionManager
}
