package plugin

import "github.com/turbot/steampipe-plugin-sdk/v5/connection"

type TableMapData struct {
	Connection      *Connection
	ConnectionCache *connection.ConnectionCache
	tempDir         string
}

// GetSourceFiles accept a source path downloads files if necessary, and returns a list of local file paths
func (d *TableMapData) GetSourceFiles(source string) ([]string, error) {
	return getSourceFiles(source, d.tempDir)
}
