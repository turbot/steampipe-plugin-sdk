package plugin

import "path"

// getConnectionTempDir appends the connection name to the plugin temporary directory path
func getConnectionTempDir(pluginTempDir string, connectionName string) string {
	return path.Join(pluginTempDir, connectionName)
}
