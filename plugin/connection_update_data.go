package plugin

import "github.com/turbot/steampipe-plugin-sdk/v5/grpc"

type connectionUpdateData struct {
	failedConnections map[string]error
	exemplarSchema    *grpc.PluginSchema
	exemplarTableMap  map[string]*Table
}

func NewConnectionUpdateData() *connectionUpdateData {
	return &connectionUpdateData{
		failedConnections: make(map[string]error),
	}
}
