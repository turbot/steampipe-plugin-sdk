package grpc

import "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"

type PluginSchema struct {
	Schema       map[string]*proto.TableSchema
	Mode         string
	RateLimiters []*proto.RateLimiterDefinition
}

func NewPluginSchema(mode string) *PluginSchema {
	return &PluginSchema{
		Schema: make(map[string]*proto.TableSchema),
		Mode:   mode,
	}
}

func (x *PluginSchema) Equals(other *PluginSchema) bool {
	if len(x.Schema) != len(other.Schema) {
		return false
	}
	for k, schema := range x.Schema {
		otherSchema, ok := other.Schema[k]
		if !ok {
			return false
		}
		if !otherSchema.Equals(schema) {
			return false
		}
	}
	return x.Mode == other.Mode
}
