package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"golang.org/x/exp/maps"
	"sort"
	"strings"
	"testing"
)

type initAggregatorTablesByConnectionTest struct {
	aggregatorConfig *proto.ConnectionConfig
	childConnections map[string]*ConnectionData
	expected         map[string][]string
}

var testCasesInitAggregatorTablesByConnection = map[string]initAggregatorTablesByConnectionTest{
	"No spec, all tables match": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				// we only need to sparsely populate table map for this test
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},

		expected: map[string][]string{
			"c1": {"t1", "t2", "t3"},
			"c2": {"t1", "t2", "t3"},
			"c3": {"t1", "t2", "t3"},
		},
	},
	"No spec, all tables match, t1 excluded in table def": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				// we only need to sparsely populate table map for this test
				TableMap: map[string]*Table{
					"t1": {Name: "t1", Aggregation: AggregationModeNone},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1", Aggregation: AggregationModeNone},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1", Aggregation: AggregationModeNone},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},

		expected: map[string][]string{
			"c1": {"t2", "t3"},
			"c2": {"t2", "t3"},
			"c3": {"t2", "t3"},
		},
	},
	"All tables match, t1 excluded in table def but overridden by spec for c3": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
			TableAggregationSpecs: []*proto.TableAggregationSpec{
				{
					Match:       "t1",
					Connections: []string{"c3"},
				},
			},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				// we only need to sparsely populate table map for this test
				TableMap: map[string]*Table{
					"t1": {Name: "t1", Aggregation: AggregationModeNone},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1", Aggregation: AggregationModeNone},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1", Aggregation: AggregationModeNone},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},

		expected: map[string][]string{
			"c1": {"t2", "t3"},
			"c2": {"t2", "t3"},
			"c3": {"t1", "t2", "t3"},
		},
	},
	"All tables match, spec with wildcard for tables and connections": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
			TableAggregationSpecs: []*proto.TableAggregationSpec{
				{
					Match:       "*",
					Connections: []string{"*"},
				},
			},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				// we only need to sparsely populate table map for this test
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},

		expected: map[string][]string{
			"c1": {"t1", "t2", "t3"},
			"c2": {"t1", "t2", "t3"},
			"c3": {"t1", "t2", "t3"},
		},
	},
	"All tables match, spec excludes c1.t1": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
			TableAggregationSpecs: []*proto.TableAggregationSpec{
				{
					Match:       "t1",
					Connections: []string{"c2", "c3"},
				},
			},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				// we only need to sparsely populate table map for this test
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},

		expected: map[string][]string{
			"c1": {"t2", "t3"}, // spec excludes c1.t1
			"c2": {"t1", "t2", "t3"},
			"c3": {"t1", "t2", "t3"},
		},
	},
	"All tables match, spec excludes t*foo (no connections in spec)": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
			TableAggregationSpecs: []*proto.TableAggregationSpec{
				{
					Match:       "t*foo",
					Connections: []string{},
				},
			},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				// we only need to sparsely populate table map for this test
				TableMap: map[string]*Table{
					"t1":    {Name: "t1"},
					"t2foo": {Name: "t2foo"},
					"t3foo": {Name: "t3foo"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1":    {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1":    {Name: "t1"},
					"t2foo": {Name: "t2foo"},
					"t3foo": {Name: "t3foo"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1":    {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1":    {Name: "t1"},
					"t2foo": {Name: "t2foo"},
					"t3foo": {Name: "t3foo"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1":    {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},

		expected: map[string][]string{
			"c1": {"t1"}, // spec excludes *.t*foo (i.e. t2foo, t3foo)
			"c2": {"t1"},
			"c3": {"t1"},
		},
	},
	"All tables match, spec excludes t*foo (mismatching connections in spec)": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
			TableAggregationSpecs: []*proto.TableAggregationSpec{
				{
					Match:       "t*foo",
					Connections: []string{"mismatch*"},
				},
			},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				// we only need to sparsely populate table map for this test
				TableMap: map[string]*Table{
					"t1":    {Name: "t1"},
					"t2foo": {Name: "t2foo"},
					"t3foo": {Name: "t3foo"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1":    {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1":    {Name: "t1"},
					"t2foo": {Name: "t2foo"},
					"t3foo": {Name: "t3foo"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1":    {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1":    {Name: "t1"},
					"t2foo": {Name: "t2foo"},
					"t3foo": {Name: "t3foo"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1":    {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3foo": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},

		expected: map[string][]string{
			"c1": {"t1"}, // spec excludes *.t*foo (i.e. t2foo, t3foo)
			"c2": {"t1"},
			"c3": {"t1"},
		},
	},
	"No spec, t3 does not match": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
			TableAggregationSpecs: []*proto.TableAggregationSpec{
				{
					Match:       "*",
					Connections: []string{"*"},
				},
			},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}}, // missing column
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},
		expected: map[string][]string{
			"c1": {"t1", "t2"},
			"c2": {"t1", "t2"},
			"c3": {"t1", "t2"},
		},
	},
	"No spec, t3 only exists in 1 connection": {
		aggregatorConfig: &proto.ConnectionConfig{
			Connection:       "agg",
			ChildConnections: []string{"c1", "c2", "c3"},
			TableAggregationSpecs: []*proto.TableAggregationSpec{
				{
					Match:       "*",
					Connections: []string{"*"},
				},
			},
		},
		childConnections: map[string]*ConnectionData{
			"c1": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c1"},
				config:     &proto.ConnectionConfig{Connection: "c1"},
			},
			"c2": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c2"},
				config:     &proto.ConnectionConfig{Connection: "c2"},
			},
			"c3": {
				TableMap: map[string]*Table{
					"t1": {Name: "t1"},
					"t2": {Name: "t2"},
					"t3": {Name: "t3"},
				},
				Schema: &grpc.PluginSchema{
					Schema: map[string]*proto.TableSchema{
						"t1": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t2": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c2", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
						"t3": {Columns: []*proto.ColumnDefinition{{Name: "c1", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}, {Name: "c3", Type: proto.ColumnType_STRING}}},
					},
					Mode: SchemaModeDynamic,
				},
				Connection: &Connection{Name: "c3"},
				config:     &proto.ConnectionConfig{Connection: "c3"},
			},
		},
		expected: map[string][]string{
			"c1": {"t1", "t2"},
			"c2": {"t1", "t2"},
			"c3": {"t1", "t2", "t3"},
		},
	},
}

func TestInitAggregatorTablesByConnection(t *testing.T) {

	for _, test := range testCasesInitAggregatorTablesByConnection {
		connectionData := NewConnectionData(
			&Connection{Name: test.aggregatorConfig.Connection},
			&Plugin{ConnectionMap: test.childConnections},
			test.aggregatorConfig,
		)

		// do it
		connectionData.initAggregatorSchema(test.aggregatorConfig)

		for connectionName, expectedTableNames := range test.expected {
			tablesForConnection, ok := connectionData.AggregatedTablesByConnection[connectionName]
			if !ok {
				t.Fatalf("expected connection %s, tables: %v, got no connection", connectionName, expectedTableNames)
			}
			actualNames := maps.Keys(tablesForConnection)
			sort.Strings(actualNames)
			if strings.Join(expectedTableNames, ",") != strings.Join(actualNames, ",") {
				t.Fatalf("connection '%s', expected tables: %v, got: %v", connectionName, expectedTableNames, actualNames)
			}
		}
	}
}
