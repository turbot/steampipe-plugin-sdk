package plugin_test

// A hydrate function named only in another hydrate's Depends list has no
// HydrateConfig entry of its own and is no column's hydrate. The SDK must
// still make a config for it, call it before the function that depends on it,
// and hand its result on, so the dependent column has its value (issue #969).

import (
	"context"
	"fmt"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v6/anywhere"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin/transform"
)

const (
	dependsConnection = "depends_fixture"
	dependsPluginName = "steampipe-plugin-depends-fixture"
	dependsRows       = 3
)

type dependsItem struct {
	ID int
}

func listDependsItems(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
	for i := 0; i < dependsRows; i++ {
		d.StreamListItem(ctx, dependsItem{ID: i})
	}
	return nil, nil
}

// getDependsTags is named only in a Depends list
func getDependsTags(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return fmt.Sprintf("tags-%d", h.Item.(dependsItem).ID), nil
}

// getDependsDetails reads the result of its dependency
func getDependsDetails(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	tags, ok := h.HydrateResults["getDependsTags"].(string)
	if !ok {
		return "", nil
	}
	return "details-" + tags, nil
}

// getDependsExtra has its own HydrateConfig entry, and is also a dependency
func getDependsExtra(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return fmt.Sprintf("extra-%d", h.Item.(dependsItem).ID), nil
}

// getDependsColumn is a column's hydrate, and is also a dependency
func getDependsColumn(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return fmt.Sprintf("col-%d", h.Item.(dependsItem).ID), nil
}

func getDependsCombined(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	extra, _ := h.HydrateResults["getDependsExtra"].(string)
	col, _ := h.HydrateResults["getDependsColumn"].(string)
	return extra + "+" + col, nil
}

func dependsFixturePlugin(_ context.Context) *plugin.Plugin {
	return &plugin.Plugin{
		Name: dependsPluginName,
		TableMap: map[string]*plugin.Table{
			"depends_widget": {
				Name: "depends_widget",
				List: &plugin.ListConfig{Hydrate: listDependsItems},
				HydrateConfig: []plugin.HydrateConfig{
					// getDependsTags has no entry of its own
					{Func: getDependsDetails, Depends: []plugin.HydrateFunc{getDependsTags}},
					// getDependsExtra has its own entry, getDependsColumn is a column's hydrate
					{Func: getDependsExtra},
					{Func: getDependsCombined, Depends: []plugin.HydrateFunc{getDependsExtra, getDependsColumn}},
				},
				Columns: []*plugin.Column{
					{Name: "id", Type: proto.ColumnType_INT, Transform: transform.FromField("ID")},
					{Name: "details", Type: proto.ColumnType_STRING, Hydrate: getDependsDetails, Transform: transform.FromValue()},
					{Name: "combined", Type: proto.ColumnType_STRING, Hydrate: getDependsCombined, Transform: transform.FromValue()},
					{Name: "col", Type: proto.ColumnType_STRING, Hydrate: getDependsColumn, Transform: transform.FromValue()},
				},
			},
		},
	}
}

func queryDependsTable(t *testing.T, columns []string) map[int]map[string]string {
	t.Helper()
	server := plugin.Server(&plugin.ServeOpts{PluginFunc: dependsFixturePlugin})
	if _, err := server.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false, MaxSizeMb: 32}); err != nil {
		t.Fatalf("SetCacheOptions failed: %v", err)
	}
	cfg := &proto.ConnectionConfig{
		Connection:      dependsConnection,
		Plugin:          dependsPluginName,
		PluginShortName: "depends-fixture",
		PluginInstance:  dependsPluginName,
	}
	if _, err := server.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs:        []*proto.ConnectionConfig{cfg},
		MaxCacheSizeMb: 32,
	}); err != nil {
		t.Fatalf("SetAllConnectionConfigs: %v", err)
	}

	ctx := context.Background()
	stream := anywhere.NewLocalPluginStream(ctx)
	req := &proto.ExecuteRequest{
		Table:                 "depends_widget",
		QueryContext:          proto.NewQueryContext(columns, nil, -1, nil),
		CallId:                grpc.BuildCallId(),
		Connection:            dependsConnection,
		ExecuteConnectionData: map[string]*proto.ExecuteConnectionData{dependsConnection: {CacheEnabled: false}},
		CacheEnabled:          false,
	}
	server.CallExecuteAsync(req, stream)

	rows := map[int]map[string]string{}
	for {
		item, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream.Recv after %d rows: %v", len(rows), err)
		}
		if item == nil {
			break
		}
		if item.Row == nil {
			continue
		}
		cols := item.Row.Columns
		id := int(cols["id"].GetIntValue())
		row := map[string]string{}
		for _, c := range columns {
			row[c] = cols[c].GetStringValue()
		}
		rows[id] = row
	}
	return rows
}

// A function named only in a Depends list is called, and the column that depends on it has its value.
func TestHydrateDependsOnFunctionWithNoConfig(t *testing.T) {
	rows := queryDependsTable(t, []string{"id", "details"})
	if len(rows) != dependsRows {
		t.Fatalf("got %d rows, want %d", len(rows), dependsRows)
	}
	for i := 0; i < dependsRows; i++ {
		want := fmt.Sprintf("details-tags-%d", i)
		if got := rows[i]["details"]; got != want {
			t.Errorf("row %d: details = %q, want %q", i, got, want)
		}
	}
}

// A dependency that already has its own config, or is a column's hydrate, still works
func TestHydrateDependsOnConfiguredFunctions(t *testing.T) {
	rows := queryDependsTable(t, []string{"id", "combined", "col"})
	if len(rows) != dependsRows {
		t.Fatalf("got %d rows, want %d", len(rows), dependsRows)
	}
	for i := 0; i < dependsRows; i++ {
		if got, want := rows[i]["combined"], fmt.Sprintf("extra-%d+col-%d", i, i); got != want {
			t.Errorf("row %d: combined = %q, want %q", i, got, want)
		}
		if got, want := rows[i]["col"], fmt.Sprintf("col-%d", i); got != want {
			t.Errorf("row %d: col = %q, want %q", i, got, want)
		}
	}
}
