package plugin_test

// Self-contained reproduction of a data race on the per-query stats counter
// QueryData.queryStatus.hydrateCalls, observable inside a *single* Execute.
//
// The race:
//
//   - WRITERS: every row schedules its hydrate calls via hydrateCall.start,
//     which bumps queryStatus.hydrateCalls; buildRowsAsync fans these out
//     across many concurrent per-row goroutines.
//
//   - READER: the streaming goroutine stamps the running total onto every
//     outgoing row's metadata (QueryMetadata.HydrateCalls) with a plain read.
//
// An unsynchronised writer and reader on the same counter = a data race,
// flagged by `go test -race`. It is NOT a cross-Execute or shared-server
// problem: a single Execute over a hydrate-backed table with enough in-flight
// rows is sufficient. The window only opens when streaming overlaps the per-row
// hydrate scheduling, so it needs a non-trivial row count — a handful of rows
// drains before the goroutines overlap and passes clean.
//
// Impact is observability-only: the counter feeds QueryMetadata.HydrateCalls
// (a stats field). Row payloads are unaffected — all rows still stream. The
// fix types the queryStatus counters as atomic.Int64 and routes every access
// through Load/Add/Store, so a plain (racy) access becomes a compile error.
//
// Run:  go test -race -run TestHydrateCallsCounterRace ./plugin/
//
// raceRows is sized well past the streaming/hydrate overlap threshold so the
// window reliably opens; the same table at ~5 rows passes clean under -race.

import (
	"context"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v6/anywhere"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin/transform"
)

const (
	raceConnection = "race_fixture"
	raceTable      = "race_widget"
	raceRows       = 2000
)

type raceWidget struct {
	ID int
}

// listRaceWidgets streams raceRows items from the List call.
func listRaceWidgets(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
	for i := 0; i < raceRows; i++ {
		d.StreamListItem(ctx, raceWidget{ID: i})
	}
	return nil, nil
}

// raceWidgetSize is a per-row hydrate function: one hydrate call per streamed
// row, so every row contributes a concurrent atomic increment to hydrateCalls.
func raceWidgetSize(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return h.Item.(raceWidget).ID * 10, nil
}

// raceFixturePlugin is a trivial in-process plugin with one hydrate-backed
// table — no cloud provider, no external calls.
func raceFixturePlugin(_ context.Context) *plugin.Plugin {
	return &plugin.Plugin{
		Name: "steampipe-plugin-race-fixture",
		TableMap: map[string]*plugin.Table{
			raceTable: {
				Name: raceTable,
				List: &plugin.ListConfig{Hydrate: listRaceWidgets},
				Columns: []*plugin.Column{
					{Name: "id", Type: proto.ColumnType_INT, Transform: transform.FromField("ID")},
					// `size` resolves from its own hydrate function: one call per row
					{Name: "size", Type: proto.ColumnType_INT, Hydrate: raceWidgetSize, Transform: transform.FromValue()},
				},
			},
		},
	}
}

// TestHydrateCallsCounterRace drives one Execute over a hydrate-backed table
// and drains every row. There is no concurrency at the test layer: a single
// Execute is enough. Under `-race` the detector reports a data race between the
// hydrate-call writers and the streaming-goroutine reader on
// queryStatus.hydrateCalls.
func TestHydrateCallsCounterRace(t *testing.T) {
	server := plugin.Server(&plugin.ServeOpts{PluginFunc: raceFixturePlugin})

	// Disable the query cache: not required for the race, but keeps the run a
	// pure provider-side scan with deterministic stats.
	server.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false, MaxSizeMb: 32})

	cfg := &proto.ConnectionConfig{
		Connection:      raceConnection,
		Plugin:          "steampipe-plugin-race-fixture",
		PluginShortName: "race-fixture",
		PluginInstance:  "steampipe-plugin-race-fixture",
	}
	if _, err := server.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs:        []*proto.ConnectionConfig{cfg},
		MaxCacheSizeMb: 32,
	}); err != nil {
		t.Fatalf("SetAllConnectionConfigs: %v", err)
	}

	ctx := context.Background()
	stream := anywhere.NewLocalPluginStream(ctx)
	qc := proto.NewQueryContext([]string{"id", "size"}, nil, -1, nil)
	ecd := &proto.ExecuteConnectionData{CacheEnabled: false}
	req := &proto.ExecuteRequest{
		Table:                 raceTable,
		QueryContext:          qc,
		CallId:                grpc.BuildCallId(),
		Connection:            raceConnection,
		ExecuteConnectionData: map[string]*proto.ExecuteConnectionData{raceConnection: ecd},
		CacheEnabled:          false,
	}

	server.CallExecuteAsync(req, stream)

	var rows int
	for {
		item, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream.Recv after %d rows: %v", rows, err)
		}
		if item == nil {
			break
		}
		if item.Row != nil {
			rows++
		}
	}

	// Payloads are unaffected by the race — every row still streams. This
	// assertion documents that the race is stats-only, not data loss.
	if rows != raceRows {
		t.Fatalf("streamed %d rows, want %d", rows, raceRows)
	}
}
