package plugin_test

// Tests for Plugin.ClientWaitsForHydrateRateLimit and WaitForHydrateRateLimit.
//
// The hydrate functions here call plugin.WaitForHydrateRateLimit where a plugin's API client
// would, just before it sends a request.
//
// Most tests count rate limiter tokens without timing them. A fixture's limiter starts with
// `budget` tokens and refills one token every 100 s. A query that needs no more than `budget`
// tokens finishes at once; a query that needs one more waits 100 s, and the test sees it still
// running after a short timeout. So "finishes with budget k" shows the query used at most k
// tokens, and "still waiting with budget k-1" shows it used k. The tests of waits made at the
// same time, of cancelled waits and of diagnostics time the waits instead, with at least 200 ms
// between the time they expect and their cutoff, so a row that starts late does not fail them.
//
// Every hydrate function is a named function: the SDK names a hydrate call after the last part
// of its Go function name, and two closures would both be named "func1".

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/anywhere"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin/transform"
	"github.com/turbot/steampipe-plugin-sdk/v6/rate_limiter"
)

const (
	rlConnection = "rate_limit_fixture"
	rlTable      = "rl_widget"
	rlLimiter    = "rl_widget_limiter"
	// a limiter that refills this slowly never refills during a test
	rlSlowFill = 0.01

	// how long a query that needs no more tokens than its budget may take
	rlFinishTimeout = 10 * time.Second
	// how long a query that needs one more token than its budget is watched before it counts as waiting
	rlWaitTimeout = time.Second
)

var widgetTags = map[string]string{"service": "widget"}

type rlWidget struct {
	ID int
}

type rlFixture struct {
	rows        int
	clientWaits bool
	limiter     *rate_limiter.Definition
	// tags for the list call
	listTags map[string]string
	// the hydrate config for each extra column, keyed by column name
	columns map[string]plugin.HydrateConfig
}

func (f rlFixture) list(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
	// as a client would before the list request: a list call is not a hydrate call, so this does nothing
	plugin.WaitForHydrateRateLimit(ctx)
	for i := 0; i < f.rows; i++ {
		d.StreamListItem(ctx, rlWidget{ID: i})
	}
	return nil, nil
}

func (f rlFixture) plugin(_ context.Context) *plugin.Plugin {
	columns := []*plugin.Column{
		{Name: "id", Type: proto.ColumnType_INT, Transform: transform.FromField("ID")},
	}
	var hydrateConfig []plugin.HydrateConfig
	for name, c := range f.columns {
		columns = append(columns, &plugin.Column{Name: name, Type: proto.ColumnType_INT, Hydrate: c.Func, Transform: transform.FromValue()})
		hydrateConfig = append(hydrateConfig, c)
	}
	return &plugin.Plugin{
		Name:                           "steampipe-plugin-rate-limit-fixture",
		RateLimiters:                   []*rate_limiter.Definition{f.limiter},
		ClientWaitsForHydrateRateLimit: f.clientWaits,
		TableMap: map[string]*plugin.Table{
			rlTable: {
				Name:          rlTable,
				List:          &plugin.ListConfig{Hydrate: f.list, Tags: f.listTags},
				HydrateConfig: hydrateConfig,
				Columns:       columns,
			},
		},
	}
}

// slowLimiter starts with budget tokens and does not refill during a test.
func slowLimiter(budget int64) *rate_limiter.Definition {
	return &rate_limiter.Definition{
		Name:       rlLimiter,
		FillRate:   rlSlowFill,
		BucketSize: budget,
		Scope:      []string{"connection"},
		Where:      "service = 'widget'",
	}
}

type rlServer struct {
	server *grpc.PluginServer
}

func newRlServer(t *testing.T, f rlFixture) *rlServer {
	t.Helper()
	server := plugin.Server(&plugin.ServeOpts{PluginFunc: f.plugin})
	if _, err := server.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false, MaxSizeMb: 32}); err != nil {
		t.Fatalf("SetCacheOptions: %v", err)
	}
	cfg := &proto.ConnectionConfig{
		Connection:      rlConnection,
		Plugin:          "steampipe-plugin-rate-limit-fixture",
		PluginShortName: "rate-limit-fixture",
		PluginInstance:  "steampipe-plugin-rate-limit-fixture",
	}
	if _, err := server.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs:        []*proto.ConnectionConfig{cfg},
		MaxCacheSizeMb: 32,
	}); err != nil {
		t.Fatalf("SetAllConnectionConfigs: %v", err)
	}
	return &rlServer{server: server}
}

// query runs one query for columns. It returns the rows, and false when the query
// was still running after timeout (it is then cancelled).
func (s *rlServer) query(t *testing.T, columns []string, timeout time.Duration) ([]*proto.Row, bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := anywhere.NewLocalPluginStream(ctx)
	req := &proto.ExecuteRequest{
		Table:                 rlTable,
		QueryContext:          proto.NewQueryContext(columns, nil, -1, nil),
		CallId:                grpc.BuildCallId(),
		Connection:            rlConnection,
		ExecuteConnectionData: map[string]*proto.ExecuteConnectionData{rlConnection: {CacheEnabled: false}},
		CacheEnabled:          false,
	}
	s.server.CallExecuteAsync(req, stream)

	type result struct {
		rows []*proto.Row
		err  error
	}
	done := make(chan result, 1)
	go func() {
		var rows []*proto.Row
		for {
			item, err := stream.Recv()
			if err != nil || item == nil {
				done <- result{rows, err}
				return
			}
			if item.Row != nil {
				rows = append(rows, item.Row)
			}
		}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("query failed after %d rows: %v", len(r.rows), r.err)
		}
		return r.rows, true
	case <-time.After(timeout):
		cancel()
		// let the cancelled query wind down before the next one starts
		select {
		case <-done:
		case <-time.After(rlFinishTimeout):
			t.Fatalf("query did not stop after it was cancelled")
		}
		return nil, false
	}
}

// assertTokens checks that the query for columns uses exactly want tokens (want > 0):
// it finishes with a budget of want, and is still waiting with a budget of want-1.
func assertTokens(t *testing.T, fixture func(budget int64) rlFixture, columns []string, want int64) {
	t.Helper()
	f := fixture(want)
	rows, finished := newRlServer(t, f).query(t, columns, rlFinishTimeout)
	if !finished {
		t.Fatalf("with %d tokens the query was still waiting after %s, so it needs more than %d", want, rlFinishTimeout, want)
	}
	if len(rows) != f.rows {
		t.Fatalf("got %d rows, want %d", len(rows), f.rows)
	}
	if _, finished := newRlServer(t, fixture(want-1)).query(t, columns, rlWaitTimeout); finished {
		t.Fatalf("with %d tokens the query finished, so it needs fewer than %d", want-1, want)
	}
}

// oneColumn is a fixture with one hydrate column, "value", and the slow limiter.
func oneColumn(rows int, clientWaits bool, config plugin.HydrateConfig) func(budget int64) rlFixture {
	return func(budget int64) rlFixture {
		return rlFixture{rows: rows, clientWaits: clientWaits, limiter: slowLimiter(budget), columns: map[string]plugin.HydrateConfig{"value": config}}
	}
}

func returnEarly(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return h.Item.(rlWidget).ID, nil
}

func sendOneRequest(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	plugin.WaitForHydrateRateLimit(ctx)
	return h.Item.(rlWidget).ID, nil
}

func sendTwoRequests(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	plugin.WaitForHydrateRateLimit(ctx)
	plugin.WaitForHydrateRateLimit(ctx)
	return h.Item.(rlWidget).ID, nil
}

// sendThreeRequestsAtOnce sends three requests from three goroutines.
func sendThreeRequestsAtOnce(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			plugin.WaitForHydrateRateLimit(ctx)
		}()
	}
	wg.Wait()
	return h.Item.(rlWidget).ID, nil
}

// firstOfThreeRequestsAtOnce sends three requests from three goroutines, and returns how many
// milliseconds the first of them waited.
func firstOfThreeRequestsAtOnce(ctx context.Context, _ *plugin.QueryData, _ *plugin.HydrateData) (any, error) {
	start := time.Now()
	waited := make(chan time.Duration, 3)
	for i := 0; i < 3; i++ {
		go func() {
			plugin.WaitForHydrateRateLimit(ctx)
			waited <- time.Since(start)
		}()
	}
	first := <-waited
	<-waited
	<-waited
	return int(first.Milliseconds()), nil
}

// sendAfterACancelledWait gives up its first wait after 50 ms, then waits again and sends its request.
func sendAfterACancelledWait(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	plugin.WaitForHydrateRateLimit(short)
	cancel()
	plugin.WaitForHydrateRateLimit(ctx)
	return h.Item.(rlWidget).ID, nil
}

// waitWithCancelledContext waits with a ctx that is already cancelled, so it sends no request.
func waitWithCancelledContext(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	plugin.WaitForHydrateRateLimit(cancelled)
	return h.Item.(rlWidget).ID, nil
}

// workWithoutRequest takes 250 ms and sends no request.
func workWithoutRequest(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	time.Sleep(250 * time.Millisecond)
	return h.Item.(rlWidget).ID, nil
}

// sendOneSlowRequest sends a request that takes 250 ms.
func sendOneSlowRequest(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	plugin.WaitForHydrateRateLimit(ctx)
	time.Sleep(250 * time.Millisecond)
	return h.Item.(rlWidget).ID, nil
}

// sendRequestForRowZero needs one token for the whole query: only the row with ID 0 sends a request.
func sendRequestForRowZero(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	id := h.Item.(rlWidget).ID
	if id == 0 {
		plugin.WaitForHydrateRateLimit(ctx)
	}
	return id, nil
}

var errRlRetry = errors.New("retry me")

// rlAttempts counts the attempts of sendRequestThenRetry for each row
var rlAttempts = struct {
	mut sync.Mutex
	n   map[int]int
}{n: map[int]int{}}

func resetRlAttempts() {
	rlAttempts.mut.Lock()
	defer rlAttempts.mut.Unlock()
	rlAttempts.n = map[int]int{}
}

// sendRequestThenRetry sends a request on each attempt, and fails its first attempt with an error that is retried.
func sendRequestThenRetry(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	plugin.WaitForHydrateRateLimit(ctx)
	id := h.Item.(rlWidget).ID
	rlAttempts.mut.Lock()
	rlAttempts.n[id]++
	attempt := rlAttempts.n[id]
	rlAttempts.mut.Unlock()
	if attempt == 1 {
		return nil, errRlRetry
	}
	return id, nil
}

var rlInFlight, rlMaxInFlight atomic.Int64

// sendSlowRequest records how many calls run at once.
func sendSlowRequest(ctx context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	n := rlInFlight.Add(1)
	defer rlInFlight.Add(-1)
	for {
		m := rlMaxInFlight.Load()
		if n <= m || rlMaxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	plugin.WaitForHydrateRateLimit(ctx)
	time.Sleep(20 * time.Millisecond)
	return h.Item.(rlWidget).ID, nil
}

func TestClientWaits_NoRequestUsesNoToken(t *testing.T) {
	// one token; three rows that send no request, then a query that needs the one token
	s := newRlServer(t, rlFixture{
		rows:        3,
		clientWaits: true,
		limiter:     slowLimiter(1),
		columns: map[string]plugin.HydrateConfig{
			"early": {Func: returnEarly, Tags: widgetTags},
			"probe": {Func: sendRequestForRowZero, Tags: widgetTags},
		},
	})
	if rows, finished := s.query(t, []string{"id", "early"}, rlFinishTimeout); !finished || len(rows) != 3 {
		t.Fatalf("rows that send no request waited for a token: finished %v, %d rows", finished, len(rows))
	}
	if _, finished := s.query(t, []string{"id", "probe"}, rlFinishTimeout); !finished {
		t.Fatalf("the one token was gone, so the rows that sent no request used it")
	}
}

func TestClientWaits_OneRequestUsesOneToken(t *testing.T) {
	assertTokens(t, oneColumn(3, true, plugin.HydrateConfig{Func: sendOneRequest, Tags: widgetTags}), []string{"id", "value"}, 3)
}

func TestClientWaits_TwoRequestsUseOneToken(t *testing.T) {
	assertTokens(t, oneColumn(2, true, plugin.HydrateConfig{Func: sendTwoRequests, Tags: widgetTags}), []string{"id", "value"}, 2)
}

func TestClientWaits_RequestsAtOnceUseOneToken(t *testing.T) {
	assertTokens(t, oneColumn(2, true, plugin.HydrateConfig{Func: sendThreeRequestsAtOnce, Tags: widgetTags}), []string{"id", "value"}, 2)
}

func TestClientWaits_RetryUsesNoMoreTokens(t *testing.T) {
	retried := func(budget int64) rlFixture {
		resetRlAttempts()
		return oneColumn(2, true, plugin.HydrateConfig{
			Func: sendRequestThenRetry,
			Tags: widgetTags,
			RetryConfig: &plugin.RetryConfig{
				ShouldRetryErrorFunc: func(_ context.Context, _ *plugin.QueryData, _ *plugin.HydrateData, err error) bool {
					return errors.Is(err, errRlRetry)
				},
				RetryInterval: 1,
			},
		})(budget)
	}
	// two rows, two attempts each: one token for each row, as when the SDK waits
	assertTokens(t, retried, []string{"id", "value"}, 2)
}

func TestClientWaits_NoMatchingLimiterDoesNotWait(t *testing.T) {
	// the hydrate has no tags, so the limiter does not apply to it
	rows, finished := newRlServer(t, oneColumn(3, true, plugin.HydrateConfig{Func: sendOneRequest})(1)).query(t, []string{"id", "value"}, rlFinishTimeout)
	if !finished || len(rows) != 3 {
		t.Fatalf("a hydrate with no limiter waited: finished %v, %d rows", finished, len(rows))
	}
}

func TestClientWaits_ListCallIsUnchanged(t *testing.T) {
	// the list call is rate limited too: the SDK waits once before it calls the list, and the
	// WaitForHydrateRateLimit in the list takes no second token, so one token is enough
	f := rlFixture{rows: 3, clientWaits: true, limiter: slowLimiter(1), listTags: widgetTags}
	if rows, finished := newRlServer(t, f).query(t, []string{"id"}, rlFinishTimeout); !finished || len(rows) != 3 {
		t.Fatalf("a list call used more than one token: finished %v, %d rows", finished, len(rows))
	}
}

func TestClientWaits_KeepsMaxConcurrency(t *testing.T) {
	rlInFlight.Store(0)
	rlMaxInFlight.Store(0)
	f := rlFixture{
		rows:        5,
		clientWaits: true,
		limiter: &rate_limiter.Definition{
			Name:           rlLimiter,
			MaxConcurrency: 1,
			Scope:          []string{"connection"},
			Where:          "service = 'widget'",
		},
		columns: map[string]plugin.HydrateConfig{"value": {Func: sendSlowRequest, Tags: widgetTags}},
	}
	rows, finished := newRlServer(t, f).query(t, []string{"id", "value"}, rlFinishTimeout)
	if !finished || len(rows) != 5 {
		t.Fatalf("finished %v, %d rows, want 5", finished, len(rows))
	}
	if got := rlMaxInFlight.Load(); got != 1 {
		t.Fatalf("%d calls ran at once, want 1", got)
	}
}

func TestClientWaits_RequestsAtOnceWaitForTheToken(t *testing.T) {
	// 2 tokens a second and a bucket of 1: the second row's token comes after 500 ms,
	// and none of its three requests may go before it
	f := rlFixture{
		rows:        2,
		clientWaits: true,
		limiter:     &rate_limiter.Definition{Name: rlLimiter, FillRate: 2, BucketSize: 1, Scope: []string{"connection"}, Where: "service = 'widget'"},
		columns:     map[string]plugin.HydrateConfig{"value": {Func: firstOfThreeRequestsAtOnce, Tags: widgetTags}},
	}
	rows, finished := newRlServer(t, f).query(t, []string{"id", "value"}, rlFinishTimeout)
	if !finished || len(rows) != 2 {
		t.Fatalf("finished %v, %d rows, want 2", finished, len(rows))
	}
	var slowest int64
	for _, row := range rows {
		slowest = max(slowest, row.Columns["value"].GetIntValue())
	}
	if slowest < 300 {
		t.Fatalf("a request went after %d ms, before its call had its token (about 500 ms)", slowest)
	}
}

func TestClientWaits_CancelledWaitTakesNoToken(t *testing.T) {
	// each row waits once with a 50 ms deadline, then again. With one token, one row gets it at once
	// and the other row's first wait gives up: its second wait must still need a token of its own.
	assertTokens(t, oneColumn(2, true, plugin.HydrateConfig{Func: sendAfterACancelledWait, Tags: widgetTags}), []string{"id", "value"}, 2)

	// 2 tokens a second and a bucket of 1: one row takes the first token at once. The other row's
	// first wait gives up after 50 ms, with no other wait queued behind it, so the limiter gets
	// its token back. The row's second wait then gets the token due at 500 ms, not the one at 1 s.
	f := rlFixture{
		rows:        2,
		clientWaits: true,
		limiter:     &rate_limiter.Definition{Name: rlLimiter, FillRate: 2, BucketSize: 1, Scope: []string{"connection"}, Where: "service = 'widget'"},
		columns:     map[string]plugin.HydrateConfig{"value": {Func: sendAfterACancelledWait, Tags: widgetTags}},
	}
	start := time.Now()
	rows, finished := newRlServer(t, f).query(t, []string{"id", "value"}, rlFinishTimeout)
	if !finished || len(rows) != 2 {
		t.Fatalf("finished %v, %d rows, want 2", finished, len(rows))
	}
	if took := time.Since(start); took >= 750*time.Millisecond {
		t.Fatalf("the query took %v, so the cancelled wait kept its token (want about 500 ms)", took)
	}
}

func TestClientWaits_WaitWithCancelledContextTakesNoToken(t *testing.T) {
	// one token; three rows that wait with a cancelled ctx, then a query that needs the one token
	s := newRlServer(t, rlFixture{
		rows:        3,
		clientWaits: true,
		limiter:     slowLimiter(1),
		columns: map[string]plugin.HydrateConfig{
			"cancelled": {Func: waitWithCancelledContext, Tags: widgetTags},
			"probe":     {Func: sendRequestForRowZero, Tags: widgetTags},
		},
	})
	if rows, finished := s.query(t, []string{"id", "cancelled"}, rlFinishTimeout); !finished || len(rows) != 3 {
		t.Fatalf("rows that wait with a cancelled ctx waited for a token: finished %v, %d rows", finished, len(rows))
	}
	if _, finished := s.query(t, []string{"id", "probe"}, rlFinishTimeout); !finished {
		t.Fatalf("a wait with a cancelled ctx took the only token")
	}
}

func TestSDKWaits_NoRequestStillUsesAToken(t *testing.T) {
	// without the setting, the SDK waits before each hydrate call runs, as before
	assertTokens(t, oneColumn(3, false, plugin.HydrateConfig{Func: returnEarly, Tags: widgetTags}), []string{"id", "value"}, 3)
}

func TestSDKWaits_WaitForHydrateRateLimitDoesNothing(t *testing.T) {
	// without the setting, the SDK has used one token for each row, and the client's wait adds none
	assertTokens(t, oneColumn(3, false, plugin.HydrateConfig{Func: sendOneRequest, Tags: widgetTags}), []string{"id", "value"}, 3)
}

type rlCtx struct {
	Diagnostics struct {
		Calls []struct {
			Type         string   `json:"type"`
			RateLimiters []string `json:"rate_limiters"`
			DelayMs      int64    `json:"rate_limiter_delay_ms"`
		} `json:"calls"`
	} `json:"diagnostics"`
}

// hydrateDelays returns the rate limiter delay of the one hydrate call in each row's _ctx.
func hydrateDelays(t *testing.T, rows []*proto.Row) []int64 {
	t.Helper()
	var delays []int64
	for _, row := range rows {
		var c rlCtx
		if err := json.Unmarshal(row.Columns["_ctx"].GetJsonValue(), &c); err != nil {
			t.Fatalf("_ctx: %v", err)
		}
		for _, call := range c.Diagnostics.Calls {
			if call.Type != "hydrate" {
				continue
			}
			if len(call.RateLimiters) != 1 || call.RateLimiters[0] != rlLimiter {
				t.Fatalf("hydrate call rate limiters = %v, want [%s]", call.RateLimiters, rlLimiter)
			}
			delays = append(delays, call.DelayMs)
		}
	}
	if len(delays) != len(rows) {
		t.Fatalf("found %d hydrate calls in %d rows", len(delays), len(rows))
	}
	return delays
}

func TestRateLimitDelayShowsInDiagnostics(t *testing.T) {
	t.Setenv(plugin.EnvDiagnosticsLevel, plugin.DiagnosticsAll)

	// 4 tokens a second and a bucket of 1: of three rows that send a request, two wait 250 ms or more,
	// so two show 50 ms or more
	fastLimiter := &rate_limiter.Definition{Name: rlLimiter, FillRate: 4, BucketSize: 1, Scope: []string{"connection"}, Where: "service = 'widget'"}
	for _, clientWaits := range []bool{true, false} {
		f := rlFixture{rows: 3, clientWaits: clientWaits, limiter: fastLimiter, columns: map[string]plugin.HydrateConfig{"value": {Func: sendOneRequest, Tags: widgetTags}}}
		rows, finished := newRlServer(t, f).query(t, []string{"id", "value"}, rlFinishTimeout)
		if !finished {
			t.Fatalf("client waits %v: query did not finish", clientWaits)
		}
		var waited int
		for _, d := range hydrateDelays(t, rows) {
			if d >= 50 {
				waited++
			}
		}
		if waited < 2 {
			t.Fatalf("client waits %v: %d rows show a rate limiter delay of 50 ms or more, want 2", clientWaits, waited)
		}
	}

	// one call at a time, each taking 250 ms: the rows that wait for the slot show that wait as their delay,
	// also when the call sends no request
	oneAtATime := &rate_limiter.Definition{Name: rlLimiter, MaxConcurrency: 1, Scope: []string{"connection"}, Where: "service = 'widget'"}
	for _, c := range []struct {
		clientWaits bool
		hydrate     plugin.HydrateFunc
	}{{true, sendOneSlowRequest}, {false, sendOneSlowRequest}, {true, workWithoutRequest}} {
		f := rlFixture{rows: 3, clientWaits: c.clientWaits, limiter: oneAtATime, columns: map[string]plugin.HydrateConfig{"value": {Func: c.hydrate, Tags: widgetTags}}}
		rows, finished := newRlServer(t, f).query(t, []string{"id", "value"}, rlFinishTimeout)
		if !finished {
			t.Fatalf("client waits %v: query did not finish", c.clientWaits)
		}
		var waited int
		for _, d := range hydrateDelays(t, rows) {
			if d >= 50 {
				waited++
			}
		}
		if waited < 2 {
			t.Fatalf("client waits %v: %d rows show a concurrency delay of 50 ms or more, want 2", c.clientWaits, waited)
		}
	}

	// rows that send no request show no delay
	f := rlFixture{rows: 3, clientWaits: true, limiter: fastLimiter, columns: map[string]plugin.HydrateConfig{"value": {Func: returnEarly, Tags: widgetTags}}}
	rows, finished := newRlServer(t, f).query(t, []string{"id", "value"}, rlFinishTimeout)
	if !finished {
		t.Fatalf("query did not finish")
	}
	for _, d := range hydrateDelays(t, rows) {
		if d != 0 {
			t.Fatalf("a row that sent no request shows a delay of %d ms", d)
		}
	}
}
