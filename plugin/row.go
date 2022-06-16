package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/logging"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RowData contains the row data
type RowData struct {
	// the output of the get/list call which is passed to all other hydrate calls
	Item interface{}
	// if there was a parent-child list call, store the parent list item
	ParentItem     interface{}
	matrixItem     map[string]interface{}
	hydrateResults map[string]interface{}
	hydrateErrors  map[string]error
	mut            sync.Mutex
	waitChan       chan bool
	wg             sync.WaitGroup
	table          *Table
	errorChan      chan error
	queryData      *QueryData
}

// placeholder struct to return when the hydrate function does not return anything
// - this allows us to determine the hydrate function _was_ called
type emptyHydrateResults struct{}

// newRowData creates an empty rowData object
func newRowData(d *QueryData, item interface{}) *RowData {
	// create buffered error channel for any errors occurring hydrate functions (+2 is for the get and list hydrate calls)
	errorChan := make(chan error, len(d.hydrateCalls)+2)

	return &RowData{
		Item:           item,
		matrixItem:     make(map[string]interface{}),
		hydrateResults: make(map[string]interface{}),
		hydrateErrors:  make(map[string]error),
		waitChan:       make(chan bool),
		table:          d.Table,
		errorChan:      errorChan,
		queryData:      d,
	}
}

func (r *RowData) getRow(ctx context.Context) (*proto.Row, error) {
	// NOTE: the RowData (may) have matrixItem set
	// (this is a data structure containing fetch specific data, e.g. region)
	// store this in the context for use by the transform functions
	rowDataCtx := context.WithValue(ctx, context_key.MatrixItem, r.matrixItem)
	// clone the query data and add the matrix properties to quals
	rowQueryData := r.queryData.ShallowCopy()
	rowQueryData.updateQualsWithMatrixItem(r.matrixItem)

	// make any required hydrate function calls
	// - these populate the row with data entries corresponding to the hydrate function name

	if err := r.startAllHydrateCalls(rowDataCtx, rowQueryData); err != nil {
		log.Printf("[WARN] startAllHydrateCalls failed with error %v", err)
		return nil, err
	}

	return r.waitForHydrateCallsToComplete(rowDataCtx)
}

// keep looping round hydrate functions until they are all started
func (r *RowData) startAllHydrateCalls(rowDataCtx context.Context, rowQueryData *QueryData) error {

	// make a map of started hydrate calls for this row - this is used to determine which calls have not started yet
	var callsStarted = map[string]bool{}

	for {
		var allStarted = true
		for _, call := range r.queryData.hydrateCalls {
			hydrateFuncName := call.Name
			// if it is already started, continue to next call
			if callsStarted[hydrateFuncName] {
				continue
			}

			// so call needs to start - can it?
			if call.CanStart(r, hydrateFuncName, r.queryData.concurrencyManager) {
				// execute the hydrate call asynchronously
				call.Start(rowDataCtx, r, rowQueryData, r.queryData.concurrencyManager)
				callsStarted[hydrateFuncName] = true
			} else {
				allStarted = false
			}
			// check for any hydrate errors
			// this is to handle the case that a hydrate function fails which another hydrate function depends on
			// in this case, we will never get out of the start loop as the dependent hydrate function will never start
			select {
			case err := <-r.errorChan:
				log.Printf("[WARN] startAllHydrateCalls failed with error %v", err)
				return err
			default:
			}
		}
		if allStarted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// wait for all hydrate calls to complete
func (r *RowData) waitForHydrateCallsToComplete(rowDataCtx context.Context) (*proto.Row, error) {
	var row *proto.Row

	// start a go routine which signals via the wait chan when all calls are complete
	// (we need this slightly convoluted mechanism to allow us to check for upstream errors
	// by also selecting the errorChan)
	go func() {
		r.wg.Wait()
		logging.LogTime("all hydrate calls complete")
		var err error

		// now execute any transforms required to populate the column values
		row, err = r.getColumnValues(rowDataCtx)
		if err != nil {
			r.queryData.streamError(err)
		}
		close(r.waitChan)
	}()

	// select both wait chan and error chan
	select {
	case err := <-r.errorChan:
		log.Println("[WARN] hydrate error chan select", "error", err)
		return nil, err
	case <-r.waitChan:
		logging.LogTime("send a row")
		return row, nil
	}
}

// generate the column values for all requested columns
func (r *RowData) getColumnValues(ctx context.Context) (*proto.Row, error) {
	row := &proto.Row{Columns: make(map[string]*proto.Column)}

	// queryData.columns contains all columns returned by the hydrate calls which have been executed
	for _, column := range r.queryData.columns {
		val, err := r.table.getColumnValue(ctx, r, column)
		if err != nil {
			return nil, err
		}
		row.Columns[column.Name] = val
	}

	return row, nil
}

// invoke a hydrate function, and set results on the rowData object. Stream errors on the rowData error channel
func (r *RowData) callHydrate(ctx context.Context, d *QueryData, hydrateFunc HydrateFunc, hydrateKey string, hydrateConfig *HydrateConfig) {
	// handle panics in the row hydrate function
	defer func() {
		if p := recover(); p != nil {
			log.Printf("[WARN] callHydrate recover: %v", p)
			r.errorChan <- status.Error(codes.Internal, fmt.Sprintf("hydrate call %s failed with panic %v", hydrateKey, p))
		}
		r.wg.Done()
	}()

	logging.LogTime(hydrateKey + " start")

	log.Printf("[TRACE] callHydrate %s, hydrateConfig %s\n", helpers.GetFunctionName(hydrateFunc), hydrateConfig.String())

	// now call the hydrate function, passing the item and hydrate results so far
	hydrateData, err := r.callHydrateWithRetries(ctx, d, hydrateFunc, hydrateConfig.IgnoreConfig, hydrateConfig.RetryConfig)
	if err != nil {
		log.Printf("[ERROR] callHydrate %s finished with error: %v\n", hydrateKey, err)
		r.setError(hydrateKey, err)
		r.errorChan <- err
	} else {
		// set the hydrate data, even if it is nil
		// (it may legitimately be nil if the hydrate function returned an ignored error)
		// if we do not set it for nil values, we will get error that required hydrate functions hav enot been called
		r.set(hydrateKey, hydrateData)
	}
	logging.LogTime(hydrateKey + " end")
}

// invoke a hydrate function, retrying as required based on the retry config, and return the result and/or error
func (r *RowData) callHydrateWithRetries(ctx context.Context, d *QueryData, hydrateFunc HydrateFunc, ignoreConfig *IgnoreConfig, retryConfig *RetryConfig) (hydrateResult interface{}, err error) {
	ctx, span := telemetry.StartSpan(ctx, r.table.Plugin.Name, "RowData.callHydrateWithRetries (%s)", r.table.Name)

	span.SetAttributes(
		attribute.String("hydrate-func", helpers.GetFunctionName(hydrateFunc)),
	)
	defer func() {
		if err != nil {
			span.SetAttributes(
				attribute.String("err", err.Error()),
			)
		}
		span.End()
	}()

	log.Printf("[TRACE] callHydrateWithRetries: %s", helpers.GetFunctionName(hydrateFunc))
	h := &HydrateData{Item: r.Item, ParentItem: r.ParentItem, HydrateResults: r.hydrateResults}
	// WrapHydrate function returns a HydrateFunc which handles Ignorable errors
	var hydrateWithIgnoreError = WrapHydrate(hydrateFunc, ignoreConfig)
	hydrateResult, err = hydrateWithIgnoreError(ctx, d, h)
	if err != nil {
		log.Printf("[TRACE] hydrateWithIgnoreError returned error %v", err)

		if shouldRetryError(ctx, d, h, err, retryConfig) {
			log.Printf("[TRACE] retrying hydrate")
			hydrateData := &HydrateData{Item: r.Item, ParentItem: r.ParentItem, HydrateResults: r.hydrateResults}
			hydrateResult, err = RetryHydrate(ctx, d, hydrateData, hydrateFunc, retryConfig)
			log.Printf("[TRACE] back from retry")
		}
	}

	return hydrateResult, err
}

func (r *RowData) set(key string, item interface{}) error {
	r.mut.Lock()
	defer r.mut.Unlock()
	if _, ok := r.hydrateResults[key]; ok {
		return fmt.Errorf("failed to save item - row data already contains item for key %s", key)
	}
	r.hydrateResults[key] = item

	return nil
}

func (r *RowData) setError(key string, err error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	if _, ok := r.hydrateErrors[key]; ok {
		log.Printf("[INFO] row data already contains error for key %s", key)
		return
	}
	r.hydrateErrors[key] = err
}

// get the name of the hydrate function which have completed
func (r *RowData) getHydrateKeys() []string {
	r.mut.Lock()
	defer r.mut.Unlock()
	var keys []string
	for key := range r.hydrateResults {
		keys = append(keys, key)
	}
	return keys
}

// GetColumnData returns the root item, and, if this column has a hydrate function registered, the associated hydrate data
func (r *RowData) GetColumnData(column *QueryColumn) (interface{}, error) {
	if column.hydrateName == "" {
		return nil, fmt.Errorf("column %s has no resolved hydrate function name", column.Name)
	}

	if hydrateItem, ok := r.hydrateResults[column.hydrateName]; !ok {
		var errorString string
		err, ok := r.hydrateErrors[column.hydrateName]
		if ok {
			errorString = fmt.Sprintf("table '%s' column '%s' requires hydrate data from %s, which failed with error %v.\n", r.table.Name, column.Name, column.hydrateName, err)
		} else {
			errorString = fmt.Sprintf("table '%s' column '%s' requires hydrate data from %s but none is available.\n", r.table.Name, column.Name, column.hydrateName)
		}
		return nil, fmt.Errorf(errorString)
	} else {
		return hydrateItem, nil
	}
}
