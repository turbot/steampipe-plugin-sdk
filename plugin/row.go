package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"

	"github.com/turbot/go-kit/helpers"
	pb "github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RowData :: struct containing row data

type RowData struct {
	// the output of the get/list call which is passed to all other hydrate calls
	Item           interface{}
	fetchMetadata  map[string]interface{}
	hydrateResults map[string]interface{}
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

// newRowData :: create an empty rowData object
func newRowData(d *QueryData, item interface{}) *RowData {
	// create buffered error channel for any errors occurring hydrate functions (+2 is for the get and list hydrate calls)
	errorChan := make(chan error, len(d.hydrateCalls)+2)

	return &RowData{
		hydrateResults: make(map[string]interface{}),
		waitChan:       make(chan bool),
		table:          d.Table,
		errorChan:      errorChan,
		queryData:      d,
		fetchMetadata:  map[string]interface{}{},
	}
}

func (r *RowData) getRow(ctx context.Context) (*pb.Row, error) {

	// NOTE: the RowData (may) have fetchMetadata set
	// (this is a data structure containing fetch specific data, e.g. region)
	// store this in the context for use by the transform functions
	rowDataCtx := context.WithValue(ctx, context_key.FetchMetadata, r.fetchMetadata)

	// make any required hydrate function calls
	// - these populate the row with data entries corresponding to the hydrate function nameSP_LOG=TRACE
	// keep looping round hydrate functions until they are all started

	// make a map of started hydrate calls for this row - this is used the determine which calls have not started yet
	var callsStarted = map[string]bool{}

	for {
		var allStarted = true
		for _, call := range r.queryData.hydrateCalls {
			hydrateFuncName := helpers.GetFunctionName(call.Func)
			if !callsStarted[hydrateFuncName] {
				if call.CanStart(r, hydrateFuncName, r.queryData.concurrencyManager) {
					// execute the hydrate call asynchronously
					call.Start(rowDataCtx, r, hydrateFuncName, r.queryData.concurrencyManager)
					callsStarted[hydrateFuncName] = true
				} else {
					allStarted = false
				}
			}
		}
		if allStarted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	var row *pb.Row

	// wait for all hydrate calls to complete and signal via the wait chan when they are
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
	case <-r.waitChan:
		logging.LogTime("send a row")
		return row, nil
	case err := <-r.errorChan:
		log.Println("[TRACE] hydrate err chan select", "error", err)
		return nil, err
	}
}

// generate the column values for for all requested columns
func (r *RowData) getColumnValues(ctx context.Context) (*pb.Row, error) {
	row := &pb.Row{Columns: make(map[string]*pb.Column)}
	// only populate columns which have been asked for
	for _, columnName := range r.queryData.QueryContext.Columns {
		// get columns schema
		column := r.table.getColumn(columnName)
		if column == nil {
			// postgres asked for a non existent column. Shouldn't happen but just ignore
			continue
		}

		var err error
		row.Columns[columnName], err = r.table.getColumnValue(ctx, r, column)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

// TODO remove GET functionality from callHydrate

// invoke a hydrate function, with syncronisation and error handling
func (r *RowData) callHydrate(ctx context.Context, d *QueryData, hydrateFunc HydrateFunc, hydrateKey string) {
	// handle panics in the row hydrate function
	defer func() {
		if p := recover(); p != nil {
			r.errorChan <- status.Error(codes.Internal, fmt.Sprintf("hydrate call %s failed with panic %v", hydrateKey, p))
		}
		r.wg.Done()
	}()

	logging.LogTime(hydrateKey + " start")

	// now call the hydrate function, passing the item and hydrate results so far
	hydrateData, err := hydrateFunc(ctx, d, &HydrateData{Item: r.Item, HydrateResults: r.hydrateResults})
	if err != nil {
		log.Printf("[ERROR] callHydrate %s finished with error: %v\n", hydrateKey, err)
		r.errorChan <- err
	} else if hydrateData != nil {
		r.set(hydrateKey, hydrateData)
	} else {
		// the the hydrate results to an empty data object
		r.set(hydrateKey, emptyHydrateResults{})
	}

	logging.LogTime(hydrateKey + " end")
}

// invoke a hydrate function, with syncronisation and error handling
func (r *RowData) callGetHydrate(ctx context.Context, d *QueryData, hydrateFunc HydrateFunc, hydrateKey string) (interface{}, error) {
	// handle panics in the row hydrate function
	defer func() {
		if p := recover(); p != nil {
			r.errorChan <- status.Error(codes.Internal, fmt.Sprintf("hydrate call %s failed with panic %v", hydrateKey, p))
		}
		r.wg.Done()
	}()

	logging.LogTime(hydrateKey + " start")

	// now call the hydrate function, passing the item and hydrate results so far
	log.Printf("[TRACE] call hydrate %s\n", hydrateKey)
	hydrateData, err := hydrateFunc(ctx, d, &HydrateData{Item: r.Item, HydrateResults: r.hydrateResults})
	if err != nil {
		log.Printf("[ERROR] callHydrate %s finished with error: %v\n", hydrateKey, err)
		r.errorChan <- err
	} else if hydrateData != nil {
		log.Printf("[TRACE] set hydrate data for %s\n", hydrateKey)
		r.set(hydrateKey, hydrateData)
	}

	logging.LogTime(hydrateKey + " end")
	// NOTE: also return the error - is this is being called by as 'get' call we can act on the error immediately
	return hydrateData, err
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

// GetColumnData :: return the root item, and, if this column has a hydrate function registered, the associated hydrate data
func (r *RowData) GetColumnData(column *Column) (interface{}, error) {

	if column.resolvedHydrateName == "" {
		return nil, fmt.Errorf("colum,n %s has no resolved hydrate function name", column.Name)
	}

	if hydrateItem, ok := r.hydrateResults[column.resolvedHydrateName]; !ok {
		log.Printf("[ERROR] column '%s' requires hydrate data from %s but none is available.\n", column.Name, column.resolvedHydrateName)
		//log.Printf("[TRACE] Hydrate keys:\n")
		//for k := range r.hydrateResults {
		//	log.Printf("[TRACE] %s\n", k)
		//}

		return nil, fmt.Errorf("column '%s' requires hydrate data from %s but none is available", column.Name, column.resolvedHydrateName)
	} else {
		return hydrateItem, nil
	}
}
