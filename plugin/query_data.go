package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	connection_manager "github.com/turbot/steampipe-plugin-sdk/connection"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
)

const itemBufferSize = 100

type QueryData struct {
	// The table this query is associated with
	Table *Table
	// if this is a get call this will be populated with the quals as a map of column name to quals
	//  (this will also be populated for a list call if list key columns are specified -
	//  however this usage is deprecated and provided for legacy reasons only)
	KeyColumnQuals map[string]*proto.QualValue
	// a map of all key column quals which were specified in the query
	Quals KeyColumnQualMap
	// columns which have a single equals qual
	// is this a 'get' or a 'list' call
	FetchType fetchType
	// query context data passed from postgres - this includes the requested columns and the quals
	QueryContext *QueryContext
	// connection details - the connection name and any config declared in the connection config file
	Connection *Connection
	// Matrix is an array of parameter maps (MatrixItems)
	// the list/get calls with be executed for each element of this array
	Matrix []map[string]interface{}
	// object to handle caching of connection specific data
	ConnectionManager *connection_manager.Manager

	// streaming funcs
	StreamListItem func(ctx context.Context, item interface{})
	// deprecated - plugins should no longer call StreamLeafListItem directly and should just call StreamListItem
	// event for the child list of a parent child list call
	StreamLeafListItem func(ctx context.Context, item interface{})
	// internal
	hydrateCalls []*HydrateCall

	concurrencyManager *ConcurrencyManager
	rowDataChan        chan *RowData
	errorChan          chan error
	streamCount        int
	stream             proto.WrapperPlugin_ExecuteServer

	// wait group used to synchronise parent-child list fetches - each child hydrate function increments this wait group
	listWg sync.WaitGroup
	// when executing parent child list calls, we cache the parent list result in the query data passed to the child list call
	parentItem interface{}
}

func newQueryData(queryContext *QueryContext, table *Table, stream proto.WrapperPlugin_ExecuteServer, connection *Connection, matrix []map[string]interface{}, connectionManager *connection_manager.Manager) *QueryData {
	d := &QueryData{
		ConnectionManager: connectionManager,
		Table:             table,
		QueryContext:      queryContext,
		Connection:        connection,
		Matrix:            matrix,
		KeyColumnQuals:    make(map[string]*proto.QualValue),
		Quals:             make(KeyColumnQualMap),

		// asyncronously read items using the 'get' or 'list' API
		// items are streamed on rowDataChan, errors returned on errorChan
		rowDataChan: make(chan *RowData, itemBufferSize),
		errorChan:   make(chan error, 1),
		stream:      stream,
	}
	d.StreamListItem = d.streamListItem
	// for legacy compatibility - plugins should no longer call StreamLeafListItem directly
	d.StreamLeafListItem = d.streamLeafListItem
	d.SetFetchType(table)

	// NOTE: for count(*) queries, there will be no columns - add in 1 column so that we have some data to return
	ensureColumns(queryContext, table)

	d.hydrateCalls = table.requiredHydrateCalls(queryContext.Columns, d.FetchType)
	d.concurrencyManager = newConcurrencyManager(table)

	return d
}

// ShallowCopy creates a shallow copy of the QueryData
// this is used to pass different quals to multiple list/get calls, when an in() clause is specified
func (d *QueryData) ShallowCopy() *QueryData {

	clone := &QueryData{
		Table:              d.Table,
		KeyColumnQuals:     make(map[string]*proto.QualValue),
		Quals:              make(KeyColumnQualMap),
		FetchType:          d.FetchType,
		QueryContext:       d.QueryContext,
		Connection:         d.Connection,
		Matrix:             d.Matrix,
		ConnectionManager:  d.ConnectionManager,
		hydrateCalls:       d.hydrateCalls,
		concurrencyManager: d.concurrencyManager,
		rowDataChan:        d.rowDataChan,
		errorChan:          d.errorChan,
		stream:             d.stream,
		listWg:             d.listWg,
	}

	// NOTE: we create a deep copy of the keyColumnQuals
	// - this is so they can be updated in the copied QueryData without mutating the original
	for k, v := range d.KeyColumnQuals {
		clone.KeyColumnQuals[k] = v
	}
	for k, v := range d.Quals {
		clone.Quals[k] = v
	}

	// NOTE: point the public streaming endpoints to their internal implementations IN THIS OBJECT
	clone.StreamListItem = clone.streamListItem
	clone.StreamLeafListItem = clone.streamLeafListItem
	return clone
}

// SetFetchType determines whether this is a get or a list call, and populates the keyColumnQualValues map
func (d *QueryData) SetFetchType(table *Table) {
	if table.Get != nil {
		// default to get, even before checking the quals
		// this handles the case of a get call only
		d.FetchType = fetchTypeGet

		// build a qual map from Get key columns
		qualMap := NewKeyColumnQualValueMap(d.QueryContext.RawQuals, table.Get.KeyColumns)
		// now see whether the qual map has everything required for the get call
		if satisfied, _ := qualMap.SatisfiesKeyColumns(table.Get.KeyColumns); satisfied {
			d.KeyColumnQuals = qualMap.ToEqualsQualValueMap()
			d.Quals = qualMap
			return
		}
	}

	if table.List != nil {
		// if there is a list config default to list, even is we are missing required quals
		d.FetchType = fetchTypeList
		if len(table.List.KeyColumns) > 0 {
			d.Quals = NewKeyColumnQualValueMap(d.QueryContext.RawQuals, table.List.KeyColumns)
		}
	}
}

// for count(*) queries, there will be no columns - add in 1 column so that we have some data to return
func ensureColumns(queryContext *QueryContext, table *Table) {
	if len(queryContext.Columns) != 0 {
		return
	}

	var col string
	for _, c := range table.Columns {
		if c.Hydrate == nil {
			col = c.Name
			break
		}
	}
	if col == "" {
		// all columns have hydrate - just return the first column
		col = table.Columns[0].Name
	}
	// set queryContext.Columns to be this single column
	queryContext.Columns = []string{col}
}

// stream an item returned from the list call
// wrap in a rowData object
func (d *QueryData) streamListItem(ctx context.Context, item interface{}) {
	callingFunction := helpers.GetCallingFunction(1)

	// if the calling function was the ParentHydrate function from the list config,
	// stream the results to the child list hydrate function and return
	d.streamCount++

	parentListHydrate := d.Table.List.ParentHydrate
	if parentListHydrate == nil {
		d.StreamLeafListItem(ctx, item)
		return
	}

	parentHydrateName := helpers.GetFunctionName(parentListHydrate)
	Logger(ctx).Trace("StreamListItem: called from parent hydrate function - streaming result to child hydrate function",
		"parent hydrate", parentHydrateName,
		"child hydrate", helpers.GetFunctionName(d.Table.List.Hydrate),
		"item", item)
	d.listWg.Add(1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := helpers.ToError(r)
				if !d.verifyCallerIsListCall(callingFunction) {
					err = fmt.Errorf("'streamListItem' must only be called from a list call. Calling function name is '%s'", callingFunction)
					log.Printf("[TRACE] 'streamListItem' failed with panic: %s. Calling function name is '%s'", err, callingFunction)
				}
				d.streamError(err)
			}
		}()
		defer d.listWg.Done()
		// create a copy of query data with the stream function set to streamLeafListItem
		childQueryData := d.ShallowCopy()
		childQueryData.StreamListItem = childQueryData.streamLeafListItem
		// set parent list result so that it can be stored in rowdata hydrate results in streamLeafListItem
		childQueryData.parentItem = item
		if _, err := d.Table.List.Hydrate(ctx, childQueryData, &HydrateData{Item: item}); err != nil {
			d.streamError(err)
		}
	}()
}

func (d *QueryData) verifyCallerIsListCall(callingFunction string) bool {
	if d.Table.List == nil {
		return false
	}
	listFunction := helpers.GetFunctionName(d.Table.List.Hydrate)
	listParentFunction := helpers.GetFunctionName(d.Table.List.ParentHydrate)
	if callingFunction != listFunction && callingFunction != listParentFunction {
		return false
	}
	return true
}

func (d *QueryData) streamLeafListItem(ctx context.Context, item interface{}) {
	// if the context is cancelled, panic to break out
	select {
	case <-d.stream.Context().Done():
		panic(contextCancelledError)
	default:
	}

	// create rowData, passing matrixItem from context
	rd := newRowData(d, item)
	rd.matrixItem = GetMatrixItem(ctx)
	// set the parent item on the row data
	rd.ParentItem = d.parentItem
	// NOTE: add the item as the hydrate data for the list call
	rd.set(helpers.GetFunctionName(d.Table.List.Hydrate), item)
	d.rowDataChan <- rd
}

// called when all items have been fetched - close the item chan
func (d *QueryData) fetchComplete() {
	// wait for any child fetches to complete before closing channel
	d.listWg.Wait()
	close(d.rowDataChan)
}

// read rows from rowChan and stream back
func (d *QueryData) streamRows(_ context.Context, rowChan chan *proto.Row) error {
	for {
		// wait for either an item or an error
		select {
		case err := <-d.errorChan:
			log.Printf("[ERROR] streamRows error chan select: %v\n", err)
			return err
		case row := <-rowChan:
			if row == nil {
				// tell the concurrency manage we are done (it may log the concurrency stats)
				log.Println("[TRACE] row chan closed, stop streaming")
				d.concurrencyManager.Close()
				// channel closed
				return nil
			}
			if err := d.streamRow(row); err != nil {
				return err
			}
		}
	}
}

func (d *QueryData) streamRow(row *proto.Row) error {
	return d.stream.Send(&proto.ExecuteResponse{Row: row})
}

func (d *QueryData) streamError(err error) {
	d.errorChan <- err
}

// iterate over rowDataChan, for each item build the row and stream over rowChan
func (d *QueryData) buildRows(ctx context.Context) chan *proto.Row {
	const rowBufferSize = 10

	// stream data for each item
	var rowChan = make(chan *proto.Row, rowBufferSize)
	// we need to use a wait group for rows we cannot close the row channel when the item channel is closed
	// as getRow is executing asyncronously
	var rowWg sync.WaitGroup

	// start goroutine to read items from item chan and generate row data
	go func() {
		for {
			// wait for either an rowData or an error
			select {
			case err := <-d.errorChan:
				log.Printf("[ERROR] error chan select: %v\n", err)
				// put it back in the channel and return
				d.errorChan <- err
				return
			case rowData := <-d.rowDataChan:
				// is channel closed?
				if rowData == nil {
					log.Println("[TRACE] rowData chan select - channel CLOSED")
					// now we know there will be no more items, start goroutine to close row chan when the wait group is complete
					// this allows time for all hydrate goroutines to complete
					go d.waitForRowsToComplete(&rowWg, rowChan)
					// rowData channel closed - nothing more to do
					return
				}
				logging.LogTime("got rowData - calling getRow")
				rowWg.Add(1)
				go d.buildRow(ctx, rowData, rowChan, &rowWg)
			}
		}
	}()

	return rowChan
}

// execute necessary hydrate calls to populate row data
func (d *QueryData) buildRow(ctx context.Context, rowData *RowData, rowChan chan *proto.Row, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			d.streamError(ToError(r))
		}
		wg.Done()
	}()

	// delegate the work to a row object
	row, err := rowData.getRow(ctx)
	if err != nil {
		log.Printf("[WARN] getRow failed with error %v", err)
		d.streamError(err)
	} else {
		rowChan <- row
	}
}

func (d *QueryData) waitForRowsToComplete(rowWg *sync.WaitGroup, rowChan chan *proto.Row) {
	log.Println("[TRACE] wait for rows")
	rowWg.Wait()
	logging.DisplayProfileData(10 * time.Millisecond)
	log.Println("[TRACE] rowWg complete - CLOSING ROW CHANNEL")
	close(rowChan)
}

// ToError is used to return an error or format the supplied value as error.
// Can be removed once go-kit version 0.2.0 is released
func ToError(val interface{}) error {
	if e, ok := val.(error); ok {
		return e
	} else {
		return fmt.Errorf("%v", val)
	}
}
