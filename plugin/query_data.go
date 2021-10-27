package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	typehelpers "github.com/turbot/go-kit/types"
	connection_manager "github.com/turbot/steampipe-plugin-sdk/connection"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-plugin-sdk/plugin/quals"
)

const itemBufferSize = 100

// NOTE - any field added here must also be added to ShallowCopy

type QueryData struct {
	// The table this query is associated with
	Table *Table
	// if this is a get call this will be populated with the quals as a map of column name to quals
	//  (this will also be populated for a list call if list key columns are specified -
	//  however this usage is deprecated and provided for legacy reasons only)
	KeyColumnQuals KeyColumnEqualsQualMap
	// a map of all key column quals which were specified in the query
	Quals KeyColumnQualMap
	// columns which have a single equals qual
	// is this a 'get' or a 'list' call
	FetchType fetchType
	// query context data passed from postgres - this includes the requested columns and the quals
	QueryContext *QueryContext
	// the status of the in-progress query
	QueryStatus *QueryStatus
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

	// a list of the required hydrate calls (EXCLUDING the fetch call)
	hydrateCalls []*HydrateCall
	// all the columns that will be returned by this query
	columns []string

	concurrencyManager *ConcurrencyManager
	rowDataChan        chan *RowData
	errorChan          chan error

	stream proto.WrapperPlugin_ExecuteServer
	// wait group used to synchronise parent-child list fetches - each child hydrate function increments this wait group
	listWg *sync.WaitGroup
	// when executing parent child list calls, we cache the parent list result in the query data passed to the child list call
	parentItem     interface{}
	filteredMatrix []map[string]interface{}
}

func newQueryData(queryContext *QueryContext, table *Table, stream proto.WrapperPlugin_ExecuteServer, connection *Connection, matrix []map[string]interface{}, connectionManager *connection_manager.Manager) *QueryData {
	var wg sync.WaitGroup
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
		listWg:      &wg,
	}
	d.StreamListItem = d.streamListItem
	// for legacy compatibility - plugins should no longer call StreamLeafListItem directly
	d.StreamLeafListItem = d.streamLeafListItem
	d.setFetchType(table)
	// if we have key column quals for any matrix properties, filter the matrix
	// to exclude items which do not satisfy the quals
	// this populates the property filteredMatrix
	d.filterMatrixItems()

	// NOTE: for count(*) queries, there will be no columns - add in 1 column so that we have some data to return
	ensureColumns(queryContext, table)

	// build list of required hydrate calls, based on requested columns
	d.hydrateCalls = table.requiredHydrateCalls(queryContext.Columns, d.FetchType)
	// build list of all columns returned by these hydrate calls (and the fetch call)
	d.populateColumns()
	d.concurrencyManager = newConcurrencyManager(table)

	// populate the query status
	// if a limit is set, use this to set rows required - otherwise just set to MaxInt32
	d.QueryStatus = newQueryStatus(d.QueryContext.Limit)

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
		ConnectionManager:  d.ConnectionManager,
		Matrix:             d.Matrix,
		filteredMatrix:     d.filteredMatrix,
		hydrateCalls:       d.hydrateCalls,
		concurrencyManager: d.concurrencyManager,
		rowDataChan:        d.rowDataChan,
		errorChan:          d.errorChan,
		stream:             d.stream,
		listWg:             d.listWg,
		columns:            d.columns,
	}

	clone.QueryStatus = &QueryStatus{
		rowsRequired: d.QueryStatus.rowsRequired,
		rowsStreamed: d.QueryStatus.rowsStreamed,
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

// build list of all columns returned by the fetch call and required hydrate calls
func (d *QueryData) populateColumns() {
	// add columns returned by fetch call
	fetchName := helpers.GetFunctionName(d.Table.getFetchFunc(d.FetchType))
	d.columns = append(d.columns, d.addColumnsForHydrate(fetchName)...)

	// add columns returned by required hydrate calls
	for _, h := range d.hydrateCalls {
		d.columns = append(d.columns, d.addColumnsForHydrate(h.Name)...)
	}
}

// get the column returned by the given hydrate call
func (d *QueryData) addColumnsForHydrate(hydrateName string) []string {
	var cols []string
	for _, columnName := range d.Table.hydrateColumnMap[hydrateName] {
		cols = append(cols, columnName)
	}
	return cols
}

// KeyColumnQualString looks for the specified key column quals and if it exists, return the value as a string
func (d *QueryData) KeyColumnQualString(key string) string {
	qualValue, ok := d.KeyColumnQuals[key]
	if !ok {
		return ""
	}
	return typehelpers.ToString(grpc.GetQualValue(qualValue).(string))
}

// add matrix item into KeyColumnQuals and Quals
func (d *QueryData) updateQualsWithMatrixItem(matrixItem map[string]interface{}) {
	for col, value := range matrixItem {
		qualValue := proto.NewQualValue(value)
		// replace any existing entry for both Quals and KeyColumnQuals
		d.KeyColumnQuals[col] = qualValue
		d.Quals[col] = &KeyColumnQuals{Name: col, Quals: []*quals.Qual{{Column: col, Value: qualValue}}}
	}
}

// setFetchType determines whether this is a get or a list call, and populates the keyColumnQualValues map
func (d *QueryData) setFetchType(table *Table) {
	log.Printf("[TRACE] setFetchType")
	if table.Get != nil {
		// default to get, even before checking the quals
		// this handles the case of a get call only
		d.FetchType = fetchTypeGet

		// build a qual map from Get key columns
		qualMap := NewKeyColumnQualValueMap(d.QueryContext.UnsafeQuals, table.Get.KeyColumns)
		// now see whether the qual map has everything required for the get call
		if unsatisfiedColumns := qualMap.GetUnsatisfiedKeyColumns(table.Get.KeyColumns); len(unsatisfiedColumns) == 0 {
			log.Printf("[TRACE] Set fetchType to fetchTypeGet")
			d.KeyColumnQuals = qualMap.ToEqualsQualValueMap()
			d.Quals = qualMap
			d.logQualMaps()
			return
		}
	}

	if table.List != nil {
		log.Printf("[TRACE] Set fetchType to fetchTypeList")
		// if there is a list config default to list, even is we are missing required quals
		d.FetchType = fetchTypeList
		if len(table.List.KeyColumns) > 0 {
			// build a qual map from List key columns
			qualMap := NewKeyColumnQualValueMap(d.QueryContext.UnsafeQuals, table.List.KeyColumns)
			// assign to the map of all key column quals
			d.Quals = qualMap
			// convert to a map of equals quals to populate legacy `KeyColumnQuals` map
			d.KeyColumnQuals = d.Quals.ToEqualsQualValueMap()
		}
		d.logQualMaps()
	}
}

func (d *QueryData) filterMatrixItems() {
	if len(d.Matrix) == 0 {
		return
	}
	log.Printf("[TRACE] filterMatrixItems - there are %d matrix items", len(d.Matrix))
	log.Printf("[TRACE] unfiltered matrix: %v", d.Matrix)
	var filteredMatrix []map[string]interface{}

	// build a keycolumn slice from the matrix items
	var matrixKeyColumns KeyColumnSlice
	for column := range d.Matrix[0] {
		matrixKeyColumns = append(matrixKeyColumns, &KeyColumn{
			Name:      column,
			Operators: []string{"="},
		})
	}
	// now see which of these key columns are satisfied by the provided quals
	matrixQualMap := NewKeyColumnQualValueMap(d.QueryContext.UnsafeQuals, matrixKeyColumns)

	for _, m := range d.Matrix {
		log.Printf("[TRACE] matrix item %v", m)
		// do all key columns which exist for this matrix item match the matrix values?
		includeMatrixItem := true

		for col, val := range m {
			log.Printf("[TRACE] col %s val %s", col, val)
			// is there a quals for this matrix column?

			if quals, ok := matrixQualMap[col]; ok {
				log.Printf("[TRACE] quals found for matrix column: %v", quals)
				// if there IS a single equals qual which DOES NOT match this matrix item, exclude the matrix item
				if quals.SingleEqualsQual() {
					includeMatrixItem = d.shouldIncludeMatrixItem(quals, val)
				}
			} else {
				log.Printf("[TRACE] quals found for matrix column: %s", col)
			}
		}

		if includeMatrixItem {
			log.Printf("[TRACE] INCLUDE matrix item")
			filteredMatrix = append(filteredMatrix, m)
		} else {
			log.Printf("[TRACE] EXCLUDE matrix item")
		}
	}
	d.filteredMatrix = filteredMatrix
	log.Printf("[TRACE] filtered matrix: %v", d.Matrix)

}

func (d *QueryData) shouldIncludeMatrixItem(quals *KeyColumnQuals, matrixVal interface{}) bool {
	log.Printf("[TRACE] there is a single equals qual")

	// if the value is an array, this is an IN query - check whether the array contains the matrix value
	if listValue := quals.Quals[0].Value.GetListValue(); listValue != nil {
		log.Printf("[TRACE] the qual value is a list: %v", listValue)
		for _, qv := range listValue.Values {
			if grpc.GetQualValue(qv) == matrixVal {
				log.Printf("[TRACE] qual value list contains matrix value %v", matrixVal)
				return true
			}
		}
		return false
	}

	// otherwise it is a single qual value
	return grpc.GetQualValue(quals.Quals[0].Value) == matrixVal
}

func (d *QueryData) logQualMaps() {
	log.Printf("[TRACE] Equals key column quals:\n%s", d.KeyColumnQuals)
	log.Printf("[TRACE] All key column quals:\n%s", d.Quals)
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

func (d *QueryData) verifyCallerIsListCall(callingFunction string) bool {
	if d.Table.List == nil {
		return false
	}
	listFunction := helpers.GetFunctionName(d.Table.List.Hydrate)
	listParentFunction := helpers.GetFunctionName(d.Table.List.ParentHydrate)
	if callingFunction != listFunction && callingFunction != listParentFunction {
		// if the calling function is NOT one of the other registered hydrate functions,
		//it must be an anonymous function so let it go
		for _, c := range d.Table.Columns {
			if c.Hydrate != nil && helpers.GetFunctionName(c.Hydrate) == callingFunction {
				return false
			}
		}
	}
	return true
}

// stream an item returned from the list call
// wrap in a rowData object
func (d *QueryData) streamListItem(ctx context.Context, item interface{}) {
	callingFunction := helpers.GetCallingFunction(1)

	// do a deep nil check on item - if nil, just return
	if helpers.IsNil(item) {
		return
	}

	// if this table has no parent hydrate function, just call steramLeafListItem directly
	parentListHydrate := d.Table.List.ParentHydrate
	if parentListHydrate == nil {
		d.streamLeafListItem(ctx, item)
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

func (d *QueryData) streamLeafListItem(ctx context.Context, item interface{}) {
	// do a deep nil check on item - if nil, just return
	if helpers.IsNil(item) {
		return
	}
	// have we streamed enough already?
	if d.QueryStatus.RowsRemaining(ctx) == 0 {
		log.Printf("[TRACE] d.QueryStatus.RowsRemaining is 0 - streamLeafListItem NOT streaming item")
		return
	}
	// increment the stream count
	d.QueryStatus.rowsStreamed++

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
func (d *QueryData) streamRows(_ context.Context, rowChan chan *proto.Row) ([]*proto.Row, error) {
	var rows []*proto.Row
	for {
		// wait for either an item or an error
		select {
		case err := <-d.errorChan:
			log.Printf("[ERROR] streamRows error chan select: %v\n", err)
			d.streamError(err)
			d.concurrencyManager.Close()
			// channel closed return what we have sent
			return rows, nil
		case row := <-rowChan:
			if row == nil {
				// tell the concurrency manage we are done (it may log the concurrency stats)
				log.Println("[TRACE] row chan closed, stop streaming")
				d.concurrencyManager.Close()
				// channel closed return what we have sent
				return rows, nil
			}
			if err := d.streamRow(row); err != nil {
				return nil, err
			}
			rows = append(rows, row)
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
			d.streamError(helpers.ToError(r))
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
