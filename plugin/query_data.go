package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v5/error_helpers"

	"github.com/turbot/go-kit/helpers"
	typehelpers "github.com/turbot/go-kit/types"
	connection_manager "github.com/turbot/steampipe-plugin-sdk/v5/connection"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
)

// how may rows do we cache in the rowdata channel
const rowDataBufferSize = 100

/*
QueryData is passed to all [HydrateFunc] calls. It contains all required information about the executing query:

  - The table ([Table]).

  - A map of all equals key column quals ([KeyColumnEqualsQualMap]).

  - A map of all key column quals ([KeyColumnQualMap]). See [key_columns].

  - Is it a list or a get call?

  - Context data passed from postgres ([QueryContext]).

  - The steampipe connection ([Connection]).

  - A cache which can be used to store connection specific data ([connection.ConnectionCache]).

  - The function which is used to stream rows of data ([connection.StreamListItem]).

Plugin examples:
  - [hackernews]
  - [pagerduty]

[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/item.go#L52
[pagerduty]: https://github.com/turbot/steampipe-plugin-pagerduty/blob/5c04d5d6636b039277285e0c3f6c07069510e267/pagerduty/table_pagerduty_user.go#L146
*/
type QueryData struct {
	// NOTE - any field added here must also be added to ShallowCopy

	// The table this query is associated with
	Table *Table

	// a map of key columns which have a _single equals qual_
	// this is intended for use by Get calls only
	// key: column name, value: QualValue
	//  (this will also be populated for a list call if list key columns are specified -
	//  however this usage is deprecated and provided for legacy reasons only)
	EqualsQuals KeyColumnEqualsQualMap
	// a map of all KeyColumnQuals which were specified in the query. ([key_columns]
	Quals KeyColumnQualMap
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
	// deprecated
	// use ConnectionCache
	ConnectionManager *connection_manager.Manager
	ConnectionCache   *connection_manager.ConnectionCache

	// streaming funcs
	StreamListItem func(context.Context, ...interface{})

	// deprecated - plugins should no longer call StreamLeafListItem directly and should just call StreamListItem
	// event for the child list of a parent child list call
	StreamLeafListItem func(context.Context, ...interface{})

	// internal
	// the status of the in-progress query
	queryStatus *queryStatus
	// the callId for this connection
	connectionCallId string
	plugin           *Plugin
	// a list of the required hydrate calls (EXCLUDING the fetch call)
	hydrateCalls []*hydrateCall

	// all the columns that will be returned by this query
	columns            map[string]*QueryColumn
	concurrencyManager *concurrencyManager
	rowDataChan        chan *rowData
	errorChan          chan error
	// channel to send results
	outputChan chan *proto.ExecuteResponse
	// wait group used to synchronise parent-child list fetches - each child hydrate function increments this wait group
	listWg *sync.WaitGroup
	// when executing parent child list calls, we cache the parent list result in the query data passed to the child list call
	parentItem     interface{}
	filteredMatrix []map[string]interface{}
	// column quals which were used to filter the matrix
	filteredMatrixColumns []string

	// ttl for the execute call
	cacheTtl int64

	cacheEnabled bool
	// if data is being cached, this will contain the id used to send rows to the cache
	cacheResultKey string
	// the names of all the columns which are actually being returned
	cacheColumns []string
	// buffer rows before sending to the cache in chunks
	cacheRows []*proto.Row

	// map of hydrate function name to columns it provides
	// (this is in queryData not Table as it gets modified per query)
	hydrateColumnMap map[string][]string
	// tactical - free memory after streaming this many rows
	freeMemInterval int64

	// temp dir for the connection
	tempDir string
}

func newQueryData(connectionCallId string, p *Plugin, queryContext *QueryContext, table *Table, connectionData *ConnectionData, executeData *proto.ExecuteConnectionData, outputChan chan *proto.ExecuteResponse) (*QueryData, error) {
	var wg sync.WaitGroup

	// get or create a connection cache as necessary
	connectionCache := p.ensureConnectionCache(connectionData.Connection.Name)
	d := &QueryData{
		// set deprecated ConnectionManager
		ConnectionManager: connection_manager.NewManager(connectionCache),
		ConnectionCache:   connectionCache,
		Table:             table,
		QueryContext:      queryContext,
		Connection:        connectionData.Connection,
		EqualsQuals:       make(map[string]*proto.QualValue),
		Quals:             make(KeyColumnQualMap),
		plugin:            p,
		connectionCallId:  connectionCallId,
		cacheTtl:          executeData.CacheTtl,
		cacheEnabled:      executeData.CacheEnabled,
		columns:           make(map[string]*QueryColumn),

		// asyncronously read items using the 'get' or 'list' API
		// items are streamed on rowDataChan, errors returned on errorChan
		rowDataChan: make(chan *rowData, rowDataBufferSize),
		errorChan:   make(chan error, 1),
		outputChan:  outputChan,
		listWg:      &wg,

		freeMemInterval: GetFreeMemInterval(),

		// temporary dir for this connection
		// this will only created if GetSourceFiles is used
		tempDir: connectionData.GetConnectionTempDir(p.tempDir),
	}

	d.StreamListItem = d.streamListItem
	// for legacy compatibility - plugins should no longer call StreamLeafListItem directly
	d.StreamLeafListItem = d.streamLeafListItem
	d.setFetchType(table)

	queryContext.ensureColumns(table)

	// build list of required hydrate calls, based on requested columns
	d.populateRequiredHydrateCalls()
	// build list of all columns returned by these hydrate calls (and the fetch call)
	d.populateColumns()
	d.concurrencyManager = newConcurrencyManager(table)
	// populate the query status
	// if a limit is set, use this to set rows required - otherwise just set to MaxInt32
	d.queryStatus = newQueryStatus(d.QueryContext.Limit)

	return d, nil
}

// ShallowCopy creates a shallow copy of the QueryData, i.e. most pointer properties are copied
// this is used to pass different quals to multiple list/get calls, when an 'in' clause is specified
func (d *QueryData) ShallowCopy() *QueryData {
	clone := &QueryData{
		Table:             d.Table,
		EqualsQuals:       make(map[string]*proto.QualValue),
		Quals:             make(KeyColumnQualMap),
		FetchType:         d.FetchType,
		QueryContext:      d.QueryContext,
		Connection:        d.Connection,
		ConnectionManager: d.ConnectionManager,
		ConnectionCache:   d.ConnectionCache,
		Matrix:            d.Matrix,
		plugin:            d.plugin,
		cacheTtl:          d.cacheTtl,
		cacheEnabled:      d.cacheEnabled,

		filteredMatrix:     d.filteredMatrix,
		hydrateCalls:       d.hydrateCalls,
		concurrencyManager: d.concurrencyManager,
		rowDataChan:        d.rowDataChan,
		errorChan:          d.errorChan,
		outputChan:         d.outputChan,
		listWg:             d.listWg,
		columns:            d.columns,
		queryStatus:        d.queryStatus,
	}

	// NOTE: we create a deep copy of the keyColumnQuals
	// - this is so they can be updated in the copied QueryData without mutating the original
	for k, v := range d.EqualsQuals {
		clone.EqualsQuals[k] = v
	}
	for k, v := range d.Quals {
		clone.Quals[k] = v
	}

	// NOTE: point the public streaming endpoints to their internal implementations IN THIS OBJECT
	clone.StreamListItem = clone.streamListItem
	clone.StreamLeafListItem = clone.streamLeafListItem
	return clone
}

// RowsRemaining returns how many rows are required to complete the query
//   - if no limit has been parsed from the query, this will return math.MaxInt32
//     (meaning an unknown number of rows remain)
//   - if there is a limit, it will return the number of rows required to reach this limit
//   - if  the context has been cancelled, it will return zero
func (d *QueryData) RowsRemaining(ctx context.Context) int64 {
	if IsCancelled(ctx) {
		return 0
	}
	rowsRemaining := d.queryStatus.rowsRequired - d.queryStatus.rowsStreamed
	return rowsRemaining
}

// EqualsQualString looks for the specified key column quals and if it exists, return the value as a string
func (d *QueryData) EqualsQualString(key string) string {
	qualValue, ok := d.EqualsQuals[key]
	if !ok {
		return ""
	}
	return typehelpers.ToString(grpc.GetQualValue(qualValue).(string))
}

func (d *QueryData) setMatrixItem(matrix []map[string]interface{}) {
	d.Matrix = matrix
	// if we have key column quals for any matrix properties, filter the matrix
	// to exclude items which do not satisfy the quals
	// this populates the property filteredMatrix
	d.filterMatrixItems()
}

// build a list of required hydrate function calls which must be executed, based on the columns which have been requested
// NOTE: 'get' and 'list' calls are hydration functions, but they must be omitted from this list as they are called
// first. BEFORE the other hydration functions
// NOTE2: this function also populates the resolvedHydrateName for each column (used to retrieve column values),
// and the hydrateColumnMap (used to determine which columns to return)
func (d *QueryData) populateRequiredHydrateCalls() {
	t := d.Table
	colsUsed := d.QueryContext.Columns
	fetchType := d.FetchType

	// what is the name of the fetch call (i.e. the get/list call)
	fetchFunc := t.getFetchFunc(fetchType)
	fetchCallName := helpers.GetFunctionName(fetchFunc)

	// initialise hydrateColumnMap
	d.hydrateColumnMap = make(map[string][]string)
	requiredCallBuilder := newRequiredHydrateCallBuilder(t, fetchCallName)

	// populate a map keyed by function name to ensure we only store each hydrate function once
	for _, column := range t.Columns {
		// see if this column specifies a hydrate function

		var hydrateName string
		if hydrateFunc := column.Hydrate; hydrateFunc == nil {
			// so there is NO hydrate call registered for the column
			// the column is provided by the fetch call
			// do not add to map of hydrate functions as the fetch call will always be called
			hydrateFunc = fetchFunc
			hydrateName = fetchCallName
		} else {
			// there is a hydrate call registered
			hydrateName = helpers.GetFunctionName(hydrateFunc)
			// if this column was requested in query, add the hydrate call to required calls
			if helpers.StringSliceContains(colsUsed, column.Name) {
				requiredCallBuilder.Add(hydrateFunc)
			}
		}

		// now update hydrateColumnMap
		d.hydrateColumnMap[hydrateName] = append(d.hydrateColumnMap[hydrateName], column.Name)
	}
	d.hydrateCalls = requiredCallBuilder.Get()

	// now we have all the hydrate calls, build a list of all the columns that will be returned by the hydrate functions.
	// these will be used for the cache

}

// build list of all columns returned by the fetch call and required hydrate calls
func (d *QueryData) populateColumns() {
	// add columns returned by fetch call
	fetchName := helpers.GetFunctionName(d.Table.getFetchFunc(d.FetchType))
	d.addColumnsForHydrate(fetchName)

	// add columns returned by required hydrate calls
	for _, h := range d.hydrateCalls {
		d.addColumnsForHydrate(h.Name)
	}
}

// get the column returned by the given hydrate call
func (d *QueryData) addColumnsForHydrate(hydrateName string) {
	for _, columnName := range d.hydrateColumnMap[hydrateName] {
		// get the column from the table
		column := d.Table.getColumn(columnName)
		d.columns[columnName] = NewQueryColumn(column, hydrateName)
	}
}

// add matrix item into KeyColumnQuals and Quals
func (d *QueryData) updateQualsWithMatrixItem(matrixItem map[string]interface{}) {
	for col, value := range matrixItem {
		qualValue := proto.NewQualValue(value)
		// replace any existing entry for both Quals and KeyColumnQuals
		d.EqualsQuals[col] = qualValue
		d.Quals[col] = &KeyColumnQuals{Name: col, Quals: []*quals.Qual{{Column: col, Value: qualValue}}}
	}
}

// setFetchType determines whether this is a get or a list call, and populates the keyColumnQualValues map
func (d *QueryData) setFetchType(table *Table) {
	log.Printf("[TRACE] setFetchType %v", d.QueryContext.UnsafeQuals)
	if table.Get != nil {
		// default to get, even before checking the quals
		// this handles the case of a get call only
		d.FetchType = fetchTypeGet

		// build a qual map from whichever quals match the Get key columns
		qualMap := NewKeyColumnQualValueMap(d.QueryContext.UnsafeQuals, table.Get.KeyColumns)
		// now see whether this qual map has everything required for the get call
		if unsatisfiedColumns := qualMap.GetUnsatisfiedKeyColumns(table.Get.KeyColumns); len(unsatisfiedColumns) == 0 {
			// so this IS a get call - all quals are satisfied
			log.Printf("[TRACE] Set fetchType to fetchTypeGet")
			d.EqualsQuals = qualMap.ToEqualsQualValueMap()
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
			d.EqualsQuals = d.Quals.ToEqualsQualValueMap()
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

			if matrixQuals, ok := matrixQualMap[col]; ok {
				log.Printf("[TRACE] quals found for matrix column: %v", matrixQuals)
				// if there IS a single equals qual which DOES NOT match this matrix item, exclude the matrix item
				if matrixQuals.SingleEqualsQual() {
					includeMatrixItem = d.shouldIncludeMatrixItem(matrixQuals, val)
					// store this column - we will need this when building a cache key
					if !includeMatrixItem {
						d.filteredMatrixColumns = append(d.filteredMatrixColumns, col)
					}
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
	log.Printf("[TRACE] Equals key column quals:\n%s", d.EqualsQuals)
	log.Printf("[TRACE] All key column quals:\n%s", d.Quals)
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
func (d *QueryData) streamListItem(ctx context.Context, items ...interface{}) {
	// loop over items
	for _, item := range items {
		// have we streamed enough already?
		if d.queryStatus.StreamingComplete {
			return
		}
		// if this table has no parent hydrate function, just call streamLeafListItem directly
		if d.Table.List.ParentHydrate != nil {
			// so there is a parent-child hydrate - call the child hydrate, passing 'item' as the parent item
			d.callChildListHydrate(ctx, item)
		} else {
			// otherwise call streamLeafListItem directly
			d.streamLeafListItem(ctx, item)
		}
	}
}

// there is a parent-child list hydration - call the child list function passing 'item' as the parent item'
func (d *QueryData) callChildListHydrate(ctx context.Context, parentItem interface{}) {
	// do a deep nil check on item - if nil, just return to skip this item
	if helpers.IsNil(parentItem) {
		return
	}
	callingFunction := helpers.GetCallingFunction(1)
	d.listWg.Add(1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := helpers.ToError(r)
				if !d.verifyCallerIsListCall(callingFunction) {
					err = fmt.Errorf("'streamListItem' must only be called from a list call. Calling function name is '%s'", callingFunction)
					log.Printf("[WARN] 'streamListItem' failed with panic: %s. Calling function name is '%s'", err, callingFunction)
				}
				d.streamError(err)
			}
		}()
		defer d.listWg.Done()
		// create a copy of query data with the stream function set to streamLeafListItem
		childQueryData := d.ShallowCopy()
		childQueryData.StreamListItem = childQueryData.streamLeafListItem
		// set parent list result so that it can be stored in rowdata hydrate results in streamLeafListItem
		childQueryData.parentItem = parentItem
		// now call the parent list
		_, err := d.Table.List.Hydrate(ctx, childQueryData, &HydrateData{Item: parentItem})
		if err != nil {
			d.streamError(err)
		}
	}()
}

func (d *QueryData) streamLeafListItem(ctx context.Context, items ...interface{}) {
	// loop over items
	for _, item := range items {
		// have we streamed enough already?
		if d.queryStatus.StreamingComplete {
			return
		}

		// if this is the first time we have received a zero rows remaining, stream an empty row and mark stream
		if d.queryStatus.RowsRemaining(ctx) == 0 {
			log.Printf("[TRACE] streamListItem RowsRemaining zero, send nil row %s", d.Connection.Name)
			d.queryStatus.StreamingComplete = true
			// if this is the first time we have received a zero rows remaining, stream an empty row
			// to indicate downstream that we are done
			d.rowDataChan <- nil
			// we are done - give memory back to OS at once
			debug.FreeOSMemory()
			return
		}

		// tactical - if we have specified freeMemInterval, check whether we have reached it and need to free memory
		// this is to reduce memory pressure dure to streaming high row counts
		if d.shouldFreeMemory() {
			debug.FreeOSMemory()
		}

		// do a deep nil check on item - if nil, just skipthis item
		if helpers.IsNil(item) {
			log.Printf("[TRACE] streamLeafListItem received nil item, skipping")
			continue
		}
		// increment the stream count
		d.queryStatus.rowsStreamed++

		// create rowData, passing matrixItem from context
		rd := newRowData(d, item)

		rd.matrixItem = GetMatrixItem(ctx)
		// set the parent item on the row data
		rd.parentItem = d.parentItem
		// NOTE: add the item as the hydrate data for the list call
		rd.set(helpers.GetFunctionName(d.Table.List.Hydrate), item)

		d.rowDataChan <- rd
	}
}

// if a free memory interval has been set, check if we have reached it
func (d *QueryData) shouldFreeMemory() bool {
	return d.freeMemInterval != 0 && d.queryStatus.rowsStreamed%d.freeMemInterval == 0
}

// called when all items have been fetched - close the item chan
func (d *QueryData) fetchComplete(ctx context.Context) {
	log.Printf("[TRACE] QueryData.fetchComplete")

	// wait for any child fetches to complete before closing channel
	d.listWg.Wait()
	close(d.rowDataChan)
}

// iterate over rowDataChan, for each item build the row and stream over rowChan
func (d *QueryData) buildRowsAsync(ctx context.Context, rowChan chan *proto.Row, doneChan chan bool) {
	// we need to use a wait group for rows we cannot close the row channel when the item channel is closed
	// as getRow is executing asyncronously
	var rowWg sync.WaitGroup

	// start goroutine to read items from item chan and generate row data
	go func() {
		for {
			// wait for either an rowData or an error
			select {
			case <-doneChan:
				log.Printf("[TRACE] buildRowsAsync done channel selected - quitting %s", d.Connection.Name)
				return
			case rowData := <-d.rowDataChan:
				logging.LogTime("got rowData - calling getRow")
				// is there any more data?
				if rowData == nil {
					log.Printf("[TRACE] rowData chan returned nil - wait for rows to complete (%s)", d.connectionCallId)
					// now we know there will be no more items, close row chan when the wait group is complete
					// this allows time for all hydrate goroutines to complete
					d.waitForRowsToComplete(&rowWg, rowChan)
					log.Printf("[TRACE] buildRowsAsync goroutine returning (%s)", d.connectionCallId)
					// rowData channel closed - nothing more to do
					return
				}

				rowWg.Add(1)
				d.buildRowAsync(ctx, rowData, rowChan, &rowWg)
			}
		}
	}()
}

// read rows from rowChan and stream back across GRPC
// (also return the rows so we can cache them when complete)
func (d *QueryData) streamRows(ctx context.Context, rowChan chan *proto.Row, doneChan chan bool) (err error) {
	ctx, span := telemetry.StartSpan(ctx, d.Table.Plugin.Name, "QueryData.streamRows (%s)", d.Table.Name)
	defer span.End()

	log.Printf("[TRACE] QueryData streamRows (%s)", d.connectionCallId)

	defer func() {
		// tell the concurrency manage we are done (it may log the concurrency stats)
		d.concurrencyManager.Close()
		log.Printf("[TRACE] QueryData streamRows DONE (%s)", d.connectionCallId)

		// if there is an error or cancellation, abort the pending set
		// if the context is cancelled and the parent callId is in the list of completed executions,
		// this means Postgres has called EndForeignScan as it has enough data, and the context has been cancelled
		// call EndSet
		if err == nil {
			// use the context error instead
			err = ctx.Err()
		}
		if err != nil {
			if error_helpers.IsContextCancelledError(err) {
				log.Printf("[TRACE] streamRows for %s - execution has been cancelled - calling queryCache.AbortSet", d.connectionCallId)
			} else {
				log.Printf("[WARN] streamRows for %s - execution has failed (%s) - calling queryCache.AbortSet", d.connectionCallId, err.Error())
			}
			d.plugin.queryCache.AbortSet(ctx, d.connectionCallId, err)
		} else {
			// if we are caching call EndSet to write to the cache
			if d.cacheEnabled {
				cacheErr := d.plugin.queryCache.EndSet(ctx, d.connectionCallId)
				if cacheErr != nil {
					// just log error, do not fail
					log.Printf("[WARN] cache set failed: %v", cacheErr)
				} else {
					log.Printf("[TRACE] cache set succeeded")
				}
			}
		}
	}()

	for {
		// wait for either an item or an error
		select {
		case err := <-d.errorChan:
			log.Printf("[TRACE] streamRows error chan select: %v", err)
			log.Printf("[TRACE] aborting cache set operation")

			// close done chan - this will cancel buildRowsAsync
			close(doneChan)
			// return what we have sent
			return err
		case row := <-rowChan:
			//log.Printf("[WARN] got row")

			// stream row (even if it is nil)
			//d.streamRow(row)

			// nil row means we are done streaming
			if row == nil {
				log.Printf("[TRACE] streamRows - nil row, stop streaming (%s)", d.connectionCallId)
				return nil
			}
			// if we are caching stream this row to the cache as well
			if d.cacheEnabled {
				d.plugin.queryCache.IterateSet(ctx, row, d.connectionCallId)
			}

			// stream row
			d.streamRow(row)
		}
	}

}

func (d *QueryData) streamRow(row *proto.Row) {
	resp := &proto.ExecuteResponse{
		Row: row,
		Metadata: &proto.QueryMetadata{
			HydrateCalls: d.queryStatus.hydrateCalls,
			// only 1 of these will be non zero
			RowsFetched: d.queryStatus.rowsStreamed + d.queryStatus.cachedRowsFetched,
			CacheHit:    d.queryStatus.cachedRowsFetched > 0,
		},
		Connection: d.Connection.Name,
	}
	d.outputChan <- resp
}

func (d *QueryData) streamError(err error) {
	log.Printf("[WARN] QueryData StreamError %v (%s)", err, d.connectionCallId)
	d.errorChan <- err
}

// execute necessary hydrate calls to populate row data
func (d *QueryData) buildRowAsync(ctx context.Context, rowData *rowData, rowChan chan *proto.Row, wg *sync.WaitGroup) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				d.streamError(helpers.ToError(r))
			}
			wg.Done()
		}()
		if rowData == nil {
			log.Printf("[TRACE] buildRowAsync nil rowData - streaming nil row (%s)", d.connectionCallId)
			rowChan <- nil
			return
		}

		// delegate the work to a row object
		row, err := rowData.getRow(ctx)
		if err != nil {
			log.Printf("[WARN] getRow failed with error %v", err)
			d.streamError(err)
		} else {
			// NOTE: add the Steampipecontext data to the row
			d.addContextData(row)

			rowChan <- row
		}
	}()
}

func (d *QueryData) addContextData(row *proto.Row) {
	jsonValue, _ := json.Marshal(map[string]string{"connection_name": d.Connection.Name})
	contextColumnName := contextColumnName(d.Table.columnNameMap())
	row.Columns[contextColumnName] = &proto.Column{Value: &proto.Column_JsonValue{JsonValue: jsonValue}}
}

func (d *QueryData) waitForRowsToComplete(rowWg *sync.WaitGroup, rowChan chan *proto.Row) {
	rowWg.Wait()
	logging.DisplayProfileData(10 * time.Millisecond)
	close(rowChan)
}

// build a map of all quals to include in the cache key
// this will include all key column quals, and also any quals which were used to filter the matrix items
func (d *QueryData) getCacheQualMap() map[string]*proto.Quals {
	res := d.Quals.ToProtoQualMap()
	// now add in any additional (non-keycolumn) quals which were used to folter the matrix
	for _, col := range d.filteredMatrixColumns {
		if _, ok := res[col]; !ok {
			log.Printf("[TRACE] getCacheQualMap - adding non-key column qual %s as it was used to filter the matrix items", col)
			res[col] = d.QueryContext.UnsafeQuals[col]
		}
	}
	return res
}
