package plugin

import (
	"context"
	"log"

	"github.com/turbot/go-kit/helpers"

	"sync"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/connection"

	"github.com/turbot/steampipe-plugin-sdk/logging"

	pb "github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

const itemBufferSize = 100

type QueryData struct {
	ConnectionManager *connection.Manager
	Table             *Table
	// if this is a get call (or a list call if list key columns are specified)
	// this will be populated with the quals as a map of column name to quals
	KeyColumnQuals map[string]*pb.QualValue
	// is this a get or a list
	FetchType    fetchType
	QueryContext *pb.QueryContext

	// internal
	hydrateCalls       []*HydrateCall
	concurrencyManager *ConcurrencyManager
	rowDataChan        chan *RowData
	errorChan          chan error
	stream             pb.WrapperPlugin_ExecuteServer
	// wait group used to syncronise parent-child list fetches - each child hydrate function increments this wait group
	listWg sync.WaitGroup
}

func newQueryData(queryContext *pb.QueryContext, table *Table, stream pb.WrapperPlugin_ExecuteServer) *QueryData {
	d := &QueryData{
		ConnectionManager: connection.NewManager(),
		Table:             table,
		QueryContext:      queryContext,

		// asyncronously read items using the 'get' or 'list' API
		// items are streamed on rowDataChan, errors returned on errorChan
		rowDataChan: make(chan *RowData, itemBufferSize),
		errorChan:   make(chan error, 1),
		stream:      stream,
	}
	// determine whether this is a get or a list call and set the KeyColumnQuals if present
	d.SetFetchType(table)

	// NOTE: for count(*) queries, there will be no columns - add in 1 column so that we have some data to return
	ensureColumns(queryContext, table)

	d.hydrateCalls = table.requiredHydrateCalls(queryContext.Columns, d.FetchType)
	d.concurrencyManager = newConcurrencyManager(table)
	return d
}

// SetFetchType :: determine whether this is a get or a list call
func (d *QueryData) SetFetchType(table *Table) {
	// populate a map of column to qual value
	var getQuals map[string]*pb.QualValue
	var listQuals map[string]*pb.QualValue

	if table.Get != nil {
		getQuals = table.getKeyColumnQuals(d, table.Get.KeyColumns)
	}
	if table.List != nil && table.List.KeyColumns != nil {
		listQuals = table.getKeyColumnQuals(d, table.List.KeyColumns)
	}
	// if quals provided in query satisfy both get and list, get wins (a get is likely to be more efficient)
	if getQuals != nil {
		log.Printf("[INFO] get quals - this is a get call  %+v", getQuals)
		d.KeyColumnQuals = getQuals
		d.FetchType = fetchTypeGet
	} else if listQuals != nil {
		log.Printf("[INFO] list quals - this is list call  %+v", listQuals)
		d.KeyColumnQuals = listQuals
		d.FetchType = fetchTypeList
	} else {
		// so we do not have required quals for either.
		// if there is a List config, set this to be a list call, otherwise set it to get
		// if we do not the required quals we will fail with an appropriate error
		if table.List != nil {
			log.Printf("[INFO] this is list call, with no list quals")
			d.FetchType = fetchTypeList
		} else {
			log.Printf("[WARN] No get quals passed but no list call defined - default to get call")
			d.FetchType = fetchTypeGet
		}
	}
}

// for count(*) queries, there will be no columns - add in 1 column so that we have some data to return
func ensureColumns(queryContext *pb.QueryContext, table *Table) {
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

// StreamListItem :: stream an item returned from the list call
// wrap in a rowData object
func (d *QueryData) StreamListItem(ctx context.Context, item interface{}) {
	// if the calling function was the ParentHydrate function from the list config,
	// stream the results to the child list hydrate function and return
	parentListHydrate := d.Table.List.ParentHydrate
	if parentListHydrate == nil {
		d.StreamLeafListItem(ctx, item)
		return
	}

	parentHydrateName := helpers.GetFunctionName(parentListHydrate)
	Logger(ctx).Debug("StreamListItem: called from parent hydrate function - streaming result to child hydrate function",
		"parent hydrate", parentHydrateName,
		"child hydrate", helpers.GetFunctionName(d.Table.List.Hydrate),
		"item", item)
	d.listWg.Add(1)

	go func() {
		defer d.listWg.Done()
		d.Table.List.Hydrate(ctx, d, &HydrateData{Item: item})
	}()
}

func (d *QueryData) StreamLeafListItem(_ context.Context, item interface{}) {
	rd := newRowData(d, item)

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
func (d *QueryData) streamRows(_ context.Context, rowChan chan *pb.Row) error {
	log.Println("[TRACE] streamRows")
	for {
		log.Println("[TRACE] stream loop")
		// wait for either an item or an error
		select {
		case row := <-rowChan:
			log.Println("[TRACE] row chan select - got a row")
			if row == nil {
				log.Println("[TRACE] row chan closed, stop streaming")
				// channel closed
				return nil
			}
			if err := d.streamRow(row); err != nil {
				log.Printf("[ERROR] stream.Send returned error: %v\n", err)
				return err
			}
		case err := <-d.errorChan:
			log.Printf("[ERROR] streamRows err chan select: %v\n", err)
			return err
		}
	}
}

func (d *QueryData) streamRow(row *pb.Row) error {
	return d.stream.Send(&pb.ExecuteResponse{Row: row})
}

func (d *QueryData) streamError(err error) {
	d.errorChan <- err
}

// iterate over rowDataChan, for each item build the row and stream over rowChan
func (d *QueryData) buildRows(ctx context.Context) chan *pb.Row {
	log.Println("[TRACE] buildRows")
	const rowBufferSize = 10

	// stream data for each item
	var rowChan = make(chan *pb.Row, rowBufferSize)
	// we need to use a wait group for rows we cannot close the row channel when the item channel is closed
	// as getRow is executing asyncronously
	var rowWg sync.WaitGroup

	// start goroutine to read items from item chan and generate row data
	go func() {
		for {
			log.Println("[TRACE] start row gen loop")
			// wait for either an rowData or an error
			select {
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
			case err := <-d.errorChan:
				log.Printf("[ERROR] err chan select: %v\n", err)
				// put it back in the channel and return
				d.errorChan <- err
				return
			}
		}
	}()

	return rowChan
}

// execute necessary hydrate calls to populate row data
func (d *QueryData) buildRow(ctx context.Context, rowData *RowData, rowChan chan *pb.Row, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	// delegate the work to a row object
	row, err := rowData.getRow(ctx)
	if err != nil {
		d.streamError(err)
	} else {
		rowChan <- row
	}

	log.Println("[TRACE] getRow return")
}

func (d *QueryData) waitForRowsToComplete(rowWg *sync.WaitGroup, rowChan chan *pb.Row) {
	log.Println("[TRACE] wait for rows")
	rowWg.Wait()
	logging.DisplayProfileData(10 * time.Millisecond)
	log.Println("[TRACE] rowWg complete - CLOSING ROW CHANNEL")
	close(rowChan)
}

// is there a single '=' qual for this column
func (d *QueryData) singleEqualsQual(column string) (*pb.Qual, bool) {
	quals, ok := d.QueryContext.Quals[column]
	log.Printf("[DEBUG] singleEqualsQual - quals: %v\n", quals)
	if !ok {
		log.Println("[DEBUG] no quals for column")
		return nil, false
	}

	if len(quals.Quals) == 1 && quals.Quals[0].GetStringValue() == "=" && quals.Quals[0].Value != nil {
		return quals.Quals[0], true
	}
	return nil, false
}
