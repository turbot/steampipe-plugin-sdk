package plugin

import (
	"context"
	"fmt"
	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strings"
	"sync"
)

type fetchType string

const (
	fetchTypeList fetchType = "list"
	fetchTypeGet  fetchType = "get"
)

// call either 'get' or 'list'.
func (t *Table) fetchItems(ctx context.Context, queryData *QueryData) error {
	ctx, span := telemetry.StartSpan(ctx, t.Plugin.Name, "Table.fetchItems (%s)", t.Name)

	defer span.End()

	// if the query contains a single 'equals' constrains for all key columns, then call the 'get' function
	if queryData.FetchType == fetchTypeGet && t.Get != nil {
		logging.LogTime("executeGetCall")
		// return get errors directly
		return t.executeGetCall(ctx, queryData)
	}
	if t.List == nil {
		log.Printf("[WARN] query is not a get call, but no list call is defined, quals: %v", grpc.QualMapToString(queryData.QueryContext.UnsafeQuals, true))
		panic("query is not a get call, but no list call is defined")
	}

	logging.LogTime("executeListCall")
	go t.executeListCall(ctx, queryData)

	return nil
}

// execute a get call for every value in the key column quals
func (t *Table) executeGetCall(ctx context.Context, queryData *QueryData) (err error) {
	ctx, span := telemetry.StartSpan(ctx, t.Plugin.Name, "Table.executeGetCall (%s)", t.Name)
	defer span.End()

	log.Printf("[TRACE] executeGetCall, table: %s, queryData.KeyColumnQuals: %v", t.Name, queryData.EqualsQuals)

	unsatisfiedColumns := queryData.Quals.GetUnsatisfiedKeyColumns(t.Get.KeyColumns)
	// verify we have the necessary quals
	if len(unsatisfiedColumns) > 0 {
		err := t.buildMissingKeyColumnError("Get", unsatisfiedColumns)
		return err
	}

	defer func() {
		// we can now close the item chan
		queryData.fetchComplete(ctx)
		if r := recover(); r != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("get call %s failed with panic %v", t.Get.namedHydrate.Name, r))
		}
	}()

	// queryData.KeyColumnQuals is a map of column to qual value
	// NOTE: if there is a SINGLE or ANY key columns, the qual value may be a list of values
	// in this case we call get for each value
	keyColumnName, keyColumnQualList := t.getListQualValueForGetCall(queryData)
	if keyColumnQualList != nil {
		return t.doGetForQualValues(ctx, queryData, keyColumnName, keyColumnQualList)
	}

	// so there is NOT a list of qual values, just call get once
	// call doGet passing nil hydrate item
	return t.doGet(ctx, queryData)
}

func (t *Table) buildMissingKeyColumnError(operation string, unsatisfiedColumns KeyColumnSlice) error {
	unsatisfied := unsatisfiedColumns.String()
	if len(unsatisfiedColumns) > 1 {
		unsatisfied = fmt.Sprintf("\n%s", helpers.Tabify(unsatisfied, "    "))
	}
	err := status.Error(codes.Internal, fmt.Sprintf("'%s' call for table '%s' is missing %d required %s: %s\n",
		operation,
		t.Name,
		len(unsatisfiedColumns),
		pluralize.NewClient().Pluralize("qual", len(unsatisfiedColumns), false),
		unsatisfied,
	))
	return err
}

// determine whether any of the get call quals are list values
// if there is a SINGLE or ANY key columns, determine whether the qual value is a list of values
// if so return the list and the key column
func (t *Table) getListQualValueForGetCall(queryData *QueryData) (string, *proto.QualValueList) {
	k := t.Get.KeyColumns

	log.Printf("[TRACE] getListQualValueForGetCall %d key column quals: %s ", len(k), k)

	// get map of all the key columns quals which have a list value
	listValueMap := queryData.EqualsQuals.GetListQualValues()
	if len(listValueMap) == 0 {
		return "", nil
	}

	// if 'any_of' key columns are defined, check each of them to see if a 'list qual value was passed
	if t.Get.KeyColumns.IsAnyOf() {
		for _, keyColumn := range t.Get.KeyColumns {
			if listValue, ok := listValueMap[keyColumn.Name]; ok {
				return keyColumn.Name, listValue
			}
		}
	}

	// is ONE of the key column quals a list?
	if len(listValueMap) == 1 {
		for keyColumnName, listValue := range listValueMap {
			return keyColumnName, listValue
		}
	} else {
		log.Printf("[WARN] more than 1 key column qual has a list value - this is unsupported")
	}

	return "", nil
}

func (t *Table) doGetForQualValues(ctx context.Context, queryData *QueryData, keyColumnName string, qualValueList *proto.QualValueList) error {
	log.Printf("[TRACE] doGetForQualValues - single qual, qual value is a list - executing get for each qual value item, qualValueList: %v", qualValueList)

	var getWg sync.WaitGroup
	var errorChan = make(chan (error), len(qualValueList.Values))

	// NOTE: ensure QueryData.rowDataChan can buffer sufficient items
	// (we normally expect it would be sufficient)
	if len(qualValueList.Values) > rowDataBufferSize {
		queryData.rowDataChan = make(chan *rowData, len(qualValueList.Values))
	}

	// we will make a copy of  queryData and update KeyColumnQuals to replace the list value with a single qual value
	for _, qv := range qualValueList.Values {
		// make a shallow copy of the query data and modify the quals
		queryDataCopy := queryData.shallowCopy()
		queryDataCopy.EqualsQuals[keyColumnName] = qv
		queryDataCopy.Quals[keyColumnName] =
			&KeyColumnQuals{Name: keyColumnName, Quals: quals.QualSlice{{Column: keyColumnName, Operator: "=", Value: qv}}}

		getWg.Add(1)
		// call doGet passing nil hydrate item (hydrate item only needed for legacy implementation)
		go func() {
			if err := t.doGet(ctx, queryDataCopy); err != nil {
				errorChan <- err
			}
			getWg.Done()
		}()
	}

	getWg.Wait()

	// err will contain the last error (is any)
	select {
	case err := <-errorChan:
		log.Printf("[WARN] doGetForQualValues returned an error: %v", err)
		return err
	default:
		return nil
	}

}

// execute a get call for a single key column qual value
// if a matrix is defined, call for every matrix item
func (t *Table) doGet(ctx context.Context, queryData *QueryData) (err error) {
	hydrateKey := t.Get.namedHydrate.Name
	defer func() {
		if p := recover(); p != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("table '%s': Get hydrate call %s failed with panic %v", t.Name, hydrateKey, p))
		}
		logging.LogTime(hydrateKey + " end")
	}()
	logging.LogTime(hydrateKey + " start")

	// do the get call, fanning out matrix if needed
	var rd *rowData
	if len(queryData.Matrix) == 0 {
		rd, err = t.get(ctx, queryData)
	} else {
		// the table has a matrix  - we will invoke get for each matrix  item
		// returns row data containing the result (if any)
		rd, err = t.getForEachMatrixItem(ctx, queryData)
	}

	if err != nil {
		log.Printf("[WARN] table '%s': Get hydrate call %s returned error %v\n", t.Name, hydrateKey, err)
		return err
	}

	// if there is no error and the getItem is nil, we assume the item does not exist
	if !helpers.IsNil(rd.item) {
		// NOTE: explicitly set the get hydrate results on rowData
		rd.set(hydrateKey, rd.item)
		// set the rowsStreamed to 1
		queryData.queryStatus.rowsStreamed = 1
		// send the result down the stream
		queryData.rowDataChan <- rd
	}

	return nil
}

// execute single get call (i.e. no matrix)
func (t *Table) get(ctx context.Context, queryData *QueryData) (*rowData, error) {
	// now we know there is no matrix,  initialise the rate limiters for this query data
	queryData.initialiseRateLimiters()
	// now wait for any configured 'get' rate limiters
	fetchDelay := queryData.fetchLimiters.wait(ctx)
	// set the metadata
	queryData.setGetLimiterMetadata(fetchDelay)
	// build rowData and use to invoke the 'get' hydrate call
	rd := newRowData(queryData, nil)
	// just invoke callHydrateWithRetries()
	var getItem any
	getItem, err := rd.callHydrateWithRetries(ctx, queryData, t.Get.namedHydrate, t.Get.IgnoreConfig, t.Get.RetryConfig)
	rd.item = getItem
	return rd, err
}

// getForEachMatrixItem executes the provided get call for each of a set of matrixItem
// enables multi-partition fetching
func (t *Table) getForEachMatrixItem(ctx context.Context, queryData *QueryData) (*rowData, error) {
	log.Printf("[TRACE] getForEachMatrixItem, matrixItem list: %v\n", queryData.filteredMatrix)

	var wg sync.WaitGroup
	errorChan := make(chan error, len(queryData.Matrix))
	var errors []error

	resultChan := make(chan *rowData, len(queryData.Matrix))
	var results []*rowData

	// NOTE - we use the filtered matrix - which means we may not actually run any hydrate calls
	// if the quals have filtered out all matrix items (e.g. select where region = 'invalid')
	for _, matrixItem := range queryData.filteredMatrix {

		// increment our own wait group
		wg.Add(1)

		// pass matrixItem into goroutine to avoid async timing issues
		go func(matrixItem map[string]any) {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						errorChan <- err
					} else {
						errorChan <- fmt.Errorf("%v", r)
					}
				}
				wg.Done()
			}()
			// create a context with the matrix item
			fetchContext := context.WithValue(ctx, context_key.MatrixItem, matrixItem)

			// clone the query data and add the matrix properties to quals
			matrixQueryData := queryData.shallowCopy()
			matrixQueryData.setMatrixItem(matrixItem)
			// now we have set the matrix item, initialise the rate limiters for this query data
			matrixQueryData.initialiseRateLimiters()
			// now wait for any configured 'get' rate limiters
			fetchDelay := matrixQueryData.fetchLimiters.wait(ctx)
			// set the metadata
			matrixQueryData.setGetLimiterMetadata(fetchDelay)
			// create a rowData for each matrix item
			matrixRd := newRowData(matrixQueryData, nil)

			// now call hydrate from the matrix rowdata
			item, err := matrixRd.callHydrateWithRetries(fetchContext, matrixQueryData, t.Get.namedHydrate, t.Get.IgnoreConfig, t.Get.RetryConfig)

			if err != nil {
				log.Printf("[WARN] callHydrateWithRetries returned error %v", err)
				errorChan <- err
			} else if !helpers.IsNil(item) {
				matrixRd.item = item
				// stream the rowdata
				resultChan <- matrixRd
			}
		}(matrixItem)
	}

	// convert wg to channel so we can select it
	doneChan := make(chan bool, 1)
	go func() {
		wg.Wait()
		doneChan <- true
	}()

	for {
		select {
		case err := <-errorChan:
			log.Printf("[WARN] err %v\n", err)
			errors = append(errors, err)
		case result := <-resultChan:
			if result != nil {
				results = append(results, result)
			}
		case <-doneChan:
			if err := buildSingleError(errors); err != nil {
				return nil, err
			}

			// if we have more than one result, this means the key is not globally unique - an error
			if len(results) > 1 {
				return nil, fmt.Errorf("get call returned %d results - the key column is not globally unique", len(results))
			}
			// if we got a result, set the matrix item on the row data
			var res *rowData
			if len(results) == 1 {
				res = results[0]
			} else {
				// return empty rowdata
				res = &rowData{}
			}
			// return item, if we have one
			return res, nil
		}
	}
}

func buildSingleError(errors []error) error {
	if len(errors) == 0 {
		return nil
	}
	if len(errors) == 1 {
		return errors[0]
	}
	errStrings := []string{}
	for _, err := range errors {
		msg := err.Error()
		if !helpers.StringSliceContains(errStrings, msg) {
			errStrings = append(errStrings, msg)
		}

	}
	return fmt.Errorf("%d list calls returned errors:\n %s", len(errors), strings.Join(errStrings, "\n"))
}

func (t *Table) executeListCall(ctx context.Context, queryData *QueryData) {
	ctx, span := telemetry.StartSpan(ctx, t.Plugin.Name, "Table.executeListCall (%s)", t.Name)
	defer span.End()

	log.Printf("[TRACE] executeListCall START (%s)", queryData.connectionCallId)
	defer log.Printf("[TRACE] executeListCall COMPLETE (%s)", queryData.connectionCallId)
	defer func() {
		if r := recover(); r != nil {
			queryData.streamError(status.Error(codes.Internal, fmt.Sprintf("list call %s failed with panic %v", t.List.namedHydrate.Name, r)))
		}
		// list call will return when it has streamed all items so close rowDataChan
		queryData.fetchComplete(ctx)
	}()

	// verify we have the necessary quals
	unsatisfiedColumns := queryData.Quals.GetUnsatisfiedKeyColumns(t.List.KeyColumns)
	if len(unsatisfiedColumns) > 0 {
		err := t.buildMissingKeyColumnError("List", unsatisfiedColumns)
		queryData.streamError(err)
		return
	}

	// invoke list call - hydrateResults is nil as list call does not use it (it must comply with HydrateFunc signature)
	var childHydrate namedHydrateFunc
	listCall := t.List.namedHydrate
	// if there is a parent hydrate function, call that
	// - the child 'Hydrate' function will be called by QueryData.StreamListItem,
	if t.List.ParentHydrate != nil {
		listCall = t.List.namedParentHydrate
		childHydrate = t.List.namedHydrate
	}

	// store the list call and child hydrate call - these will be used later when we call setListMetadata
	queryData.setListCalls(listCall, childHydrate)

	// NOTE: if there is an IN qual, the qual value will be a list of values
	// in this case we call list for each value
	if listQual := t.getListCallQualValueList(queryData); listQual != nil {
		log.Printf("[TRACE] one qual with list value will be processed: %v", *listQual)
		qualValueList := listQual.Value.GetListValue()
		t.doListForQualValues(ctx, queryData, listQual.Column, qualValueList, listCall)
		return
	}

	t.doList(ctx, queryData, listCall)
}

// if this table defines key columns, and if there is a SINGLE qual with a list value
// return that qual
func (t *Table) getListCallQualValueList(queryData *QueryData) *quals.Qual {
	if len(t.List.KeyColumns) == 0 {
		return nil
	}

	log.Printf("[TRACE] list config defines key columns, checking for list qual values")

	// we can support IN calls for key columns if only 1 qual has a list value

	// first determine whether more than 1 qual has a list value
	qualsWithListValues := queryData.Quals.GetListQualValues()

	numQualsWithListValues := len(qualsWithListValues)
	if numQualsWithListValues > 0 {
		log.Printf("[TRACE] %d %s have list values",
			numQualsWithListValues,
			pluralize.NewClient().Pluralize("qual", numQualsWithListValues, false))

		// if we have more than one qual with list values, extract the required ones
		// if more than one of these is required, this is an error
		// - we do not support multiple required quals with list values
		var requiredListQuals quals.QualSlice
		if numQualsWithListValues > 1 {
			log.Printf("[TRACE] more than 1 qual has a list value - counting required quals with list value")

			for _, listQual := range qualsWithListValues {

				// find key column
				if c := t.List.KeyColumns.Find(listQual.Column); c.Require == Required {
					requiredListQuals = append(requiredListQuals, listQual)
				}
			}
			if len(requiredListQuals) > 1 {
				log.Printf("[WARN] more than 1 required qual has a list value - we cannot call list for each so passing quals through to plugin unaltered")
				qualsWithListValues = nil
			} else {
				log.Printf("[TRACE] after removing optional quals %d required remain", len(requiredListQuals))
				qualsWithListValues = requiredListQuals
			}
		}
	}
	// list are there any list quals left to process?
	if len(qualsWithListValues) == 1 {
		return qualsWithListValues[0]
	}
	return nil
}

// doListForQualValues is called when there is an equals qual and the qual value is a list of values
func (t *Table) doListForQualValues(ctx context.Context, queryData *QueryData, keyColumn string, qualValueList *proto.QualValueList, listCall namedHydrateFunc) {
	var listWg sync.WaitGroup

	log.Printf("[TRACE] doListForQualValues - qual value is a list - executing list for each qual value item, qualValueList: %v", qualValueList)
	// we will make a copy of  queryData and update KeyColumnQuals to replace the list value with a single qual value
	for _, qv := range qualValueList.Values {
		log.Printf("[TRACE] executeListCall passing updated query data, qv: %v", qv)
		// make a shallow copy of the query data and modify the value of the key column qual to be the value list item
		queryDataCopy := queryData.shallowCopy()
		// update qual maps to replace list value with list element
		queryDataCopy.EqualsQuals[keyColumn] = qv
		queryDataCopy.Quals[keyColumn] = &KeyColumnQuals{
			Name: keyColumn, Quals: quals.QualSlice{{Column: keyColumn, Operator: "=", Value: qv}}}

		listWg.Add(1)
		// call doGet passing nil hydrate item (hydrate item only needed for legacy implementation)
		go func() {
			t.doList(ctx, queryDataCopy, listCall)
			listWg.Done()
		}()
	}
	// and we are done
	listWg.Wait()
}

func (t *Table) doList(ctx context.Context, queryData *QueryData, listCall namedHydrateFunc) {
	ctx, span := telemetry.StartSpan(ctx, t.Plugin.Name, "Table.doList (%s)", t.Name)
	defer span.End()

	log.Printf("[TRACE] doList (%s)", queryData.connectionCallId)

	// create rowData, purely so we can call callHydrateWithRetries
	rd := newRowData(queryData, nil)

	// if a matrix is defined, run listForEachMatrixItem
	if queryData.Matrix != nil {
		log.Printf("[TRACE] doList: matrix len %d - calling  listForEachMatrixItem", len(queryData.Matrix))
		t.listForEachMatrixItem(ctx, queryData, listCall)
		return
	}

	// OK we know there is no matrix, initialise the rate limiters for this query data
	queryData.initialiseRateLimiters()
	// now wait for any configured 'list' rate limiters
	fetchDelay := queryData.fetchLimiters.wait(ctx)
	// set the metadata
	queryData.setListMetadata(fetchDelay)

	log.Printf("[TRACE] doList: no matrix item")

	// we cannot retry errors in the list hydrate function after streaming has started
	listRetryConfig := t.List.RetryConfig.GetListRetryConfig()

	if _, err := rd.callHydrateWithRetries(ctx, queryData, listCall, t.List.IgnoreConfig, listRetryConfig); err != nil {
		log.Printf("[WARN] doList callHydrateWithRetries (%s) returned err %s", queryData.connectionCallId, err.Error())
		queryData.streamError(err)
	}

}

// ListForEach executes the provided list call for each of a set of matrixItem
// enables multi-partition fetching
func (t *Table) listForEachMatrixItem(ctx context.Context, queryData *QueryData, listCall namedHydrateFunc) {
	ctx, span := telemetry.StartSpan(ctx, t.Plugin.Name, "Table.listForEachMatrixItem (%s)", t.Name)
	// TODO add matrix item to span
	defer span.End()

	log.Printf("[TRACE] listForEachMatrixItem: %v\n", queryData.Matrix)
	var wg sync.WaitGroup
	// NOTE - we use the filtered matrix - which means we may not actually run any hydrate calls
	// if the quals have filtered out all matrix items (e.g. select where region = 'invalid')
	for _, matrixItem := range queryData.filteredMatrix {
		// create a context with the matrixItem
		fetchContext := context.WithValue(ctx, context_key.MatrixItem, matrixItem)
		wg.Add(1)

		// pass matrixItem into goroutine to avoid async timing issues
		go func(matrixItem map[string]any) {
			defer func() {
				if r := recover(); r != nil {
					queryData.streamError(helpers.ToError(r))
				}
				wg.Done()
			}()
			// create rowData, purely so we can call callHydrateWithRetries
			rd := newRowData(queryData, nil)
			rd.matrixItem = matrixItem

			// clone the query data and add the matrix properties to quals
			matrixQueryData := queryData.shallowCopy()
			matrixQueryData.setMatrixItem(matrixItem)

			// now we have set the matrix item, initialise the rate limiters for this query data
			matrixQueryData.initialiseRateLimiters()
			// now wait for any configured 'list' rate limiters
			fetchDelay := matrixQueryData.fetchLimiters.wait(ctx)
			// set the metadata
			matrixQueryData.setListMetadata(fetchDelay)

			// we cannot retry errors in the list hydrate function after streaming has started
			listRetryConfig := t.List.RetryConfig.GetListRetryConfig()

			_, err := rd.callHydrateWithRetries(fetchContext, matrixQueryData, listCall, t.List.IgnoreConfig, listRetryConfig)
			if err != nil {
				log.Printf("[WARN] callHydrateWithRetries returned error %v", err)
				queryData.streamError(err)
			}
		}(matrixItem)
	}
	wg.Wait()
}
