package plugin

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fetchType string

const (
	fetchTypeList fetchType = "list"
	fetchTypeGet            = "get"
)

// call either 'get' or 'list'.
func (t *Table) fetchItems(ctx context.Context, queryData *QueryData) error {
	// if the query contains a single 'equals' constrains for all key columns, then call the 'get' function
	if queryData.FetchType == fetchTypeGet && t.Get != nil {
		logging.LogTime("executeGetCall")
		// return get errors directly
		return t.executeGetCall(ctx, queryData)
	}
	if t.List == nil {
		log.Printf("[WARN] query is not a get call, but no list call is defined, quals: %v", grpc.QualMapToString(queryData.QueryContext.Quals))
		panic("query is not a get call, but no list call is defined")
	}

	logging.LogTime("executeListCall")
	go t.executeListCall(ctx, queryData)

	return nil
}

//  execute a get call for every value in the key column quals
func (t *Table) executeGetCall(ctx context.Context, queryData *QueryData) (err error) {
	logger := t.Plugin.Logger
	logger.Trace("executeGetCall", "table", t.Name, "queryData.KeyColumnQuals", queryData.KeyColumnQuals)
	// verify we have the necessary quals
	if len(queryData.KeyColumnQuals) == 0 {
		return status.Error(codes.Internal, fmt.Sprintf("'Get' call requires an '=' qual for %s", t.Get.KeyColumns.ToString()))
	}

	// deprecated - ItemFromKey is no longer recommended or required
	if t.Get.ItemFromKey != nil {
		return t.executeLegacyGetCall(ctx, queryData)
	}

	defer func() {
		// we can now close the item chan
		queryData.fetchComplete()
		if r := recover(); r != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("get call %s failed with panic %v", helpers.GetFunctionName(t.Get.Hydrate), r))
		}
	}()

	// queryData.KeyColumnQuals is a map of column to qual value
	// NOTE: if there is a SINGLE key column, the qual value may be a list of values
	// in this case we call get for each value
	if keyColumn := t.Get.KeyColumns.Single; keyColumn != "" {
		if qualValueList := queryData.KeyColumnQuals[keyColumn].GetListValue(); qualValueList != nil {
			return t.doGetForQualValues(ctx, queryData, keyColumn, qualValueList)
		}
	}

	// so there is NOT a list of qual values, just call get once
	// call doGet passing nil hydrate item
	return t.doGet(ctx, queryData, nil)
}

func (t *Table) doGetForQualValues(ctx context.Context, queryData *QueryData, keyColumn string, qualValueList *proto.QualValueList) error {
	logger := t.Plugin.Logger
	logger.Trace("executeGetCall - single qual, qual value is a list - executing get for each qual value item", "qualValueList", qualValueList)

	var getWg sync.WaitGroup
	var errorChan = make(chan (error), len(qualValueList.Values))

	// we will make a copy of  queryData and update KeyColumnQuals to replace the list value with a single qual value
	for _, qv := range qualValueList.Values {
		// make a shallow copy of the query data and modify the quals
		queryDataCopy := queryData.ShallowCopy()
		queryDataCopy.KeyColumnQuals[keyColumn] = qv
		// call doGet passing nil hydrate item (hydrate item only needed for legacy implementation)
		go func() {
			if err := t.doGet(ctx, queryDataCopy, nil); err != nil {
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
// if a matrix is defined, defined for every atrix item2
func (t *Table) doGet(ctx context.Context, queryData *QueryData, hydrateItem interface{}) (err error) {
	hydrateKey := helpers.GetFunctionName(t.Get.Hydrate)
	defer func() {
		if p := recover(); p != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("table '%': Get hydrate call %s failed with panic %v", t.Name, hydrateKey, p))
		}
		logging.LogTime(hydrateKey + " end")
	}()
	logging.LogTime(hydrateKey + " start")

	// build rowData item, passing the hydrate item and use to invoke the 'get' hydrate call(s)
	// NOTE the hydrate item is only needed for legacy get calls
	rd := newRowData(queryData, hydrateItem)
	var getItem interface{}

	if len(queryData.Matrix) == 0 {
		retryConfig, shouldIgnoreError := t.buildGetConfig()

		// just invoke callHydrateWithRetries()
		getItem, err = rd.callHydrateWithRetries(ctx, queryData, t.Get.Hydrate, retryConfig, shouldIgnoreError)

	} else {
		// the table has a matrix  - we will invoke get for each matrix  item
		getItem, err = t.getForEach(ctx, queryData, rd)
	}

	if err != nil {
		log.Printf("[WARN] table '%': Get hydrate call %s returned error %v\n", t.Name, hydrateKey, err)
		return err
	}

	// if there is no error and the getItem is nil, we assume the item does not exist
	if getItem != nil {
		// set the rowData Item to the result of the Get hydrate call - this will be passed through to all other hydrate calls
		rd.Item = getItem
		// NOTE: explicitly set the get hydrate results on rowData
		rd.set(hydrateKey, getItem)
		queryData.rowDataChan <- rd
	}

	return nil
}

// getForEach :: execute the provided get call for each of a set of matrixItem
// enables multi-partition fetching
func (t *Table) getForEach(ctx context.Context, queryData *QueryData, rd *RowData) (interface{}, error) {

	log.Printf("[DEBUG] getForEach, matrixItem list: %v\n", queryData.Matrix)

	var wg sync.WaitGroup
	errorChan := make(chan error, len(queryData.Matrix))
	var errors []error
	// define type to stream down results channel - package the item and the matrix item
	// this will for example allow us to determine which region contains the resulting get item
	type resultWithMetadata struct {
		item       interface{}
		matrixItem map[string]interface{}
	}
	resultChan := make(chan *resultWithMetadata, len(queryData.Matrix))
	var results []*resultWithMetadata

	for _, matrixItem := range queryData.Matrix {
		// increment our own wait group
		wg.Add(1)

		go func(matrixItem map[string]interface{}) {
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
			retryConfig, shouldIgnoreError := t.buildGetConfig()

			item, err := rd.callHydrateWithRetries(fetchContext, queryData, t.Get.Hydrate, retryConfig, shouldIgnoreError)

			if err != nil {
				errorChan <- err
			} else if item != nil {
				// stream the get item AND the matrix item
				resultChan <- &resultWithMetadata{item, matrixItem}
			}
		}(matrixItem) // pass matrixItem into goroutine to avoid async timing issues
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
			var item interface{}
			if len(results) == 1 {
				// set the matrix item on the row data
				rd.matrixItem = results[0].matrixItem
				item = results[0].item
			}
			// return item, if we have one
			return item, nil
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
	defer func() {
		if r := recover(); r != nil {
			queryData.streamError(status.Error(codes.Internal, fmt.Sprintf("list call %s failed with panic %v", helpers.GetFunctionName(t.List.Hydrate), r)))
		}
		// list call will return when it has streamed all items so close rowDataChan
		queryData.fetchComplete()
	}()

	logger := t.Plugin.Logger
	// verify we have the necessary quals
	if t.List.KeyColumns != nil && len(queryData.KeyColumnQuals) == 0 {
		err := status.Error(codes.Internal, fmt.Sprintf("'List' call requires an '=' qual for %s", t.List.KeyColumns.ToString()))
		queryData.streamError(err)
	}

	// invoke list call - hydrateResults is nil as list call does not use it (it must comply with HydrateFunc signature)
	listCall := t.List.Hydrate
	// if there is a parent hydrate function, call that
	// - the child 'Hydrate' function will be called by QueryData.StreamListIte,
	if t.List.ParentHydrate != nil {
		listCall = t.List.ParentHydrate
	}

	// queryData.KeyColumnQuals is a map of column to qual value
	// NOTE: if there is a SINGLE key column, the qual value may be a list of values
	// in this case we call get for each value
	if t.List.KeyColumns != nil && t.List.KeyColumns.Single != "" {
		keyColumn := t.List.KeyColumns.Single
		logger.Warn("executeListCall we have single key column")
		if qualValueList := queryData.KeyColumnQuals[keyColumn].GetListValue(); qualValueList != nil {
			t.doListForQualValues(ctx, queryData, keyColumn, qualValueList, listCall)
			return
		}
	}
	t.doList(ctx, queryData, listCall)
}

func (t *Table) doListForQualValues(ctx context.Context, queryData *QueryData, keyColumn string, qualValueList *proto.QualValueList, listCall HydrateFunc) {
	logger := t.Plugin.Logger
	var listWg sync.WaitGroup

	logger.Warn("executeListCall - single qual, qual value is a list - executing list for each qual value item", "qualValueList", qualValueList)
	// we will make a copy of  queryData and update KeyColumnQuals to replace the list value with a single qual value
	for _, qv := range qualValueList.Values {
		logger.Warn("executeListCall passing updated query data", "qv", qv)
		// make a shallow copy of the query data and modify the quals
		queryDataCopy := queryData.ShallowCopy()
		queryDataCopy.KeyColumnQuals[keyColumn] = qv
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

func (t *Table) doList(ctx context.Context, queryData *QueryData, listCall HydrateFunc) {
	rd := newRowData(queryData, nil)

	retryConfig, shouldIgnoreError := t.buildListConfig()

	if len(queryData.Matrix) == 0 {
		log.Printf("[DEBUG] No matrix item")
		if _, err := rd.callHydrateWithRetries(ctx, queryData, listCall, retryConfig, shouldIgnoreError); err != nil {
			queryData.streamError(err)
		}
	} else {
		t.listForEach(ctx, queryData, listCall)
	}
}

// ListForEach :: execute the provided list call for each of a set of matrixItem
// enables multi-partition fetching
func (t *Table) listForEach(ctx context.Context, queryData *QueryData, listCall HydrateFunc) {
	log.Printf("[DEBUG] listForEach: %v\n", queryData.Matrix)
	var wg sync.WaitGroup
	for _, matrixItem := range queryData.Matrix {

		// check whether there is a single equals qual for each matrix item property and if so, check whether
		// the matrix item property values satisfy the conditions
		if !t.matrixItemMeetsQuals(matrixItem, queryData) {
			log.Printf("[INFO] matrix item item does not meet quals, %v, %v\n", queryData.equalsQuals, matrixItem)
			continue
		}

		// create a context with the matrixItem
		fetchContext := context.WithValue(ctx, context_key.MatrixItem, matrixItem)
		wg.Add(1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					queryData.streamError(helpers.ToError(r))
				}
				wg.Done()
			}()
			rd := newRowData(queryData, nil)

			retryConfig, shouldIgnoreError := t.buildListConfig()
			_, err := rd.callHydrateWithRetries(fetchContext, queryData, listCall, retryConfig, shouldIgnoreError)
			if err != nil {
				queryData.streamError(err)
			}
		}()
	}
	wg.Wait()
}

func (t *Table) matrixItemMeetsQuals(matrixItem map[string]interface{}, queryData *QueryData) bool {
	// for the purposes of optimisation , assume matrix item properties correspond to column names
	// if this is NOT the case, it will not fail, but this optimisation will not do anything
	for columnName, metadataValue := range matrixItem {
		// check this matrix item property corresponds to a column
		column, ok := t.columnForName(columnName)
		if !ok {
			// if no, just continue to next property
			continue
		}
		// is there a single equals qual for this column
		if qualValue, ok := queryData.equalsQuals[columnName]; ok {

			// get the underlying qual value
			requiredValue := ColumnQualValue(qualValue, column)

			if requiredValue != metadataValue {
				return false
			}
		}
	}
	return true
}

// deprecated implementation
// t.Get.ItemFromKey is deprectaed. If this table has this property set, run legacy get
func (t *Table) executeLegacyGetCall(ctx context.Context, queryData *QueryData) error {
	defer func() {
		// we can now close the item chan
		queryData.fetchComplete()

		if r := recover(); r != nil {
			queryData.streamError(status.Error(codes.Internal, fmt.Sprintf("get call %s failed with panic %v", helpers.GetFunctionName(t.Get.Hydrate), r)))
		}
	}()

	// build the hydrate input by calling  t.Get.ItemFromKey
	hydrateInput, err := t.legacyBuildHydrateInputForGetCall(ctx, queryData)
	if err != nil {
		queryData.streamError(err)
		return err
	}
	t.Plugin.Logger.Debug("executeLegacyGetCall", "hydrateInput", hydrateInput)
	// there may be more than one hydrate item - loop over them
	for _, hydrateItem := range hydrateInput {
		t.Plugin.Logger.Debug("hydrateItem", "hydrateItem", hydrateItem)
		if err := t.doGet(ctx, queryData, hydrateItem); err != nil {
			queryData.streamError(err)
			return err
		}
	}
	return nil
}

// use the quals to build one call to populate one or more base hydrate item from the quals
// NOTE: if there is a list of quals for the key column then we create a hydrate item for each
// - this is handle `in` clauses
func (t *Table) legacyBuildHydrateInputForGetCall(ctx context.Context, queryData *QueryData) ([]interface{}, error) {
	// NOTE: if there qual value is actually a list of values, we call ItemFromKey for each qual
	// this is only possible for single key column tables (as otherwise we would not have identified this as a get call)
	if keyColumn := t.Get.KeyColumns.Single; keyColumn != "" {
		if qualValueList := queryData.KeyColumnQuals[keyColumn].GetListValue(); qualValueList != nil {
			return t.legacyBuildHydrateInputForMultiQualValueGetCall(ctx, queryData)
		}
	}

	hydrateInputItem, err := t.Get.ItemFromKey(ctx, queryData, &HydrateData{})
	if err != nil {
		return nil, err
	}

	return []interface{}{hydrateInputItem}, nil
}

func (t *Table) legacyBuildHydrateInputForMultiQualValueGetCall(ctx context.Context, queryData *QueryData) ([]interface{}, error) {
	keyColumn := t.Get.KeyColumns.Single
	qualValueList := queryData.KeyColumnQuals[keyColumn].GetListValue()
	log.Println("[TRACE] table has single key, key qual has multiple values")

	// NOTE: store the keyColumnQuals as we will mutate the query data for the calls to ItemFromKey
	keyColumnQuals := queryData.KeyColumnQuals

	var hydrateInput []interface{}
	for _, qualValue := range qualValueList.Values {
		// build a new map for KeyColumnQuals and mutate the query data
		queryData.KeyColumnQuals = map[string]*proto.QualValue{
			keyColumn: qualValue,
		}
		hydrateInputItem, err := t.Get.ItemFromKey(ctx, queryData, &HydrateData{})
		if err != nil {
			return nil, err
		}
		hydrateInput = append(hydrateInput, hydrateInputItem)
	}

	// now revert keyColumnQuals on the query data for good manners
	queryData.KeyColumnQuals = keyColumnQuals

	return hydrateInput, nil
}

func (t *Table) buildGetConfig() (*RetryConfig, ErrorPredicate) {
	retryConfig := t.Get.RetryConfig
	if retryConfig == nil && t.Plugin.DefaultGetConfig != nil {
		retryConfig = t.Plugin.DefaultGetConfig.RetryConfig
	}
	shouldIgnoreError := t.Get.ShouldIgnoreError
	if shouldIgnoreError == nil && t.Plugin.DefaultGetConfig != nil {
		shouldIgnoreError = t.Plugin.DefaultGetConfig.ShouldIgnoreError
	}
	return retryConfig, shouldIgnoreError
}

func (t *Table) buildListConfig() (*RetryConfig, ErrorPredicate) {
	retryConfig := t.List.RetryConfig
	if retryConfig == nil {
		retryConfig = t.Plugin.DefaultRetryConfig
	}
	shouldIgnoreError := t.List.ShouldIgnoreError
	return retryConfig, shouldIgnoreError
}
