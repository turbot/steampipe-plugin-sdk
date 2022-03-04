package plugin

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v2/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v2/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v2/logging"
	"github.com/turbot/steampipe-plugin-sdk/v2/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v2/plugin/quals"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fetchType string

const (
	fetchTypeList fetchType = "list"
	fetchTypeGet            = "get"
)

/*
multiregion fetch
Each plugin table can optionally define a function `GetMatrixItem`.

This returns a list of maps, each of which contains the parameters required
to do get/list for a given region (or whatever partitioning is relevant to the plugin)

The plugin would typically get this information from the connection config

If a matrix is returned by the plugin, we execute Get/List calls for each matrix item (e.g. each region)

NOTE: if the quals include the matrix property (or properties),w e check whether each matrix
item meets the quals and if not, do not execute for that item

For example, for the query
	select vpc_id, region from aws_vpc where region = 'us-east-1'
we would only execute a List function for the matrix item { region: "us-east-1" },
even if other were defined in the connection config

When executing for each matrix item, the matrix item is put into the context, available for use by the get/list call
*/

// call either 'get' or 'list'.
func (t *Table) fetchItems(ctx context.Context, queryData *QueryData) error {
	// if the query contains a single 'equals' constrains for all key columns, then call the 'get' function
	if queryData.FetchType == fetchTypeGet && t.Get != nil {
		logging.LogTime("executeGetCall")
		// return get errors directly
		return t.executeGetCall(ctx, queryData)
	}
	if t.List == nil {
		log.Printf("[WARN] query is not a get call, but no list call is defined, quals: %v", grpc.QualMapToString(queryData.QueryContext.UnsafeQuals))
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
	unsatisfiedColumns := queryData.Quals.GetUnsatisfiedKeyColumns(t.Get.KeyColumns)

	// verify we have the necessary quals
	if len(unsatisfiedColumns) > 0 {
		err := t.buildMissingKeyColumnError("Get", unsatisfiedColumns)
		return err
	}

	defer func() {
		// we can now close the item chan
		queryData.fetchComplete()
		if r := recover(); r != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("get call %s failed with panic %v", helpers.GetFunctionName(t.Get.Hydrate), r))
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
	return t.doGet(ctx, queryData, nil)
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
	listValueMap := queryData.KeyColumnQuals.GetListQualValues()
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
	logger := t.Plugin.Logger
	logger.Warn("executeGetCall - single qual, qual value is a list - executing get for each qual value item", "qualValueList", qualValueList)

	var getWg sync.WaitGroup
	var errorChan = make(chan (error), len(qualValueList.Values))

	// we will make a copy of  queryData and update KeyColumnQuals to replace the list value with a single qual value
	for _, qv := range qualValueList.Values {
		// make a shallow copy of the query data and modify the quals
		queryDataCopy := queryData.ShallowCopy()
		queryDataCopy.KeyColumnQuals[keyColumnName] = qv
		getWg.Add(1)
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
// if a matrix is defined, call for every matrix item
func (t *Table) doGet(ctx context.Context, queryData *QueryData, hydrateItem interface{}) (err error) {
	hydrateKey := helpers.GetFunctionName(t.Get.Hydrate)
	defer func() {
		if p := recover(); p != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("table '%s': Get hydrate call %s failed with panic %v", t.Name, hydrateKey, p))
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
		log.Printf("[WARN] table '%s': Get hydrate call %s returned error %v\n", t.Name, hydrateKey, err)
		return err
	}

	// if there is no error and the getItem is nil, we assume the item does not exist
	if !helpers.IsNil(getItem) {
		// set the rowData Item to the result of the Get hydrate call - this will be passed through to all other hydrate calls
		rd.Item = getItem
		// NOTE: explicitly set the get hydrate results on rowData
		rd.set(hydrateKey, getItem)
		queryData.rowDataChan <- rd
	}

	return nil
}

// getForEach executes the provided get call for each of a set of matrixItem
// enables multi-partition fetching
func (t *Table) getForEach(ctx context.Context, queryData *QueryData, rd *RowData) (interface{}, error) {

	log.Printf("[TRACE] getForEach, matrixItem list: %v\n", queryData.filteredMatrix)

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

	// NOTE - we use the filtered matrix - which means we may not actually run any hydrate calls
	// if the quals have filtered out all matrix items (e.g. select where region = 'invalid')
	for _, matrixItem := range queryData.filteredMatrix {

		// increment our own wait group
		wg.Add(1)

		// pass matrixItem into goroutine to avoid async timing issues
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

			// clone the query data and add the matrix properties to quals
			matrixQueryData := queryData.ShallowCopy()
			matrixQueryData.updateQualsWithMatrixItem(matrixItem)

			log.Printf("[TRACE] callHydrateWithRetries for matrixItem %v, key columns %v", matrixItem, matrixQueryData.KeyColumnQuals)
			item, err := rd.callHydrateWithRetries(fetchContext, matrixQueryData, t.Get.Hydrate, retryConfig, shouldIgnoreError)

			if err != nil {
				log.Printf("[TRACE] callHydrateWithRetries returned error %v", err)
				errorChan <- err
			} else if !helpers.IsNil(item) {
				// stream the get item AND the matrix item
				resultChan <- &resultWithMetadata{item, matrixItem}
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
	log.Printf("[TRACE] executeListCall")
	defer func() {
		if r := recover(); r != nil {
			queryData.streamError(status.Error(codes.Internal, fmt.Sprintf("list call %s failed with panic %v", helpers.GetFunctionName(t.List.Hydrate), r)))
		}
		// list call will return when it has streamed all items so close rowDataChan
		queryData.fetchComplete()
	}()

	// verify we have the necessary quals
	unsatisfiedColumns := queryData.Quals.GetUnsatisfiedKeyColumns(t.List.KeyColumns)
	if len(unsatisfiedColumns) > 0 {
		err := t.buildMissingKeyColumnError("List", unsatisfiedColumns)
		queryData.streamError(err)
		return
	}

	// invoke list call - hydrateResults is nil as list call does not use it (it must comply with HydrateFunc signature)
	listCall := t.List.Hydrate
	// if there is a parent hydrate function, call that
	// - the child 'Hydrate' function will be called by QueryData.StreamListIte,
	if t.List.ParentHydrate != nil {
		listCall = t.List.ParentHydrate
	}

	// NOTE: if there is an IN qual, the qual value will be a list of values
	// in this case we call list for each value
	if len(t.List.KeyColumns) > 0 {
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
					log.Printf("[WARN] more than 1 required qual has a list value - we cannot call list for each to passing qualds through to plugin unaltered")
					qualsWithListValues = nil
				} else {
					log.Printf("[TRACE] after removing optional quals %d required remain", len(requiredListQuals))
					qualsWithListValues = requiredListQuals
				}
			}
		}
		// list are there any list quals left to process?
		if len(qualsWithListValues) == 1 {
			listQual := qualsWithListValues[0]
			log.Printf("[TRACE] one qual with list value will be processed: %v", *listQual)
			qualValueList := listQual.Value.GetListValue()
			t.doListForQualValues(ctx, queryData, listQual.Column, qualValueList, listCall)
			return
		}

	}

	t.doList(ctx, queryData, listCall)
}

// doListForQualValues is called when there is an equals qual and the qual value is a list of values
func (t *Table) doListForQualValues(ctx context.Context, queryData *QueryData, keyColumn string, qualValueList *proto.QualValueList, listCall HydrateFunc) {
	logger := t.Plugin.Logger
	var listWg sync.WaitGroup

	logger.Trace("doListForQualValues - qual value is a list - executing list for each qual value item", "qualValueList", qualValueList)
	// we will make a copy of  queryData and update KeyColumnQuals to replace the list value with a single qual value
	for _, qv := range qualValueList.Values {
		logger.Trace("executeListCall passing updated query data", "qv", qv)
		// make a shallow copy of the query data and modify the value of the key column qual to be the value list item
		queryDataCopy := queryData.ShallowCopy()
		// update qual maps to replace list value with list element
		queryDataCopy.KeyColumnQuals[keyColumn] = qv
		queryDataCopy.Quals[keyColumn].Quals = quals.QualSlice{{
			Column:   keyColumn,
			Operator: "=",
			Value:    qv,
		}}

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
		log.Printf("[TRACE] doList: no matrix item")
		if _, err := rd.callHydrateWithRetries(ctx, queryData, listCall, retryConfig, shouldIgnoreError); err != nil {
			queryData.streamError(err)
		}
	} else {
		t.listForEach(ctx, queryData, listCall)
	}
}

// ListForEach executes the provided list call for each of a set of matrixItem
// enables multi-partition fetching
func (t *Table) listForEach(ctx context.Context, queryData *QueryData, listCall HydrateFunc) {
	log.Printf("[TRACE] listForEach: %v\n", queryData.Matrix)
	var wg sync.WaitGroup
	// NOTE - we use the filtered matrix - which means we may not actually run any hydrate calls
	// if the quals have filtered out all matrix items (e.g. select where region = 'invalid')
	for _, matrixItem := range queryData.filteredMatrix {

		// check whether there is a single equals qual for each matrix item property and if so, check whether
		// the matrix item property values satisfy the conditions
		if !t.matrixItemMeetsQuals(matrixItem, queryData) {
			log.Printf("[INFO] listForEach: matrix item item does not meet quals, %v\n", matrixItem)
			continue
		}

		// create a context with the matrixItem
		fetchContext := context.WithValue(ctx, context_key.MatrixItem, matrixItem)
		wg.Add(1)

		// pass matrixItem into goroutine to avoid async timing issues
		go func(matrixItem map[string]interface{}) {
			defer func() {
				if r := recover(); r != nil {
					queryData.streamError(helpers.ToError(r))
				}
				wg.Done()
			}()
			rd := newRowData(queryData, nil)

			// clone the query data and add the matrix properties to quals
			matrixQueryData := queryData.ShallowCopy()
			matrixQueryData.updateQualsWithMatrixItem(matrixItem)

			retryConfig, shouldIgnoreError := t.buildListConfig()

			log.Printf("[TRACE] callHydrateWithRetries for matrixItem %v, key columns %v", matrixItem, matrixQueryData.KeyColumnQuals)

			_, err := rd.callHydrateWithRetries(fetchContext, matrixQueryData, listCall, retryConfig, shouldIgnoreError)
			if err != nil {
				log.Printf("[TRACE] callHydrateWithRetries returned error %v", err)
				queryData.streamError(err)
			}
		}(matrixItem)
	}
	wg.Wait()
}

func (t *Table) matrixItemMeetsQuals(matrixItem map[string]interface{}, queryData *QueryData) bool {
	// for the purposes of optimisation , assume matrix item properties correspond to column names
	// if this is NOT the case, it will not fail, but this optimisation will not do anything
	for columnName, metadataValue := range matrixItem {
		// is there a single equals qual for this column
		if qualValue, ok := queryData.Quals[columnName]; ok {
			if qualValue.SingleEqualsQual() {
				// get the underlying qual value
				requiredValue := grpc.GetQualValue(qualValue.Quals[0].Value)
				if requiredValue != metadataValue {
					return false
				}
			}
		}
	}
	return true
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
