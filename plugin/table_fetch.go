package plugin

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"

	"github.com/turbot/steampipe-plugin-sdk/grpc"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
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
		return t.executeGetCall(ctx, queryData)
	} else {
		if t.List == nil {
			log.Printf("[WARN] query is not a get call, but no list call is defined, quals: %v", grpc.QualMapToString(queryData.QueryContext.Quals))
			panic("query is not a get call, but no list call is defined")
		}
		logging.LogTime("executeListCall")
		go t.executeListCall(ctx, queryData)
	}
	return nil
}

func (t *Table) executeGetCall(ctx context.Context, queryData *QueryData) (err error) {
	logger := t.Plugin.Logger
	// verify we have the necessary quals
	if queryData.KeyColumnQuals == nil {
		return status.Error(codes.Internal, fmt.Sprintf("'Get' call requires an '=' qual for %s", t.Get.KeyColumns.ToString()))
	}

	// deprecated - ItemFromKey is no longer recommended or required
	if t.Get.ItemFromKey != nil {
		t.executeLegacyGetCall(ctx, queryData)
		return nil
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
			logger.Trace("executeGetCall - single qual, qual value is a list - executing get for each qual value item", "qualValueList", qualValueList)
			// we will mutate queryData.KeyColumnQuals to replace the list value with a single qual value
			for _, qv := range qualValueList.Values {
				// mutate KeyColumnQuals
				queryData.KeyColumnQuals[keyColumn] = qv
				// call doGet passing nil hydrate item
				if err := t.doGet(ctx, queryData, nil); err != nil {
					return err
				}
			}
			// and we are done
			return
		}
	}

	// so there is NOT a list of qual values, just call get once
	// call doGet passing nil hydrate item
	return t.doGet(ctx, queryData, nil)
}

// t.Get.ItemFromKey is deprectaed. If this table has this property set, run legacy get
func (t *Table) executeLegacyGetCall(ctx context.Context, queryData *QueryData) {
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
		return
	}

	// there may be more than one hydrate item - loop over them
	for _, hydrateItem := range hydrateInput {
		if err := t.doGet(ctx, queryData, hydrateItem); err != nil {
			queryData.streamError(err)
			break
		}
	}
}

func (t *Table) doGet(ctx context.Context, queryData *QueryData, hydrateItem interface{}) (err error) {
	hydrateKey := helpers.GetFunctionName(t.Get.Hydrate)
	defer func() {
		if p := recover(); p != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("hydrate call %s failed with panic %v", hydrateKey, p))
		}
		logging.LogTime(hydrateKey + " end")
	}()
	logging.LogTime(hydrateKey + " start")

	// build rowData item, passing the hydrate item and use to invoke the 'get' hydrate call(s)
	// NOTE the hydrate item is only needed for legacy get calls
	rd := newRowData(queryData, hydrateItem)
	var getItem interface{}

	if len(t.FetchMetadata) == 0 {
		// just invoke SafeGet()
		getItem, err = t.SafeGet()(ctx, queryData, &HydrateData{})
	} else {
		// the table has fetch metadata - we will invoke get for each fetch metadata item
		getItem, err = t.getForEach(ctx, queryData, rd)
	}

	if err != nil {
		log.Printf("[WARN] hydrate call returned error %v\n", err)
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

// getForEach :: execute the provided get call for each of a set of fetchMetadata
// enables multi-partition fetching
func (t *Table) getForEach(ctx context.Context, queryData *QueryData, rd *RowData) (interface{}, error) {
	getCall := t.SafeGet()

	log.Printf("[DEBUG] getForEach, fetchMetadata list: %v\n", t.FetchMetadata)

	var wg sync.WaitGroup
	errorChan := make(chan error, len(t.FetchMetadata))
	var errors []error
	// define type to stream down results channel - package the item and the fetch metadata
	// this will for example allow us to determine which region contains the resulting get item
	type resultWithMetadata struct {
		item          interface{}
		fetchMetadata map[string]interface{}
	}
	resultChan := make(chan *resultWithMetadata, len(t.FetchMetadata))
	var results []*resultWithMetadata

	for _, fetchMetadata := range t.FetchMetadata {
		// increment our own wait group
		wg.Add(1)

		go func(fetchMetadata map[string]interface{}) {
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
			// create a context with the fetch metadata
			fetchContext := context.WithValue(ctx, context_key.FetchMetadata, fetchMetadata)

			item, err := getCall(fetchContext, queryData, &HydrateData{})
			if err != nil {
				errorChan <- err
			} else if item != nil {
				// stream the get item AND the fetch metadata
				resultChan <- &resultWithMetadata{item, fetchMetadata}
			}
		}(fetchMetadata) // pass fetchMetadata into goroutine to avoid async timing issues
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
				// set the fetch metadata on the row data
				rd.fetchMetadata = results[0].fetchMetadata
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

// execute the list call in a goroutine
func (t *Table) executeListCall(ctx context.Context, queryData *QueryData) {
	defer func() {
		if r := recover(); r != nil {
			queryData.streamError(status.Error(codes.Internal, fmt.Sprintf("list call %s failed with panic %v", helpers.GetFunctionName(t.List.Hydrate), r)))
		}
	}()

	// verify we have the necessary quals
	if t.List.KeyColumns != nil && queryData.KeyColumnQuals == nil {
		queryData.streamError(status.Error(codes.Internal, fmt.Sprintf("'List' call requires an '=' qual for %s", t.List.KeyColumns.ToString())))
	}

	// if list key columns were specified, verify we have the necessary quals
	if queryData.KeyColumnQuals == nil && t.List.KeyColumns != nil {
		panic("we do not have the quals for a list")
	}

	// invoke list call - hydrateResults is nil as list call does not use it (it must comply with HydrateFunc signature)
	listCall := t.List.Hydrate
	// if there is a parent hydrate function, call that
	// - the child 'Hydrate' function will be called by QueryData.StreamListIte,
	if t.List.ParentHydrate != nil {
		listCall = t.List.ParentHydrate
	}

	if len(t.FetchMetadata) == 0 {
		if _, err := listCall(ctx, queryData, &HydrateData{}); err != nil {
			queryData.streamError(err)
		}
	} else if len(t.FetchMetadata) == 1 {
		log.Printf("[DEBUG] running list for single fetchMetadata: %v", t.FetchMetadata[0])
		// create a context with the fetchMetadata
		fetchContext := context.WithValue(ctx, context_key.FetchMetadata, t.FetchMetadata[0])
		if _, err := listCall(fetchContext, queryData, &HydrateData{}); err != nil {
			queryData.streamError(err)
		}
	} else {
		t.listForEach(ctx, queryData, listCall)
	}

	// list call will return when it has streamed all items so close rowDataChan
	queryData.fetchComplete()

}

// ListForEach :: execute the provided list call for each of a set of fetchMetadata
// enables multi-partition fetching
func (t *Table) listForEach(ctx context.Context, queryData *QueryData, listCall HydrateFunc) {
	var wg sync.WaitGroup
	for _, fetchMetadata := range t.FetchMetadata {
		log.Printf("[DEBUG] ListForEach, running list for fetchMetadata: %v", fetchMetadata)
		// create a context with the fetchMetadata
		fetchContext := context.WithValue(ctx, context_key.FetchMetadata, fetchMetadata)
		wg.Add(1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						queryData.streamError(err)
					} else {
						queryData.streamError(fmt.Errorf("%v", r))
					}
				}
				wg.Done()
			}()

			_, err := listCall(fetchContext, queryData, &HydrateData{})
			if err != nil {
				queryData.streamError(err)
			}
		}()
	}

	wg.Wait()
}
