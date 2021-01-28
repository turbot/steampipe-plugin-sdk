package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"

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
func (t *Table) fetchItems(ctx context.Context, d *QueryData) {
	log.Println("[TRACE] fetchItems")
	// if the query contains a single 'equals' constrains for all key columns, then call the 'get' function
	if d.FetchType == fetchTypeGet && t.Get != nil {
		logging.LogTime("executeGetCall")
		t.executeGetCall(ctx, d)
	} else {
		if t.List == nil {
			log.Printf("[WARN] query is not a get call, but no list call is defined, quals: %v", grpc.QualMapToString(d.QueryContext.Quals))
			panic(" query is not a get call, but no list call is defined")
		}
		logging.LogTime("executeListCall")
		go t.executeListCall(ctx, d)
	}
}

func (t *Table) executeGetCall(ctx context.Context, d *QueryData) {
	log.Println("[TRACE] executeGetCall")

	// verify we have the necessary quals
	if d.KeyColumnQuals == nil {
		d.streamError(status.Error(codes.Internal, fmt.Sprintf("'Get' call requires an '=' qual for %s", t.Get.KeyColumns.ToString())))
	}

	// deprecated - ItemFromKey is no longer recommended or required
	if t.Get.ItemFromKey != nil {
		t.executeLegacyGetCall(ctx, d)
		return
	}

	defer func() {
		// we can now close the item chan
		d.fetchComplete()
		if r := recover(); r != nil {
			d.streamError(status.Error(codes.Internal, fmt.Sprintf("get call %s failed with panic %v", helpers.GetFunctionName(t.Get.Hydrate), r)))
		}
	}()

	// d.KeyColumnQuals is a map of column to qual value
	// NOTE: if there is a SINGLE key column, the qual value may be a list of values
	// in this case we call get for each value
	if keyColumn := t.Get.KeyColumns.Single; keyColumn != "" {
		if qualValueList := d.KeyColumnQuals[keyColumn].GetListValue(); qualValueList != nil {
			// we will mutate d.KeyColumnQuals to replace the list value with a single qual value
			for _, qv := range qualValueList.Values {
				// mutate KeyColumnQuals
				d.KeyColumnQuals[keyColumn] = qv
				// call doGet passing nil hydrate item
				if err := t.doGet(ctx, d, nil); err != nil {
					d.streamError(err)
					break
				}
			}
			// and we are done
			return
		}
	}

	// so there is NOT a list of qual values, just call get once
	// call doGet passing nil hydrate item
	if err := t.doGet(ctx, d, nil); err != nil {
		d.streamError(err)
	}
}

// t.Get.ItemFromKey is deprectaed. If this table has this property set, run legacy get
func (t *Table) executeLegacyGetCall(ctx context.Context, d *QueryData) {
	log.Printf("[DEBUG] executeLegacyGetCall\n")
	defer func() {
		// we can now close the item chan
		d.fetchComplete()

		if r := recover(); r != nil {
			d.streamError(status.Error(codes.Internal, fmt.Sprintf("get call %s failed with panic %v", helpers.GetFunctionName(t.Get.Hydrate), r)))
		}
	}()
	log.Println("[TRACE] executeGetCall")

	// build the hydrate input by calling  t.Get.ItemFromKey
	hydrateInput, err := t.buildHydrateInputForGetCall(ctx, d)
	if err != nil {
		d.streamError(err)
		return
	}

	// there may be more than one hydrate item - loop over them
	for _, hydrateItem := range hydrateInput {
		if err := t.doGet(ctx, d, hydrateItem); err != nil {
			d.streamError(err)
			break
		}
	}
}

func (t *Table) doGet(ctx context.Context, d *QueryData, hydrateItem interface{}) error {
	// build rowData item, passing the hydrate item and use to invoke the 'get' hydrate call
	rd := newRowData(d, hydrateItem)
	// we need to get the hydrate function name before wrapping it in SafeGet()
	hydrateKey := helpers.GetFunctionName(t.Get.Hydrate)

	log.Printf("[DEBUG] calling 'get' hydrate function '%s' \n", hydrateKey)

	// call the get function defined by the table - SafeGet wraps the get call in the not found handler defined by the plugin
	rd.wg.Add(1)
	hydrateData, err := rd.callHydrate(ctx, d, t.SafeGet(), hydrateKey)
	if err != nil {
		log.Printf("[WARN] hydrate call returned error %v\n", err)
		return err
	}
	// if there is no error and the hydrateData is nil,we assume the item does not exist
	if hydrateData != nil {
		// set rd.Item to the result of the Get hydrate call - this will be passed through to all other hydrate calls
		rd.Item = hydrateData
		log.Println("[TRACE] got item")
		d.rowDataChan <- rd
	}
	return nil
}

// use the quals to build one call to populate one or more base hydrate item from the quals
// NOTE: if there is a list of quals for the key column then we create a hydrate item for each
// - this is handle `in` clauses
func (t *Table) buildHydrateInputForGetCall(ctx context.Context, d *QueryData) ([]interface{}, error) {
	// first call ItemFromKey

	// NOTE: if there qual value is actually a list of values, we call ItemFromKey for each qual
	// this is only possible for single key column tables (as otherwise we would not have identified this as a get call)
	if keyColumn := t.Get.KeyColumns.Single; keyColumn != "" {
		if qualValueList := d.KeyColumnQuals[keyColumn].GetListValue(); qualValueList != nil {
			return t.buildHydrateInputForMultiQualValueGetCall(ctx, d)
		}
	}

	hydrateInputItem, err := t.Get.ItemFromKey(ctx, d, &HydrateData{})
	if err != nil {
		return nil, err
	}
	log.Printf("[TRACE] built hydrate input for get call; %v", hydrateInputItem)

	return []interface{}{hydrateInputItem}, nil
}

func (t *Table) buildHydrateInputForMultiQualValueGetCall(ctx context.Context, d *QueryData) ([]interface{}, error) {
	keyColumn := t.Get.KeyColumns.Single
	qualValueList := d.KeyColumnQuals[keyColumn].GetListValue()
	log.Println("[DEBUG] table has single key, key qual has multiple values")

	// NOTE: store the keyColumnQuals as we will mutate the query data for the calls to ItemFromKey
	keyColumnQuals := d.KeyColumnQuals

	var hydrateInput []interface{}
	for _, qualValue := range qualValueList.Values {
		// build a new map for KeyColumnQuals and mutate the query data
		d.KeyColumnQuals = map[string]*proto.QualValue{
			keyColumn: qualValue,
		}
		hydrateInputItem, err := t.Get.ItemFromKey(ctx, d, &HydrateData{})
		if err != nil {
			return nil, err
		}
		log.Printf("[TRACE] built hydrate input for get call")
		hydrateInput = append(hydrateInput, hydrateInputItem)
	}

	// now revert keyColumnQuals on the query data for good manners
	d.KeyColumnQuals = keyColumnQuals

	return hydrateInput, nil
}

// execute the list call in a goroutine
func (t *Table) executeListCall(ctx context.Context, d *QueryData) {
	defer func() {
		if r := recover(); r != nil {
			d.streamError(status.Error(codes.Internal, fmt.Sprintf("list call %s failed with panic %v", helpers.GetFunctionName(t.List.Hydrate), r)))
		}
	}()

	// verify we have the necessary quals
	if t.List.KeyColumns != nil && d.KeyColumnQuals == nil {
		d.streamError(status.Error(codes.Internal, fmt.Sprintf("'List' call requires an '=' qual for %s", t.List.KeyColumns.ToString())))
	}

	// if list key columns were specified, verify we have the necessary quals
	if d.KeyColumnQuals == nil && t.List.KeyColumns != nil {
		panic("we do not have the quals for a list")
	}

	// invoke list call - hydrateResults is nil as list call does not use it (it must comply with HydrateFunc signature)
	listCall := t.List.Hydrate
	// if there is a parent hydrate function, call that
	// - the child 'Hydrate' function will be called by QueryData.StreamListIte,
	if t.List.ParentHydrate != nil {
		listCall = t.List.ParentHydrate
	}
	if _, err := listCall(ctx, d, &HydrateData{}); err != nil {
		d.streamError(err)
	}
	// list call will return when it has streamed all items so close rowDataChan
	d.fetchComplete()
}

func ExecListForParams(ctx context.Context, queryData *QueryData, hydrateData *HydrateData, listCall HydrateFunc, listParams []map[string]string) (interface{}, error) {
	if len(listParams) == 0 {
		return nil, fmt.Errorf("no input parameters passed to ExecListForEachParam")
	}
	if len(listParams) == 1 {
		hydrateData.Params = listParams[0]
		return listCall(ctx, queryData, hydrateData)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(listParams))
	var errors []error
	for _, params := range listParams {
		// clone hydrate data and set params
		hd := hydrateData.Clone()
		hd.Params = params
		wg.Add(1)

		go func() {
			_, err := listCall(ctx, queryData, hd)
			if err != nil {
				errorChan <- err
			}
			wg.Done()
		}()
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
			errors = append(errors, err)
		case <-doneChan:
			err := buildSingleError(errors)
			return nil, err
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
	errString := ""
	for _, err := range errors {
		errString += fmt.Sprintf("\n%s", err.Error())
	}
	return fmt.Errorf("%d list calls returned errors: %s", len(errors), errString)
}
