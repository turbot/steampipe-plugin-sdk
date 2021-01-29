package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// ListForEach :: execute the provided list call for each of a set of parameterMaps
// intended for use with multi-region list calls
func ListForEach(ctx context.Context, queryData *QueryData, hydrateData *HydrateData, listCall HydrateFunc, parameterMaps []map[string]string) (interface{}, error) {

	log.Printf("[WARN] ListForEach, params: %v", parameterMaps)
	if len(parameterMaps) == 0 {
		return nil, fmt.Errorf("no input parameters passed to ListForEach")
	}
	if len(parameterMaps) == 1 {
		hydrateData.Params = parameterMaps[0]
		return listCall(ctx, queryData, hydrateData)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(parameterMaps))
	var errors []error
	for _, parameterMap := range parameterMaps {
		// clone hydrate data and set params
		hd := hydrateData.Clone()
		hd.Params = parameterMap
		wg.Add(1)

		go func() {
			log.Printf("[WARN] run list with params: %v", parameterMap)
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

// GetForEach :: execute the provided get call for each of a set of parameterMaps
// intended for use with multi-region list calls
func GetForEach(ctx context.Context, queryData *QueryData, hydrateData *HydrateData, getCall HydrateFunc, partitionParamsList []map[string]string) (interface{}, error) {

	log.Printf("[WARN] ListForEach, params: %v", partitionParamsList)
	if len(partitionParamsList) == 0 {
		return nil, fmt.Errorf("no input parameters passed to ExecListForEachParam")
	}
	if len(partitionParamsList) == 1 {
		hydrateData.Params = partitionParamsList[0]
		return getCall(ctx, queryData, hydrateData)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(partitionParamsList))
	var errors []error
	resultsChan := make(chan interface{}, len(partitionParamsList))
	var results []interface{}
	for _, partitionParams := range partitionParamsList {
		// clone hydrate data and set params
		hd := hydrateData.Clone()
		hd.Params = partitionParams
		wg.Add(1)

		go func() {
			log.Printf("[WARN] run list with params: %v", partitionParams)
			result, err := getCall(ctx, queryData, hd)
			if err != nil {
				errorChan <- err
			} else {
				resultsChan <- result
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
		case result := <-resultsChan:
			if result != nil {
				results = append(results, result)
			}
		case <-doneChan:
			err := buildSingleError(errors)
			// if we have more than one result, this means the key is not globally unique - an error
			if len(results) > 1 {
				return nil, fmt.Errorf("get call returned %d results - the key column is not globally unique", len(results))
			}
			var result interface{}
			if len(results) == 1 {
				result = results[0]
			}

			return result, err
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
