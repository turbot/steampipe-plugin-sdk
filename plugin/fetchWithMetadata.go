package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
)

// ListForEach :: execute the provided list call for each of a set of fetchMetadata
// enables multi-partition fetching
func ListForEach(ctx context.Context, queryData *QueryData, hydrateData *HydrateData, listCall HydrateFunc, fetchMetadataList []map[string]interface{}) (interface{}, error) {

	if len(fetchMetadataList) == 0 {
		return nil, fmt.Errorf("no fetchMetadata passed to ListForEach")
	}
	if len(fetchMetadataList) == 1 {
		log.Printf("[DEBUG] ListForEach, running list for single fetchMetadata: %v", fetchMetadataList[0])
		// create a context with the fetchMetadata
		fetchContext := context.WithValue(ctx, context_key.FetchMetadata, fetchMetadataList[0])
		return listCall(fetchContext, queryData, hydrateData)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(fetchMetadataList))
	var errors []error
	for _, fetchMetadata := range fetchMetadataList {
		log.Printf("[DEBUG] ListForEach, running list for fetchMetadata: %v", fetchMetadata)
		// create a context with the fetchMetadata
		fetchContext := context.WithValue(ctx, context_key.FetchMetadata, fetchMetadata)
		wg.Add(1)

		go func() {
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

			_, err := listCall(fetchContext, queryData, hydrateData)
			if err != nil {
				errorChan <- err
			}
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

// HACK create a type to wrap the get reaults with the fetch metadata
type ItemWithFetchMetadata struct {
	Item          interface{}
	FetchMetadata map[string]interface{}
}

// GetForEach :: execute the provided get call for each of a set of fetchMetadata
// enables multi-partition fetching
func GetForEach(ctx context.Context, queryData *QueryData, hydrateData *HydrateData, getCall HydrateFunc, fetchMetadataList []map[string]interface{}) (interface{}, error) {
	log.Printf("[DEBUG] GetForEach, params: %v\n", fetchMetadataList)
	if len(fetchMetadataList) == 0 {
		return nil, fmt.Errorf("no fetchMetadata passed to GetForEach")
	}
	if len(fetchMetadataList) == 1 {
		log.Printf("[DEBUG] GetForEach, running get for region params: %v\n", fetchMetadataList[0])
		// create a context with the fetchMetadata
		fetchContext := context.WithValue(ctx, context_key.FetchMetadata, fetchMetadataList[0])
		item, err := getCall(fetchContext, queryData, hydrateData)
		// NOTE wrap the item in a ItemWithFetchMetadata struct to allow us to pass back the fetch metadata
		return ItemWithFetchMetadata{item, fetchMetadataList[0]}, err
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(fetchMetadataList))
	var errors []error
	resultsChan := make(chan interface{}, len(fetchMetadataList))
	var results []interface{}
	for _, fetchMetadata := range fetchMetadataList {
		log.Printf("[DEBUG] GetForEach, running get for fetchMetadata: %v\n", fetchMetadata)
		// create a context with the fetch params
		fetchContext := context.WithValue(ctx, context_key.FetchMetadata, fetchMetadata)
		wg.Add(1)

		go func() {
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

			result, err := getCall(fetchContext, queryData, hydrateData)
			if err != nil {
				errorChan <- err
			} else {
				resultsChan <- result
			}
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

			// NOTE wrap the item in a ItemWithFetchMetadata struct to allow us to pass back the fetch metadata
			return ItemWithFetchMetadata{result, fetchMetadataList[0]}, err
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
