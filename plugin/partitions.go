package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// ListForPartitions :: execute the provided list call for a set of partitions (e.g. region).
// The partition is define by a parameter map containing required partition details (e.g. region name)
func ListForPartitions(ctx context.Context, queryData *QueryData, hydrateData *HydrateData, listCall HydrateFunc, partitionParamsList []map[string]string) (interface{}, error) {

	log.Printf("[WARN] ListForPartitions, params: %v", partitionParamsList)
	if len(partitionParamsList) == 0 {
		return nil, fmt.Errorf("no input parameters passed to ExecListForEachParam")
	}
	if len(partitionParamsList) == 1 {
		hydrateData.Params = partitionParamsList[0]
		return listCall(ctx, queryData, hydrateData)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(partitionParamsList))
	var errors []error
	for _, partitionParams := range partitionParamsList {
		// clone hydrate data and set params
		hd := hydrateData.Clone()
		hd.Params = partitionParams
		wg.Add(1)

		go func() {
			log.Printf("[WARN] run list with params: %v", partitionParams)
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

// GetForPartitions :: execute the provided get call for a set of partitions (e.g. region).
// The partition is define by a parameter map containing required partition details (e.g. region name)
func GetForPartitions(ctx context.Context, queryData *QueryData, hydrateData *HydrateData, listCall HydrateFunc, partitionParamsList []map[string]string) (interface{}, error) {

	log.Printf("[WARN] ListForPartitions, params: %v", partitionParamsList)
	if len(partitionParamsList) == 0 {
		return nil, fmt.Errorf("no input parameters passed to ExecListForEachParam")
	}
	if len(partitionParamsList) == 1 {
		hydrateData.Params = partitionParamsList[0]
		return listCall(ctx, queryData, hydrateData)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(partitionParamsList))
	var errors []error
	for _, partitionParams := range partitionParamsList {
		// clone hydrate data and set params
		hd := hydrateData.Clone()
		hd.Params = partitionParams
		wg.Add(1)

		go func() {
			log.Printf("[WARN] run list with params: %v", partitionParams)
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
