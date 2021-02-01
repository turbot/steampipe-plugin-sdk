package transform

import (
	"context"
	"fmt"
	"log"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TransformCall :: a transform function and parameter to invoke it with
type TransformCall struct {
	Transform TransformFunc
	Param     interface{}
}

// Execute :: execute a transform call
func (tr *TransformCall) Execute(ctx context.Context, value interface{}, hydrateItem interface{}, hydrateResults map[string]interface{}, columnName string) (transformedValue interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("transform %s failed with panic %v", helpers.GetFunctionName(tr.Transform), r))
		}
	}()

	// retrieve fetch metadata from the context - this is generally used to store the fetch region
	var fetchMetadata = map[string]interface{}{}
	if contextValue := ctx.Value(context_key.FetchMetadata); contextValue != nil {
		fetchMetadata = contextValue.(map[string]interface{})
	}

	td := &TransformData{
		Param:          tr.Param,
		Value:          value,
		HydrateItem:    hydrateItem,
		HydrateResults: hydrateResults,
		ColumnName:     columnName,
		FetchMetadata:  fetchMetadata,
	}
	transformedValue, err = tr.Transform(ctx, td)
	if err != nil {
		log.Printf("[ERROR] transform %s returned error %v\n", helpers.GetFunctionName(tr.Transform), err)
	}
	return
}
