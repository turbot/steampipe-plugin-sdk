package transform

import (
	"context"
	"fmt"
	"log"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/context_key"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TransformCall is a transform function and parameter to invoke it with
type TransformCall struct {
	Transform TransformFunc
	Param     interface{}
}

// Execute function executes a transform call
func (tr *TransformCall) Execute(ctx context.Context, value interface{}, transformData *TransformData) (transformedValue interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = status.Error(codes.Internal, fmt.Sprintf("transform %s failed with panic %v", helpers.GetFunctionName(tr.Transform), r))
		}
	}()

	// retrieve matrix item from the context - this is generally used to store the fetch region
	var matrixItem = map[string]interface{}{}
	if contextValue := ctx.Value(context_key.MatrixItem); contextValue != nil {
		matrixItem = contextValue.(map[string]interface{})
	}

	transformData.Param = tr.Param
	transformData.Value = value
	transformData.MatrixItem = matrixItem

	transformedValue, err = tr.Transform(ctx, transformData)
	if err != nil {
		log.Printf("[ERROR] transform %s returned error %v\n", helpers.GetFunctionName(tr.Transform), err)
	}
	return
}
