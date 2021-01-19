package transform

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
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

	td := &TransformData{
		Param:          tr.Param,
		Value:          value,
		HydrateItem:    hydrateItem,
		HydrateResults: hydrateResults,
		ColumnName:     columnName,
	}
	transformedValue, err = tr.Transform(ctx, td)
	if err != nil {
		log.Printf("[ERROR] transform %s returned error %v\n", helpers.GetFunctionName(tr.Transform), err)
	} else {
		log.Printf("[TRACE] transform returned %v\n", value)
	}
	return
}
