package grpc

import (
	"fmt"
	"math/rand"
	"time"
)

// BuildCallId generates a unique id based on the current time
// this can be passed into plugin calls to assist with tracking parallel calls
func BuildCallId(connectionName string) string {
	// include the connection name in the call ID
	//- it is used to identify calls to the shared cache service so there is a chance of callId clash
	return fmt.Sprintf("%s-%d%d", connectionName, time.Now().Unix(), rand.Intn(1000))
}
