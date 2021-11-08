package grpc

import (
	"fmt"
	"math/rand"
	"time"
)

// BuildCallId generates a unique id based on the current time
// this can be passed into plugin calls to assist with tracking parallel calls
func BuildCallId() string {
	return fmt.Sprintf("%d%d", time.Now().Unix(), rand.Intn(1000))
}
