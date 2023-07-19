package rate_limiter

import (
	"testing"
)

func TestRateLimiter(t *testing.T) {
	//fmt.Printf("x_time_rate")
	//limiter := &MultiLimiter{}
	//limiter.Add(10, 100, nil)
	//limiter.Add(1, 5, KeyMap{"hydrate": "fxn1"})
	//limiter.Add(2, 5, KeyMap{"hydrate": "fxn2"})
	//
	//save := time.Now()
	//
	//var wg sync.WaitGroup
	//makeApiCalls := func(hydrate string) {
	//	for i := 0; i < 50; i++ {
	//		limiter.Wait(context.Background(), KeyMap{"hydrate": hydrate})
	//		fmt.Printf("%s, %d, %v\n", hydrate, i, time.Since(save))
	//	}
	//	wg.Done()
	//}
	//wg.Add(1)
	//go makeApiCalls("fxn1")
	//wg.Add(1)
	//go makeApiCalls("fxn2")
	//wg.Add(1)
	//go makeApiCalls("fxn3")
	//
	//wg.Wait()
}
