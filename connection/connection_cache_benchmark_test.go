package connection

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSetThenGetConsistency verifies that Wait() properly ensures visibility
func TestSetThenGetConsistency(t *testing.T) {
	cache, err := NewConnectionCache("consistency_test", 100*1024*1024)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	ctx := context.Background()

	// Test multiple set/get pairs
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		err := cache.SetWithTTL(ctx, key, value, time.Hour)
		if err != nil {
			t.Fatalf("SetWithTTL failed: %v", err)
		}

		// Immediately get - should be visible due to Wait()
		got, found := cache.Get(ctx, key)
		if !found {
			t.Fatalf("Get failed for key %s: not found", key)
		}
		if got != value {
			t.Fatalf("Get returned wrong value for key %s: got %v, want %v", key, got, value)
		}
	}
}

func BenchmarkSetWithTTL(b *testing.B) {
	cache, err := NewConnectionCache("benchmark_test", 100*1024*1024)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.SetWithTTL(ctx, fmt.Sprintf("key-%d", i), "value", time.Hour)
	}
}

func BenchmarkSetWithTTLThenGet(b *testing.B) {
	cache, err := NewConnectionCache("benchmark_test", 100*1024*1024)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		cache.SetWithTTL(ctx, key, "value", time.Hour)
		// Immediately get to verify visibility
		cache.Get(ctx, key)
	}
}
