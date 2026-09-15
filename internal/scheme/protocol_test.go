package scheme

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestHandleRequest exercises parsing and validation without a real TCP connection.
func TestHandleRequest(t *testing.T) {
	srv := NewServer(Config{
		DelayThreshold: 100,
		MaxDelay:       50 * time.Millisecond,
	})

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"valid request, no delay", "PAYMENT|100", formatResponse(statusAccepted, reasonProcessed)},
		{"missing separator", "PAYMENT100", formatResponse(statusRejected, reasonInvalidRequest)},
		{"wrong prefix", "TRANSFER|100", formatResponse(statusRejected, reasonInvalidRequest)},
		{"too many segments", "PAYMENT|100|EXTRA", formatResponse(statusRejected, reasonInvalidRequest)},
		{"empty request", "", formatResponse(statusRejected, reasonInvalidRequest)},
		{"non-numeric amount", "PAYMENT|abc", formatResponse(statusRejected, reasonInvalidAmount)},
		{"zero amount", "PAYMENT|0", formatResponse(statusRejected, reasonInvalidAmount)},
		{"negative amount", "PAYMENT|-5", formatResponse(statusRejected, reasonInvalidAmount)},
		{"decimal amount", "PAYMENT|10.5", formatResponse(statusRejected, reasonInvalidAmount)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, srv.handleRequest(tt.input))
		})
	}
}

func TestHandleRequest_DelayCappedAtMaxDelay(t *testing.T) {
	srv := NewServer(Config{DelayThreshold: 100, MaxDelay: 50 * time.Millisecond})

	start := time.Now()
	got := srv.handleRequest("PAYMENT|1000000")
	elapsed := time.Since(start)

	assert.Equal(t, formatResponse(statusAccepted, reasonProcessed), got)
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond) // capped at MaxDelay
	assert.LessOrEqual(t, elapsed, 100*time.Millisecond)
}

func TestHandleRequest_CancelledOnceShutdownRequested(t *testing.T) {
	srv := NewServer(Config{DelayThreshold: 100, MaxDelay: 10 * time.Second})
	srv.cancelRequests() // simulate the grace period having already expired

	start := time.Now()
	got := srv.handleRequest("PAYMENT|5000")
	elapsed := time.Since(start)

	assert.Equal(t, formatResponse(statusRejected, reasonCancelled), got)
	assert.LessOrEqual(t, elapsed, 20*time.Millisecond, "expected immediate cancellation")
}
