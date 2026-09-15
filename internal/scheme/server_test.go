package scheme

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTestServer starts a Server on an ephemeral port and shuts it down on
// cleanup. serveDone closes once the accept loop has actually returned.
func startTestServer(t *testing.T, cfg Config) (srv *Server, addr string, serveDone <-chan struct{}) {
	t.Helper()

	if cfg.Address == "" {
		cfg.Address = "127.0.0.1:0"
	}

	srv = NewServer(cfg)
	require.NoError(t, srv.Listen(), "failed to listen")

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, srv.Serve(), "serve")
	}()

	t.Cleanup(func() {
		srv.Shutdown()
		<-done
	})

	return srv, srv.Addr().String(), done
}

// triggerShutdownAsync runs Shutdown in the background and returns a func
// that waits for it to finish. Callers must call it before the test returns
// so the goroutine is joined.
func triggerShutdownAsync(srv *Server) (wait func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.Shutdown()
	}()
	return func() { <-done }
}

// sendRequest opens a connection, sends one request, and returns the
// trimmed response and how long it took.
func sendRequest(t *testing.T, addr, request string) (response string, elapsed time.Duration, err error) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return "", 0, err
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "%s\n", request); err != nil {
		return "", 0, err
	}

	start := time.Now()
	line, err := bufio.NewReader(conn).ReadString('\n')
	elapsed = time.Since(start)
	if err != nil {
		return "", elapsed, err
	}

	return strings.TrimSpace(line), elapsed, nil
}

func TestSchemeSimulator_Protocol(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{
		DelayThreshold: 100,
		MaxDelay:       200 * time.Millisecond, // keep the "capped delay" case fast to test
		GracePeriod:    time.Second,
	})

	tests := []struct {
		name           string
		input          string
		expectedOutput string
		minDuration    time.Duration
		maxDuration    time.Duration
	}{
		{
			name:           "Valid request, no delay",
			input:          "PAYMENT|10",
			expectedOutput: "RESPONSE|ACCEPTED|Transaction processed",
			maxDuration:    50 * time.Millisecond,
		},
		{
			name:           "Valid request with delay",
			input:          "PAYMENT|101",
			expectedOutput: "RESPONSE|ACCEPTED|Transaction processed",
			minDuration:    101 * time.Millisecond,
			maxDuration:    151 * time.Millisecond,
		},
		{
			name:           "Invalid request format",
			input:          "INVALID|100",
			expectedOutput: "RESPONSE|REJECTED|Invalid request",
			maxDuration:    10 * time.Millisecond,
		},
		{
			name:           "Invalid amount",
			input:          "PAYMENT|abc",
			expectedOutput: "RESPONSE|REJECTED|Invalid amount",
			maxDuration:    10 * time.Millisecond,
		},
		{
			name:           "Non-positive amount",
			input:          "PAYMENT|0",
			expectedOutput: "RESPONSE|REJECTED|Invalid amount",
			maxDuration:    10 * time.Millisecond,
		},
		{
			name:           "Large amount, delay capped at MaxDelay",
			input:          "PAYMENT|20000",
			expectedOutput: "RESPONSE|ACCEPTED|Transaction processed",
			minDuration:    200 * time.Millisecond,
			maxDuration:    250 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, duration, err := sendRequest(t, addr, tt.input)
			require.NoError(t, err, "sendRequest")

			assert.Equal(t, tt.expectedOutput, response, "unexpected response")
			if tt.minDuration > 0 {
				assert.GreaterOrEqual(t, duration, tt.minDuration, "response arrived earlier than expected")
			}
			if tt.maxDuration > 0 {
				assert.LessOrEqual(t, duration, tt.maxDuration, "response arrived later than expected")
			}
		})
	}
}

// TestSchemeSimulator_ConnectionReuse checks a connection can be reused for
// several sequential requests.
func TestSchemeSimulator_ConnectionReuse(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{DelayThreshold: 100, MaxDelay: time.Second, GracePeriod: time.Second})

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "dial")
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for i := range 3 {
		_, err := fmt.Fprintf(conn, "PAYMENT|10\n")
		require.NoErrorf(t, err, "request %d: write", i)

		line, err := reader.ReadString('\n')
		require.NoErrorf(t, err, "request %d: read", i)

		assert.Equalf(t, "RESPONSE|ACCEPTED|Transaction processed", strings.TrimSpace(line), "request %d", i)
	}
}

// TestSchemeSimulator_OversizedRequestIsClosedWithoutResponse checks a
// request too large to buffer safely (protocol's "Other" rule) closes the
// connection without a response.
func TestSchemeSimulator_OversizedRequestIsClosedWithoutResponse(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{DelayThreshold: 100, MaxDelay: time.Second, GracePeriod: time.Second})

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "dial")
	defer conn.Close()

	oversized := "PAYMENT|" + strings.Repeat("9", 100*1024) + "\n" // > bufio.MaxScanTokenSize (64KB)
	_, err = conn.Write([]byte(oversized))
	require.NoError(t, err, "write")

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	assert.Error(t, err, "expected the connection to be closed without a response")
	assert.Zero(t, n)
}

// TestSchemeSimulator_ConcurrentConnectionsDoNotBlockEachOther checks a slow
// request doesn't delay a fast one on another connection.
func TestSchemeSimulator_ConcurrentConnectionsDoNotBlockEachOther(t *testing.T) {
	_, addr, _ := startTestServer(t, Config{DelayThreshold: 100, MaxDelay: time.Second, GracePeriod: time.Second})

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _, err := sendRequest(t, addr, "PAYMENT|300")
		assert.NoError(t, err, "slow request")
	})

	time.Sleep(20 * time.Millisecond) // let the slow request start first

	_, fastDuration, err := sendRequest(t, addr, "PAYMENT|10")
	require.NoError(t, err, "fast request")
	assert.LessOrEqual(t, fastDuration, 50*time.Millisecond, "fast request appears blocked by the slow one")

	wg.Wait()
}
