package scheme

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGracefulShutdown_StopsAcceptingNewConnections checks new connections
// are refused once shutdown starts, even mid-grace-period.
func TestGracefulShutdown_StopsAcceptingNewConnections(t *testing.T) {
	srv, addr, serveDone := startTestServer(t, Config{
		DelayThreshold: 100,
		MaxDelay:       time.Second,
		GracePeriod:    200 * time.Millisecond,
	})

	// Keep a slow request in flight so Shutdown doesn't return before we
	// try dialing a new connection.
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "dial")
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "PAYMENT|500\n")
	require.NoError(t, err, "write")

	wait := triggerShutdownAsync(srv)

	// Wait for the accept loop to actually stop instead of racing a sleep.
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("accept loop did not stop after shutdown started")
	}

	_, err = net.DialTimeout("tcp", addr, 50*time.Millisecond)
	assert.Error(t, err, "expected new connections to be refused once shutdown started")

	wait()
}

// TestGracefulShutdown_CancelsInFlightRequestsAfterGracePeriod checks a
// request still running after the grace period gets "Cancelled".
func TestGracefulShutdown_CancelsInFlightRequestsAfterGracePeriod(t *testing.T) {
	gracePeriod := 150 * time.Millisecond
	srv, addr, _ := startTestServer(t, Config{
		DelayThreshold: 100,
		MaxDelay:       10 * time.Second,
		GracePeriod:    gracePeriod,
	})

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "dial")
	defer conn.Close()

	// 2s simulated delay, far more than the grace period.
	_, err = fmt.Fprintf(conn, "PAYMENT|2000\n")
	require.NoError(t, err, "write")

	time.Sleep(20 * time.Millisecond) // let the server accept it first

	shutdownStart := time.Now()
	wait := triggerShutdownAsync(srv)

	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err, "read")
	elapsed := time.Since(shutdownStart)

	assert.Equal(t, "RESPONSE|REJECTED|Cancelled", strings.TrimSpace(line))
	assert.GreaterOrEqual(t, elapsed, gracePeriod)
	assert.LessOrEqual(t, elapsed, gracePeriod+50*time.Millisecond)

	wait()
}

// TestGracefulShutdown_LetsInFlightRequestsCompleteWithinGracePeriod checks
// a request finishing within the grace period still gets its normal response.
func TestGracefulShutdown_LetsInFlightRequestsCompleteWithinGracePeriod(t *testing.T) {
	srv, addr, _ := startTestServer(t, Config{
		DelayThreshold: 100,
		MaxDelay:       time.Second,
		GracePeriod:    300 * time.Millisecond,
	})

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "dial")
	defer conn.Close()

	// 150ms delay, comfortably within the 300ms grace period.
	_, err = fmt.Fprintf(conn, "PAYMENT|150\n")
	require.NoError(t, err, "write")

	time.Sleep(20 * time.Millisecond) // ensure the request has been accepted
	wait := triggerShutdownAsync(srv)

	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err, "read")
	assert.Equal(t, "RESPONSE|ACCEPTED|Transaction processed", strings.TrimSpace(line))

	wait()
}

// TestGracefulShutdown_DiscardsIdleConnectionsWithoutResponse checks a
// connection that never sent a request is dropped without a response.
func TestGracefulShutdown_DiscardsIdleConnectionsWithoutResponse(t *testing.T) {
	srv, addr, _ := startTestServer(t, Config{
		DelayThreshold: 100,
		MaxDelay:       time.Second,
		GracePeriod:    100 * time.Millisecond,
	})

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "dial")
	defer conn.Close()
	// Intentionally never send a request.

	wait := triggerShutdownAsync(srv)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	assert.Error(t, err, "expected the connection to be closed without a response")
	assert.Zero(t, n)

	wait()
}

// TestGracefulShutdown_ReturnsImmediatelyWhenIdle checks Shutdown doesn't
// wait out the grace period when nothing is in flight.
func TestGracefulShutdown_ReturnsImmediatelyWhenIdle(t *testing.T) {
	srv, _, _ := startTestServer(t, Config{
		DelayThreshold: 100,
		MaxDelay:       time.Second,
		GracePeriod:    time.Second,
	})

	start := time.Now()
	srv.Shutdown()
	assert.LessOrEqual(t, time.Since(start), 50*time.Millisecond)
}
