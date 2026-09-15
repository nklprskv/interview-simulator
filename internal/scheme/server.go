package scheme

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds runtime configuration (plain struct, no files/env/flags).
type Config struct {
	// Address is the TCP address to listen on.
	Address string
	// GracePeriod is how long Shutdown waits before canceling in-flight requests.
	GracePeriod time.Duration
	// DelayThreshold is the amount above which a processing delay is simulated.
	DelayThreshold int
	// MaxDelay caps the simulated delay.
	MaxDelay time.Duration
}

// DefaultConfig returns the configuration described in the README.
func DefaultConfig() Config {
	return Config{
		Address:        ":8080",
		GracePeriod:    3 * time.Second,
		DelayThreshold: 100,
		MaxDelay:       10 * time.Second,
	}
}

// Server is a TCP scheme simulator.
type Server struct {
	cfg      Config
	listener net.Listener
	wg       sync.WaitGroup // in-flight handleConnection goroutines

	// shutdownCtx is canceled when the grace period expires, so a request
	// mid-delay returns "Cancelled" instead of running to completion.
	shutdownCtx    context.Context
	cancelRequests context.CancelFunc

	// mu guards conns and closing, shared by Accept and Shutdown so a
	// connection can't register (wg.Add) concurrently with wg.Wait.
	mu      sync.Mutex
	closing bool
	conns   map[net.Conn]*atomic.Bool // true while idle (no request in flight)

	shutdownOnce sync.Once
	shutdownDone chan struct{}
}

// NewServer creates a Server that has not started listening yet.
func NewServer(cfg Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:            cfg,
		shutdownCtx:    ctx,
		cancelRequests: cancel,
		conns:          make(map[net.Conn]*atomic.Bool),
		shutdownDone:   make(chan struct{}),
	}
}

// Listen binds the listening socket. It must be called before Serve.
func (s *Server) Listen() error {
	listener, err := net.Listen("tcp", s.cfg.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Address, err)
	}
	s.listener = listener
	return nil
}

// Addr returns the address the server is listening on. Only valid after Listen.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// Serve accepts connections until the listener is closed by Shutdown. It
// returns nil on a graceful listener close, or the error that stopped it.
func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("accept error: %v", err)
			continue
		}

		s.mu.Lock()
		if s.closing {
			// Accepted right as shutdown began: treat as never "accepted".
			s.mu.Unlock()
			conn.Close()
			continue
		}
		s.wg.Add(1)
		s.mu.Unlock()

		go s.handleConnection(conn)
	}
}

// Shutdown stops new connections and waits up to cfg.GracePeriod for
// in-flight requests. After that, running requests get "Cancelled" and idle
// connections (no request accepted yet) are dropped without a response.
// Safe to call more than once or concurrently.
func (s *Server) Shutdown() {
	s.shutdownOnce.Do(func() {
		s.doShutdown()
		close(s.shutdownDone)
	})
	<-s.shutdownDone
}

func (s *Server) doShutdown() {
	s.mu.Lock()
	s.closing = true
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Unlock()

	allDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(allDone)
	}()

	timer := time.NewTimer(s.cfg.GracePeriod)
	defer timer.Stop()

	select {
	case <-allDone:
		return // everything finished within the grace period
	case <-timer.C:
	}

	// Grace period expired: cancel in-flight delays, drop idle connections.
	s.cancelRequests()
	s.closeIdleConnections()

	<-allDone
}

func (s *Server) closeIdleConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for conn, idle := range s.conns {
		if idle.Load() {
			conn.Close()
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	idle := &atomic.Bool{}
	idle.Store(true)

	s.mu.Lock()
	s.conns[conn] = idle
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		idle.Store(false) // request accepted, now in-flight

		response := s.handleRequest(scanner.Text())

		if _, err := fmt.Fprintf(conn, "%s\n", response); err != nil {
			return
		}

		if s.shutdownCtx.Err() != nil {
			// Grace period already over; closeIdleConnections only sweeps
			// once, so don't wait on another request that may never come.
			return
		}

		idle.Store(true)
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("connection read error: %v", err)
	}
}

// handleRequest parses and processes one request line, returning the
// response line (without trailing newline).
func (s *Server) handleRequest(line string) string {
	parts := strings.Split(line, "|")
	if len(parts) != 2 || parts[0] != requestPrefix {
		return formatResponse(statusRejected, reasonInvalidRequest)
	}

	amount, err := strconv.Atoi(parts[1])
	if err != nil || amount <= 0 {
		return formatResponse(statusRejected, reasonInvalidAmount)
	}

	if amount <= s.cfg.DelayThreshold {
		return formatResponse(statusAccepted, reasonProcessed)
	}

	delay := min(time.Duration(amount)*time.Millisecond, s.cfg.MaxDelay)

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return formatResponse(statusAccepted, reasonProcessed)
	case <-s.shutdownCtx.Done():
		return formatResponse(statusRejected, reasonCancelled)
	}
}
