package redis_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// watchMockServer is a minimal RESP2 server with real WATCH/MULTI/EXEC
// semantics: every write bumps a per-key version, WATCH records versions,
// and EXEC aborts with a nil array when any watched version moved.
type watchMockServer struct {
	ln net.Listener

	mu       sync.Mutex
	values   map[string]string
	versions map[string]uint64
}

func newWatchMockServer(t *testing.T) *watchMockServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &watchMockServer{
		ln:       ln,
		values:   make(map[string]string),
		versions: make(map[string]uint64),
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return srv
}

func (s *watchMockServer) client(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr: s.ln.Addr().String(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func (s *watchMockServer) set(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = val
	s.versions[key]++
}

func (s *watchMockServer) get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[key]
	return v, ok
}

func (s *watchMockServer) serve(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)

	var (
		watched map[string]uint64
		inMulti bool
		queued  [][]string
	)

	reply := func(msg string) bool {
		_, err := conn.Write([]byte(msg))
		return err == nil
	}

	for {
		args, err := readMockCommand(rd)
		if err != nil {
			return
		}
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			if !reply("-ERR unknown command 'HELLO'\r\n") {
				return
			}
		case "CLIENT", "COMMAND":
			if !reply("+OK\r\n") {
				return
			}
		case "PING":
			if !reply("+PONG\r\n") {
				return
			}
		case "WATCH":
			s.mu.Lock()
			if watched == nil {
				watched = make(map[string]uint64)
			}
			for _, key := range args[1:] {
				watched[key] = s.versions[key]
			}
			s.mu.Unlock()
			if !reply("+OK\r\n") {
				return
			}
		case "UNWATCH":
			watched = nil
			if !reply("+OK\r\n") {
				return
			}
		case "MULTI":
			if inMulti {
				if !reply("-ERR MULTI calls can not be nested\r\n") {
					return
				}
				continue
			}
			inMulti = true
			queued = queued[:0]
			if !reply("+OK\r\n") {
				return
			}
		case "DISCARD":
			if !inMulti {
				if !reply("-ERR DISCARD without MULTI\r\n") {
					return
				}
				continue
			}
			inMulti = false
			queued = queued[:0]
			if !reply("+OK\r\n") {
				return
			}
		case "EXEC":
			if !inMulti {
				if !reply("-ERR EXEC without MULTI\r\n") {
					return
				}
				continue
			}
			inMulti = false
			dirty := false
			s.mu.Lock()
			for key, ver := range watched {
				if s.versions[key] != ver {
					dirty = true
					break
				}
			}
			s.mu.Unlock()
			watched = nil
			if dirty {
				queued = queued[:0]
				if !reply("*-1\r\n") {
					return
				}
				continue
			}
			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", len(queued))
			for _, q := range queued {
				b.WriteString(s.apply(q))
			}
			queued = queued[:0]
			if !reply(b.String()) {
				return
			}
		default:
			if inMulti {
				queued = append(queued, args)
				if !reply("+QUEUED\r\n") {
					return
				}
				continue
			}
			if !reply(s.apply(args)) {
				return
			}
		}
	}
}

// apply executes a command immediately and returns its RESP reply.
func (s *watchMockServer) apply(args []string) string {
	switch strings.ToUpper(args[0]) {
	case "SET":
		s.set(args[1], args[2])
		return "+OK\r\n"
	case "GET":
		if v, ok := s.get(args[1]); ok {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		}
		return "$-1\r\n"
	case "DUMP":
		if v, ok := s.get(args[1]); ok {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		}
		return "$-1\r\n"
	case "DEL":
		s.mu.Lock()
		n := 0
		for _, key := range args[1:] {
			if _, ok := s.values[key]; ok {
				delete(s.values, key)
				s.versions[key]++
				n++
			}
		}
		s.mu.Unlock()
		return fmt.Sprintf(":%d\r\n", n)
	default:
		return "+OK\r\n"
	}
}

// readMockCommand parses one RESP2 array-of-bulk-strings request.
func readMockCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("unexpected request line %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		sizeLine, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		sizeLine = strings.TrimSuffix(strings.TrimSuffix(sizeLine, "\n"), "\r")
		if len(sizeLine) == 0 || sizeLine[0] != '$' {
			return nil, fmt.Errorf("unexpected bulk header %q", sizeLine)
		}
		size, err := strconv.Atoi(sizeLine[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func TestTxFailedErrorReportsChangedKeysAndQueuedCommands(t *testing.T) {
	srv := newWatchMockServer(t)
	client := srv.client(t)
	ctx := context.Background()

	srv.set("k1", "a")
	srv.set("k2", "b")
	srv.set("k3", "c")

	var txErr *redis.TxFailedError
	err := client.Watch(ctx, func(tx *redis.Tx) error {
		// A third party modifies one of the watched keys after WATCH.
		srv.set("k2", "changed")

		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "k1", "new1", 0)
			pipe.Set(ctx, "k2", "new2", 0)
			return nil
		})
		return err
	}, "k1", "k2", "k3")

	if err == nil {
		t.Fatal("expected transaction to fail")
	}
	if !errors.Is(err, redis.TxFailedErr) {
		t.Fatalf("expected errors.Is(err, TxFailedErr), got %v", err)
	}
	if !errors.As(err, &txErr) {
		t.Fatalf("expected *redis.TxFailedError, got %T: %v", err, err)
	}
	if len(txErr.ChangedKeys) != 1 || txErr.ChangedKeys[0] != "k2" {
		t.Fatalf("expected ChangedKeys [k2], got %v", txErr.ChangedKeys)
	}
	if len(txErr.WatchedKeys) != 3 {
		t.Fatalf("expected 3 watched keys, got %v", txErr.WatchedKeys)
	}
	if txErr.QueuedCommands != 2 {
		t.Fatalf("expected 2 queued commands, got %d", txErr.QueuedCommands)
	}
	if msg := err.Error(); !strings.Contains(msg, "k2") || !strings.Contains(msg, "2") {
		t.Fatalf("error must name changed keys and queued count together, got %q", msg)
	}

	// The aborted pipeline must not have taken effect.
	if v, _ := srv.get("k1"); v != "a" {
		t.Fatalf("k1 must be unchanged after abort, got %q", v)
	}
	if v, _ := srv.get("k2"); v != "changed" {
		t.Fatalf("k2 must keep the third-party value, got %q", v)
	}

	// The failed transaction must have released the watch: a follow-up
	// transaction on the same keys starts clean and commits.
	err = client.Watch(ctx, func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "k1", "final", 0)
			return nil
		})
		return err
	}, "k1")
	if err != nil {
		t.Fatalf("follow-up transaction must not inherit the conflict: %v", err)
	}
	if v, _ := srv.get("k1"); v != "final" {
		t.Fatalf("expected k1=final, got %q", v)
	}
}

func TestTxFailedErrorNoFalseConflict(t *testing.T) {
	srv := newWatchMockServer(t)
	client := srv.client(t)
	ctx := context.Background()

	srv.set("k1", "a")

	// Touching an unwatched key must not abort the transaction.
	err := client.Watch(ctx, func(tx *redis.Tx) error {
		srv.set("other", "x")
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "k1", "first", 0)
			pipe.Set(ctx, "k1", "second", 0)
			return nil
		})
		return err
	}, "k1")
	if err != nil {
		t.Fatalf("unexpected conflict: %v", err)
	}
	// Commands apply in order; the final read sees the last write.
	if v, _ := srv.get("k1"); v != "second" {
		t.Fatalf("expected k1=second, got %q", v)
	}
}

func TestTxFailedErrorDeletedKeyCountsAsChanged(t *testing.T) {
	srv := newWatchMockServer(t)
	client := srv.client(t)
	ctx := context.Background()

	srv.set("victim", "1")

	var txErr *redis.TxFailedError
	err := client.Watch(ctx, func(tx *redis.Tx) error {
		srv.mu.Lock()
		delete(srv.values, "victim")
		srv.versions["victim"]++
		srv.mu.Unlock()

		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "victim", "2", 0)
			return nil
		})
		return err
	}, "victim")

	if !errors.As(err, &txErr) {
		t.Fatalf("expected *redis.TxFailedError, got %v", err)
	}
	if len(txErr.ChangedKeys) != 1 || txErr.ChangedKeys[0] != "victim" {
		t.Fatalf("deleted key must be reported as changed, got %v", txErr.ChangedKeys)
	}
}

func TestTxUnwatchReleasesWatchBeforeMulti(t *testing.T) {
	srv := newWatchMockServer(t)
	client := srv.client(t)
	ctx := context.Background()

	srv.set("k", "0")

	// Watch, then give up before MULTI: the watch must be released, so a
	// later change cannot abort the next transaction.
	err := client.Watch(ctx, func(tx *redis.Tx) error {
		if err := tx.Unwatch(ctx).Err(); err != nil {
			return err
		}
		srv.set("k", "1")
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "k", "2", 0)
			return nil
		})
		return err
	}, "k")
	if err != nil {
		t.Fatalf("transaction after UNWATCH must succeed: %v", err)
	}
	if v, _ := srv.get("k"); v != "2" {
		t.Fatalf("expected k=2, got %q", v)
	}
}

func TestTxDiscardLeavesNothingForNextTransaction(t *testing.T) {
	srv := newWatchMockServer(t)
	client := srv.client(t)
	ctx := context.Background()

	err := client.Watch(ctx, func(tx *redis.Tx) error {
		cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "k1", "discarded", 0)
			pipe.Discard()
			pipe.Set(ctx, "k2", "kept", 0)
			return nil
		})
		if err != nil {
			return err
		}
		if len(cmds) != 1 {
			t.Fatalf("expected 1 executed command after Discard, got %d", len(cmds))
		}
		return nil
	}, "k1", "k2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := srv.get("k1"); ok {
		t.Fatal("discarded command must not leak into the transaction")
	}
	if v, _ := srv.get("k2"); v != "kept" {
		t.Fatalf("expected k2=kept, got %q", v)
	}
}

func TestTxFailedErrorIsMessageCompatible(t *testing.T) {
	err := &redis.TxFailedError{ChangedKeys: []string{"a", "b"}, QueuedCommands: 3}
	if !errors.Is(err, redis.TxFailedErr) {
		t.Fatal("TxFailedError must match the TxFailedErr sentinel")
	}
	msg := err.Error()
	for _, want := range []string{"redis: transaction failed", "a", "b", "3"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q must contain %q", msg, want)
		}
	}
}
