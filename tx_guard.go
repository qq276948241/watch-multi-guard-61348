package redis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// TxConflictError is returned by TxGuard.Commit when one or more watched keys
// were modified by another client between Watch and Commit. It names every
// watched key that changed and reports how many queued commands were
// discarded without taking effect.
type TxConflictError struct {
	// Keys is the sorted list of watched keys that changed since they were
	// watched (including keys that were deleted).
	Keys []string
	// Queued is the number of commands that were queued in the transaction
	// and discarded.
	Queued int
}

func (e *TxConflictError) Error() string {
	return fmt.Sprintf("redis: transaction failed: watched keys changed: [%s], %d queued command(s) discarded",
		strings.Join(e.Keys, ", "), e.Queued)
}

// IsTxConflict reports whether err is (or wraps) a *TxConflictError and
// returns it.
func IsTxConflict(err error) (*TxConflictError, bool) {
	var conflict *TxConflictError
	if errors.As(err, &conflict) {
		return conflict, true
	}
	return nil, false
}

// guardSnapshot is the client-side snapshot of a watched key taken when the
// key was watched. It is used to name the keys that changed when EXEC aborts,
// which the server does not report.
type guardSnapshot struct {
	dump   string
	exists bool
}

// TxGuard is a watched transaction guard: it watches a set of keys, queues
// commands on the same sticky connection, and commits them atomically with
// MULTI/EXEC. If any watched key changes before Commit, Commit fails with a
// *TxConflictError naming the changed keys and none of the queued commands
// take effect. If nothing changed, every queued command takes effect in
// order and results come back in the order the commands were queued.
//
// The guard, its watch and its pipeline all live on a single dedicated
// connection borrowed from the client. If that connection breaks, the watch
// and every queued command are voided: the guard becomes unusable and nothing
// is replayed or committed on a later connection.
//
// A TxGuard is NOT safe for concurrent use by multiple goroutines.
type TxGuard struct {
	tx *Tx

	cmdable
	statefulCmdable

	mu      sync.Mutex
	watched map[string]guardSnapshot
	pre     []Cmder // queued outside a transaction; never swept into Commit
	queued  []Cmder // queued inside the current transaction
	inTx    bool
	armed   bool // a WATCH is active on the connection
	closed  bool
	broken  bool // the underlying connection was lost
}

// NewTxGuard creates a transaction guard on a dedicated connection and
// watches keys (if any). The caller must Close the guard when done.
//
// With no keys the guard degrades to a plain MULTI/EXEC commit of the queued
// commands, mirroring TxPipelined.
func (c *Client) NewTxGuard(ctx context.Context, keys ...string) (*TxGuard, error) {
	g := &TxGuard{
		tx:      c.newTx(),
		watched: make(map[string]guardSnapshot),
	}
	g.cmdable = g.Process
	g.statefulCmdable = g.Process
	if len(keys) > 0 {
		if err := g.Watch(ctx, keys...); err != nil {
			_ = g.tx.Close(ctx)
			return nil, err
		}
	}
	return g, nil
}

func (g *TxGuard) checkUsable() error {
	switch {
	case g.closed:
		return errors.New("redis: TxGuard is closed")
	case g.broken:
		return errors.New("redis: TxGuard connection was lost; watch and queued commands are voided")
	}
	return nil
}

// Watch adds keys to the watched set and snapshots their current values.
// Watching the same key twice refreshes its snapshot. Watch is rejected while
// a transaction is in progress.
func (g *TxGuard) Watch(ctx context.Context, keys ...string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsable(); err != nil {
		return err
	}
	if g.inTx {
		return errors.New("redis: cannot watch while a transaction is in progress")
	}
	if len(keys) == 0 {
		return nil
	}
	if err := g.tx.Watch(ctx, keys...).Err(); err != nil {
		g.markBrokenLocked(err)
		return err
	}
	for _, key := range keys {
		snap, err := g.snapshotKey(ctx, key)
		if err != nil {
			g.markBrokenLocked(err)
			return err
		}
		g.watched[key] = snap
	}
	g.armed = true
	return nil
}

func (g *TxGuard) snapshotKey(ctx context.Context, key string) (guardSnapshot, error) {
	dump, err := g.tx.Dump(ctx, key).Result()
	if err == Nil {
		return guardSnapshot{exists: false}, nil
	}
	if err != nil {
		return guardSnapshot{}, err
	}
	return guardSnapshot{dump: dump, exists: true}, nil
}

// Unwatch releases all watched keys without committing anything.
func (g *TxGuard) Unwatch(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsable(); err != nil {
		return err
	}
	if g.inTx {
		return errors.New("redis: cannot unwatch while a transaction is in progress; commit or discard first")
	}
	if !g.armed {
		return nil
	}
	if err := g.tx.Unwatch(ctx).Err(); err != nil {
		g.markBrokenLocked(err)
		return err
	}
	g.armed = false
	g.watched = make(map[string]guardSnapshot)
	return nil
}

// Begin starts a transaction. Commands queued after Begin (via the guard's
// cmdable methods or Process) are committed atomically by Commit. Commands
// queued before Begin stay in the pre-transaction buffer and are never swept
// into the transaction. Nesting a second Begin is rejected.
func (g *TxGuard) Begin(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsable(); err != nil {
		return err
	}
	if g.inTx {
		return errors.New("redis: nested transactions are not supported")
	}
	g.inTx = true
	g.queued = nil
	return nil
}

// Process queues cmd. Inside a transaction it joins the transactional
// buffer; outside a transaction it joins the pre-transaction buffer (see
// Flush). The command's result is only set once the buffer is executed.
func (g *TxGuard) Process(ctx context.Context, cmd Cmder) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsable(); err != nil {
		cmd.SetErr(err)
		return err
	}
	if g.inTx {
		g.queued = append(g.queued, cmd)
	} else {
		g.pre = append(g.pre, cmd)
	}
	return nil
}

// Len returns the number of commands queued in the current transaction.
func (g *TxGuard) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.queued)
}

// WatchedKeys returns the sorted list of currently watched keys.
func (g *TxGuard) WatchedKeys() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	keys := make([]string, 0, len(g.watched))
	for key := range g.watched {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Flush executes the commands queued outside a transaction as a plain
// pipeline on the guard's connection and clears the pre-transaction buffer.
// It never touches the transactional buffer.
func (g *TxGuard) Flush(ctx context.Context) ([]Cmder, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsable(); err != nil {
		return nil, err
	}
	if g.inTx {
		return nil, errors.New("redis: cannot flush while a transaction is in progress")
	}
	cmds := g.pre
	g.pre = nil
	if len(cmds) == 0 {
		return nil, nil
	}
	err := g.tx.processPipelineHook(ctx, cmds)
	if err != nil {
		g.markBrokenLocked(err)
		return nil, err
	}
	return cmds, nil
}

// Commit commits the commands queued since Begin. If a watched key changed
// since it was watched, Commit returns a *TxConflictError naming the changed
// keys and the number of discarded commands, and none of the queued commands
// take effect. Otherwise every command takes effect in order and the
// returned commands carry their results in queue order.
//
// Commit always ends the transaction and releases the watch, whether it
// succeeds or fails.
func (g *TxGuard) Commit(ctx context.Context) ([]Cmder, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsable(); err != nil {
		return nil, err
	}
	if !g.inTx {
		return nil, errors.New("redis: no transaction in progress")
	}
	queued := g.queued
	g.queued = nil
	g.inTx = false

	exec := make([]Cmder, 0, len(queued)+2)
	exec = append(exec, NewStatusCmd(ctx, "multi"))
	exec = append(exec, queued...)
	exec = append(exec, NewSliceCmd(ctx, "exec"))

	err := g.tx.processTxPipelineHook(ctx, exec)
	switch {
	case err == nil:
		// EXEC ran and committed; the server discarded the watch.
		g.armed = false
		g.watched = make(map[string]guardSnapshot)
		return queued, nil
	case errors.Is(err, TxFailedErr):
		// EXEC ran and aborted; the server discarded the watch.
		g.armed = false
		changed := g.changedKeysLocked(ctx)
		g.watched = make(map[string]guardSnapshot)
		return nil, &TxConflictError{Keys: changed, Queued: len(queued)}
	case IsExecAbortError(err):
		g.armed = false
		g.watched = make(map[string]guardSnapshot)
		return nil, err
	default:
		// The connection (or something before EXEC) failed. The watch and
		// the queued commands are void; nothing may be replayed later.
		g.markBrokenLocked(err)
		return nil, err
	}
}

// changedKeysLocked diffs the watched-key snapshots against the live values.
// Best effort: keys whose state can no longer be fetched are reported as
// changed so the caller is never told "conflict" without names.
func (g *TxGuard) changedKeysLocked(ctx context.Context) []string {
	changed := make([]string, 0, len(g.watched))
	for key, snap := range g.watched {
		cur, err := g.snapshotKey(ctx, key)
		if err != nil || cur.exists != snap.exists || cur.dump != snap.dump {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

// Discard aborts the current transaction (if any), releases the watch (if
// any) and clears both command buffers. Discarded commands never leak into a
// later transaction. Discard fails when there is neither a transaction in
// progress nor an active watch, so a successful Commit and a successful
// Discard can never both apply to the same transaction.
func (g *TxGuard) Discard(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsable(); err != nil {
		return err
	}
	if !g.inTx && !g.armed {
		return errors.New("redis: no transaction or watch to discard")
	}
	g.inTx = false
	g.queued = nil
	g.pre = nil
	if g.armed {
		if err := g.tx.Unwatch(ctx).Err(); err != nil {
			g.markBrokenLocked(err)
			return err
		}
		g.armed = false
	}
	g.watched = make(map[string]guardSnapshot)
	return nil
}

// Close releases any active watch and returns the connection to the client.
// Uncommitted queued commands are dropped, never executed.
func (g *TxGuard) Close(ctx context.Context) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	armed := g.armed && !g.broken
	g.armed = false
	g.inTx = false
	g.queued = nil
	g.pre = nil
	g.watched = make(map[string]guardSnapshot)
	g.mu.Unlock()

	if armed {
		_ = g.tx.Unwatch(ctx).Err()
	}
	return g.tx.Close(ctx)
}

// markBrokenLocked voids the guard after the underlying connection was lost.
// The server-side watch dies with the connection and the client-side buffers
// are dropped, so a later connection can never observe or replay this
// transaction.
func (g *TxGuard) markBrokenLocked(err error) {
	if err == nil || errors.Is(err, TxFailedErr) || IsExecAbortError(err) {
		return
	}
	if !isBadConn(err, false, g.tx.opt.Addr) {
		return
	}
	g.broken = true
	g.armed = false
	g.inTx = false
	g.queued = nil
	g.pre = nil
	g.watched = make(map[string]guardSnapshot)
}
