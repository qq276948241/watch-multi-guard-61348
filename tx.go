package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9/internal/proto"
)

// TxFailedErr transaction redis failed.
const TxFailedErr = proto.RedisError("redis: transaction failed")

// TxFailedError is returned by Tx.TxPipeline (and Tx.TxPipelined) when EXEC
// aborts because a watched key was modified. It reports which watched keys
// changed and how many pipelined commands were discarded with the
// transaction, so callers can log or inspect the conflict instead of only
// knowing that one happened.
//
// It matches the TxFailedErr sentinel, so both
// errors.Is(err, redis.TxFailedErr) and errors.As(err, &txErr) work.
type TxFailedError struct {
	// ChangedKeys lists the watched keys that were modified between WATCH
	// and EXEC (including keys that were deleted). When the client could
	// not determine the exact keys it falls back to all watched keys.
	ChangedKeys []string
	// WatchedKeys lists every key watched on the transaction connection.
	WatchedKeys []string
	// QueuedCommands is the number of pipelined commands that were
	// discarded when the transaction aborted.
	QueuedCommands int
}

func (e *TxFailedError) Error() string {
	if len(e.ChangedKeys) == 0 {
		return fmt.Sprintf("redis: transaction failed: %d queued command(s) discarded",
			e.QueuedCommands)
	}
	return fmt.Sprintf("redis: transaction failed: watched keys changed: [%s], %d queued command(s) discarded",
		strings.Join(e.ChangedKeys, " "), e.QueuedCommands)
}

// Is reports TxFailedError as a match for the TxFailedErr sentinel.
func (e *TxFailedError) Is(target error) bool {
	return target == TxFailedErr
}

// Unwrap exposes the TxFailedErr sentinel for errors.Is/errors.As chains.
func (e *TxFailedError) Unwrap() error {
	return TxFailedErr
}

// watchFingerprintMissing marks a watched key that did not exist at WATCH
// time. A key that later appears (or disappears) counts as changed.
const watchFingerprintMissing = "\x00redis:tx:watch:missing"

// Tx implements Redis transactions as described in
// https://redis.io/docs/latest/develop/using-commands/transactions. It's NOT safe for concurrent use
// by multiple goroutines, because Exec resets list of watched keys.
//
// If you don't need WATCH, use Pipeline instead.
type Tx struct {
	baseClient
	cmdable
	statefulCmdable

	// watchArmed reports whether a WATCH issued through Tx.Watch may still be
	// active on the connection. It is set on a successful WATCH and cleared once
	// the watched keys are discarded server-side: on UNWATCH (Tx.Unwatch) and on
	// EXEC, including an aborted EXEC (the TxPipeline closure). Close uses it to
	// skip an otherwise redundant UNWATCH round trip.
	//
	// Only Tx.Watch, Tx.Unwatch and the TxPipeline EXEC closure maintain this
	// flag. go-redis never issues a standalone DISCARD (Pipeline.Discard is a
	// client-side buffer reset, not a server command). Issuing a WATCH directly
	// via Process bypasses tracking, so Close would not release it and the watch
	// would leak onto the pooled connection; that is the one case where skipping
	// UNWATCH is unsafe, and using raw Process for WATCH/EXEC/UNWATCH is therefore
	// unsupported.
	//
	// Tx is not safe for concurrent use (see above), so watchArmed is accessed
	// only from the goroutine that owns the Tx and needs no synchronization.
	watchArmed bool

	// watchedKeys are the keys currently watched on the transaction
	// connection, in WATCH order. watchSnapshot holds a DUMP fingerprint of
	// each watched key taken when it was watched, used to report exactly
	// which keys changed when EXEC aborts. Both are maintained only by
	// Tx.Watch, Tx.Unwatch and the TxPipeline EXEC closure, on the goroutine
	// that owns the Tx, so they need no synchronization.
	watchedKeys   []string
	watchSnapshot map[string]string
}

func (c *Client) newTx() *Tx {
	tx := Tx{
		baseClient: baseClient{
			opt:           c.cloneOpt(), // Clone options under optLock to avoid race with initConn
			connPool:      c.baseClient.newStickyConnPool(),
			hooksMixin:    c.hooksMixin.clone(),
			pushProcessor: c.pushProcessor, // Copy push processor from parent client
			onClose:       &onCloseHooks{},
			// Share the HIMPORT fieldset registry: the sticky pool borrows
			// connections from the parent client's pool, so fieldsets
			// prepared on them stay valid after the connections are
			// returned.
			himport: c.himport,
			// Carry the shared eviction hook (not csc: a sticky Tx must not serve
			// cached reads) so close/reinit hooks on a Watch-initialized conn still
			// evict from the parent cache.
			cscPoolHook: c.cscPoolHook,
			cscActive:   c.cscActive,
		},
	}
	tx.init()
	return &tx
}

func (c *Tx) init() {
	c.cmdable = c.Process
	c.statefulCmdable = c.Process

	c.initHooks(hooks{
		dial:       c.baseClient.dial,
		process:    c.baseClient.process,
		pipeline:   c.baseClient.processPipeline,
		txPipeline: c.baseClient.processTxPipeline,
	})
}

func (c *Tx) Process(ctx context.Context, cmd Cmder) error {
	err := c.processHook(ctx, cmd)
	cmd.SetErr(err)
	return err
}

// Watch prepares a transaction and marks the keys to be watched
// for conditional execution if there are any keys.
//
// The transaction is automatically closed when fn exits.
func (c *Client) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	tx := c.newTx()
	defer tx.Close(ctx)
	if len(keys) > 0 {
		if err := tx.Watch(ctx, keys...).Err(); err != nil {
			return err
		}
	}
	return fn(tx)
}

// Close closes the transaction, releasing any open resources.
func (c *Tx) Close(ctx context.Context) error {
	// UNWATCH is only needed while a WATCH is still active. EXEC discards the
	// watched keys server-side on both commit and abort, so the common
	// WATCH/.../EXEC paths leave nothing to release and avoid the extra round
	// trip.
	if c.watchArmed {
		_ = c.Unwatch(ctx).Err()
	}
	return c.baseClient.Close()
}

// Watch marks the keys to be watched for conditional execution
// of a transaction.
func (c *Tx) Watch(ctx context.Context, keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "watch"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	// A successful WATCH leaves keys watched on the connection that Close must
	// later release with UNWATCH.
	if cmd.Err() == nil {
		c.watchArmed = true
		c.snapshotWatchedKeys(ctx, keys)
	}
	return cmd
}

// snapshotWatchedKeys records the watched keys and takes a DUMP fingerprint
// of each so a later aborted EXEC can report exactly which keys changed.
// WATCH accumulates server-side, so keys are appended; keys watched again
// keep their original fingerprint.
func (c *Tx) snapshotWatchedKeys(ctx context.Context, keys []string) {
	if len(keys) == 0 {
		return
	}
	if c.watchSnapshot == nil {
		c.watchSnapshot = make(map[string]string, len(keys))
	}
	for _, key := range keys {
		if _, ok := c.watchSnapshot[key]; ok {
			continue
		}
		c.watchedKeys = append(c.watchedKeys, key)
		c.watchSnapshot[key] = c.keyFingerprint(ctx, key)
	}
}

// keyFingerprint returns a comparable fingerprint of a key's current value.
// DUMP works for every data type; a missing key gets a dedicated marker.
func (c *Tx) keyFingerprint(ctx context.Context, key string) string {
	cmd := NewStringCmd(ctx, "dump", key)
	if err := c.processHook(ctx, cmd); err != nil {
		if err == Nil {
			return watchFingerprintMissing
		}
		return "\x00redis:tx:watch:error:" + err.Error()
	}
	return "v:" + cmd.Val()
}

// changedWatchedKeys re-fingerprints the watched keys and returns those whose
// value changed since WATCH (including deleted or newly created keys). If the
// comparison cannot be performed it falls back to all watched keys so the
// failure is never reported without naming keys.
func (c *Tx) changedWatchedKeys(ctx context.Context) []string {
	if len(c.watchedKeys) == 0 {
		return nil
	}
	changed := make([]string, 0, len(c.watchedKeys))
	for _, key := range c.watchedKeys {
		if c.keyFingerprint(ctx, key) != c.watchSnapshot[key] {
			changed = append(changed, key)
		}
	}
	if len(changed) == 0 {
		// EXEC aborted but no difference could be observed (e.g. the key
		// was changed and changed back, or DUMP is unsupported). Report
		// all watched keys rather than none.
		changed = append(changed, c.watchedKeys...)
	}
	return changed
}

// resetWatch clears the client-side watch bookkeeping once the server has
// released the watched keys (EXEC, UNWATCH, or connection close).
func (c *Tx) resetWatch() {
	c.watchArmed = false
	c.watchedKeys = nil
	c.watchSnapshot = nil
}

// Unwatch flushes all the previously watched keys for a transaction.
func (c *Tx) Unwatch(ctx context.Context, keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "unwatch"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	// The watched keys have been released, so Close need not UNWATCH again.
	if cmd.Err() == nil {
		c.resetWatch()
	}
	return cmd
}

// Pipeline creates a pipeline. Usually it is more convenient to use Pipelined.
func (c *Tx) Pipeline() Pipeliner {
	pipe := Pipeline{
		exec: func(ctx context.Context, cmds []Cmder) error {
			return c.processPipelineHook(ctx, cmds)
		},
	}
	pipe.init()
	return &pipe
}

// Pipelined executes commands queued in the fn outside of the transaction.
// Use TxPipelined if you need transactional behavior.
func (c *Tx) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

// TxPipelined executes commands queued in the fn in the transaction.
//
// When using WATCH, EXEC will execute commands only if the watched keys
// were not modified, allowing for a check-and-set mechanism.
//
// Exec always returns list of commands. If the transaction fails because a
// watched key changed, a *TxFailedError is returned: it matches TxFailedErr
// via errors.Is and reports the conflicting keys and the number of discarded
// pipeline commands. Otherwise Exec returns an error of the first failed
// command or nil.
func (c *Tx) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

// TxPipeline creates a pipeline. Usually it is more convenient to use TxPipelined.
func (c *Tx) TxPipeline() Pipeliner {
	pipe := Pipeline{
		exec: func(ctx context.Context, cmds []Cmder) error {
			queued := len(cmds)
			cmds = wrapMultiExec(ctx, cmds)
			err := c.processTxPipelineHook(ctx, cmds)
			// EXEC discards the watched keys server-side, so the watch is
			// cleared only when EXEC actually ran: a nil error (committed),
			// TxFailedErr (a watched key changed, EXEC returned nil), or an
			// EXECABORT error (a queued command was rejected, so EXEC discarded
			// the transaction). All three release the watched keys. Any other
			// error may be reported before EXEC executes (for example -LOADING on
			// the MULTI reply) or on a broken connection, so leave watchArmed set
			// and let Close send UNWATCH rather than risk leaving a watch on a
			// pooled connection.
			if err == nil || IsExecAbortError(err) {
				c.resetWatch()
				return err
			}
			if errors.Is(err, TxFailedErr) {
				// The transaction aborted because a watched key changed.
				// Name the conflicting keys and the number of discarded
				// pipeline commands in the returned error.
				txErr := &TxFailedError{
					ChangedKeys:    c.changedWatchedKeys(ctx),
					WatchedKeys:    append([]string(nil), c.watchedKeys...),
					QueuedCommands: queued,
				}
				c.resetWatch()
				return txErr
			}
			return err
		},
	}
	pipe.init()
	return &pipe
}

func wrapMultiExec(ctx context.Context, cmds []Cmder) []Cmder {
	if len(cmds) == 0 {
		panic("not reached")
	}
	cmdsCopy := make([]Cmder, len(cmds)+2)
	cmdsCopy[0] = NewStatusCmd(ctx, "multi")
	copy(cmdsCopy[1:], cmds)
	cmdsCopy[len(cmdsCopy)-1] = NewSliceCmd(ctx, "exec")
	return cmdsCopy
}
