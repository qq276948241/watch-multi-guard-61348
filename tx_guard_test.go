package redis_test

import (
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("TxGuard", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("commits all queued commands in order when nothing changed", func() {
		Expect(client.Set(ctx, "g1", "0", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "g1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		set1 := g.Set(ctx, "g1", "1", 0)
		set2 := g.Set(ctx, "g1", "2", 0)
		get := g.Get(ctx, "g1")
		Expect(g.Len()).To(Equal(3))

		cmds, err := g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(3))
		Expect(cmds[0]).To(Equal(set1))
		Expect(cmds[1]).To(Equal(set2))
		Expect(cmds[2]).To(Equal(get))
		Expect(set1.Err()).NotTo(HaveOccurred())
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("2"))

		// After a successful commit, reads observe the final written value.
		Expect(client.Get(ctx, "g1").Val()).To(Equal("2"))
	})

	It("fails the commit and names the changed keys when a watched key changed", func() {
		Expect(client.Set(ctx, "c1", "a", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "c2", "b", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "c1", "c2")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "c1", "x", 0)
		g.Set(ctx, "c2", "y", 0)
		g.Set(ctx, "c3", "z", 0)

		// Another client modifies one of the watched keys.
		Expect(client.Set(ctx, "c2", "other", 0).Err()).NotTo(HaveOccurred())

		_, err = g.Commit(ctx)
		Expect(err).To(HaveOccurred())
		conflict, ok := redis.IsTxConflict(err)
		Expect(ok).To(BeTrue())
		Expect(conflict.Keys).To(Equal([]string{"c2"}))
		Expect(conflict.Queued).To(Equal(3))
		Expect(err.Error()).To(ContainSubstring("c2"))
		Expect(err.Error()).To(ContainSubstring("3"))
		Expect(err.Error()).NotTo(ContainSubstring("c1"))

		// None of the queued commands took effect.
		Expect(client.Get(ctx, "c1").Val()).To(Equal("a"))
		Expect(client.Get(ctx, "c2").Val()).To(Equal("other"))
		Expect(client.Exists(ctx, "c3").Val()).To(Equal(int64(0)))
	})

	It("does not report a conflict when no watched key changed", func() {
		Expect(client.Set(ctx, "n1", "a", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "n2", "b", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "n1", "n2")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "n1", "a2", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "n1").Val()).To(Equal("a2"))
	})

	It("commits when only unwatched keys changed", func() {
		Expect(client.Set(ctx, "w1", "a", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "u1", "b", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "w1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "w1", "c", 0)
		Expect(client.Set(ctx, "u1", "changed", 0).Err()).NotTo(HaveOccurred())

		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "w1").Val()).To(Equal("c"))
	})

	It("treats a deleted watched key as changed", func() {
		Expect(client.Set(ctx, "d1", "a", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "d1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "d1", "b", 0)
		Expect(client.Del(ctx, "d1").Err()).NotTo(HaveOccurred())

		_, err = g.Commit(ctx)
		conflict, ok := redis.IsTxConflict(err)
		Expect(ok).To(BeTrue())
		Expect(conflict.Keys).To(Equal([]string{"d1"}))
	})

	It("releases the watch and clears the pipeline on discard", func() {
		Expect(client.Set(ctx, "k1", "a", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "k1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "k1", "b", 0)
		g.Set(ctx, "k2", "c", 0)
		Expect(g.Discard(ctx)).NotTo(HaveOccurred())
		Expect(g.Len()).To(Equal(0))

		// Nothing was applied.
		Expect(client.Get(ctx, "k1").Val()).To(Equal("a"))
		Expect(client.Exists(ctx, "k2").Val()).To(Equal(int64(0)))

		// The discarded commands are not carried into the next transaction,
		// and a change made after the discard does not fail it.
		Expect(client.Set(ctx, "k1", "other", 0).Err()).NotTo(HaveOccurred())
		Expect(g.Watch(ctx, "k1")).NotTo(HaveOccurred())
		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "k3", "v", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "k3").Val()).To(Equal("v"))
		Expect(client.Exists(ctx, "k2").Val()).To(Equal(int64(0)))
	})

	It("releases the watch when discarded before Begin", func() {
		Expect(client.Set(ctx, "p1", "a", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "p1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Discard(ctx)).NotTo(HaveOccurred())
		Expect(g.WatchedKeys()).To(BeEmpty())

		// A change after the discard must not fail the next transaction.
		Expect(client.Set(ctx, "p1", "other", 0).Err()).NotTo(HaveOccurred())
		Expect(g.Watch(ctx, "p1")).NotTo(HaveOccurred())
		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "p1", "b", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects a nested Begin", func() {
		g, err := client.NewTxGuard(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "q1", "1", 0)
		Expect(g.Begin(ctx)).To(HaveOccurred())

		// The first transaction is still intact and commits alone.
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "q1").Val()).To(Equal("1"))
	})

	It("does not sweep pre-transaction commands into the commit", func() {
		g, err := client.NewTxGuard(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		pre := g.Set(ctx, "pre1", "v", 0)
		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "tx1", "v", 0)
		cmds, err := g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(1))

		Expect(client.Get(ctx, "tx1").Val()).To(Equal("v"))
		Expect(client.Exists(ctx, "pre1").Val()).To(Equal(int64(0)))

		// The pre-transaction command can still be flushed separately.
		_, err = g.Flush(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(pre.Err()).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "pre1").Val()).To(Equal("v"))
	})

	It("does not allow both commit and discard to succeed", func() {
		g, err := client.NewTxGuard(ctx, "b1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "b1", "v", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(g.Discard(ctx)).To(HaveOccurred())
	})

	It("releases the watch after a failed commit", func() {
		Expect(client.Set(ctx, "f1", "a", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "f1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "f1", "b", 0)
		Expect(client.Set(ctx, "f1", "other", 0).Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		_, ok := redis.IsTxConflict(err)
		Expect(ok).To(BeTrue())

		// The failed transaction left no watch behind: the next transaction
		// does not start in conflict.
		Expect(g.WatchedKeys()).To(BeEmpty())
		Expect(g.Watch(ctx, "f1")).NotTo(HaveOccurred())
		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "f1", "c", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "f1").Val()).To(Equal("c"))
	})

	It("keeps two sequential transactions isolated", func() {
		Expect(client.Set(ctx, "s1", "a", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "s2", "a", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "s1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "s1", "b", 0)
		Expect(client.Set(ctx, "s1", "other", 0).Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		_, ok := redis.IsTxConflict(err)
		Expect(ok).To(BeTrue())

		// The second transaction watches only its own keys and starts clean.
		Expect(g.Watch(ctx, "s2")).NotTo(HaveOccurred())
		Expect(g.WatchedKeys()).To(Equal([]string{"s2"}))
		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "s2", "b", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "s2").Val()).To(Equal("b"))
	})

	It("voids the watch and the queue when the connection breaks", func() {
		Expect(client.Set(ctx, "x1", "a", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "x1")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "x1", "b", 0)

		// Break the underlying connections without Commit or Discard.
		Expect(client.Close()).NotTo(HaveOccurred())

		_, err = g.Commit(ctx)
		Expect(err).To(HaveOccurred())
		_, ok := redis.IsTxConflict(err)
		Expect(ok).To(BeFalse())
		Expect(g.Begin(ctx)).To(HaveOccurred())

		// Nothing was auto-committed on a later connection.
		fresh := redis.NewClient(redisOptions())
		defer fresh.Close()
		Expect(fresh.Get(ctx, "x1").Val()).To(Equal("a"))
	})

	It("still supports a plain commit without watching", func() {
		g, err := client.NewTxGuard(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "plain1", "1", 0)
		g.Incr(ctx, "plain2")
		cmds, err := g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
		Expect(client.Get(ctx, "plain1").Val()).To(Equal("1"))
		Expect(client.Get(ctx, "plain2").Val()).To(Equal("1"))
	})

	It("reports changed keys and queue length in one error", func() {
		Expect(client.Set(ctx, "m1", "a", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "m2", "a", 0).Err()).NotTo(HaveOccurred())

		g, err := client.NewTxGuard(ctx, "m1", "m2")
		Expect(err).NotTo(HaveOccurred())
		defer g.Close(ctx)

		Expect(g.Begin(ctx)).NotTo(HaveOccurred())
		g.Set(ctx, "m1", "b", 0)
		g.Set(ctx, "m2", "b", 0)
		Expect(client.Set(ctx, "m1", "x", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "m2", "y", 0).Err()).NotTo(HaveOccurred())

		_, err = g.Commit(ctx)
		Expect(err).To(HaveOccurred())
		msg := err.Error()
		Expect(msg).To(ContainSubstring("m1"))
		Expect(msg).To(ContainSubstring("m2"))
		Expect(strings.Contains(msg, "2")).To(BeTrue())
	})
})
