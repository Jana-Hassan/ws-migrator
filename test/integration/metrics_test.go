package integration

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Jana-Hassan/ws-migrator/gateway"
)

func (c *cluster) stats() gateway.Stats {
	c.t.Helper()

	resp, err := http.Get(c.http.URL + "/admin/stats")
	if err != nil {
		c.t.Fatalf("stats request: %v", err)
	}
	defer resp.Body.Close()

	var stats gateway.Stats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		c.t.Fatalf("decode stats: %v", err)
	}
	return stats
}

func TestStatsCountSessionsAndMessages(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	client.sendExpectingNext("hello")

	stats := c.stats()
	if stats.SessionsActive != 1 || stats.SessionsStarted != 1 {
		t.Fatalf("sessions active=%d started=%d, wanted 1 and 1", stats.SessionsActive, stats.SessionsStarted)
	}
	if stats.MessagesForwarded != 2 {
		t.Fatalf("messages forwarded %d, wanted 2", stats.MessagesForwarded)
	}
	if stats.WorkersTotal != 2 || stats.WorkersHealthy != 2 {
		t.Fatalf("workers total=%d healthy=%d, wanted 2 and 2", stats.WorkersTotal, stats.WorkersHealthy)
	}

	client.conn.Close()
	waitFor(t, "session counted as ended", func() bool {
		return c.stats().SessionsEnded == 1 && c.stats().SessionsActive == 0
	})

	_ = first
}

func TestStatsCountMigrations(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.migrateOK(first.sessionID, target)
	client.waitForWorker(target)

	c.migrateOK(first.sessionID, "worker-does-not-exist")

	waitFor(t, "one migration succeeded and one failed", func() bool {
		s := c.stats()
		return s.MigrationsSucceeded == 1 && s.MigrationsFailed == 1 && s.MigrationsAttempted == 2
	})
}

func TestStatsCountRecoveryAndResends(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	c.waitForSnapshot(first.sessionID, "1")

	c.worker(first.worker).kill()
	c.waitForSessionOff(first.sessionID, first.worker)

	client.sendExpectingNext("after-crash")

	stats := c.stats()
	if stats.RecoveriesSucceeded != 1 {
		t.Fatalf("recoveries succeeded %d, wanted 1", stats.RecoveriesSucceeded)
	}
	if stats.ResendGaps != 0 {
		t.Fatalf("resend gaps %d, wanted 0", stats.ResendGaps)
	}
}

func TestStatsCountRejectedSessions(t *testing.T) {
	c := newEmptyCluster(t)

	conn, _, err := dialGateway(c)
	if err == nil {
		conn.Close()
	}

	waitFor(t, "rejected session counted", func() bool {
		return c.stats().SessionsRejected == 1
	})
}
