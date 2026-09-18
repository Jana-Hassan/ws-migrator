package integration

import (
	"net/http"
	"testing"
)

func TestDrainMovesEverySession(t *testing.T) {
	c := newCluster(t, 2)

	clients := make([]*testClient, 5)
	for i := range clients {
		clients[i] = c.connect()
		clients[i].sendExpectingNext("hello")
	}

	victim := clients[0].sendExpectingNext("ping").worker
	survivor := c.otherWorker(victim)

	c.worker(victim).worker.Drain("test")

	for _, client := range clients {
		r := client.sendExpectingNext("after-drain")
		if r.worker == victim {
			t.Fatalf("session %s still on drained worker %s", r.sessionID, victim)
		}
		if r.worker != survivor {
			t.Fatalf("session %s landed on %s, wanted %s", r.sessionID, r.worker, survivor)
		}
	}
}

func TestDrainingWorkerGetsNoNewSessions(t *testing.T) {
	c := newCluster(t, 2)

	probe := c.connect()
	victim := probe.sendExpectingNext("hello").worker
	survivor := c.otherWorker(victim)

	c.worker(victim).worker.Drain("test")

	for i := 0; i < 3; i++ {
		client := c.connect()
		if r := client.sendExpectingNext("hello"); r.worker != survivor {
			t.Fatalf("new session landed on %s, wanted %s", r.worker, survivor)
		}
	}
}

func TestDrainingWorkerIsNotAMigrationTarget(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.worker(target).worker.Drain("test")

	c.migrateOK(first.sessionID, target)

	r := client.sendExpectingNext("still-here")
	if r.worker != first.worker {
		t.Fatalf("session moved to draining worker %s", r.worker)
	}
}

func TestDrainWithNoTargetKeepsSessionsAlive(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")

	c.worker(first.worker).worker.Drain("test")

	r := client.sendExpectingNext("still-here")
	if r.worker != first.worker {
		t.Fatalf("session moved to %s with no target available", r.worker)
	}
}

func TestMigrateToUnknownSessionIs404(t *testing.T) {
	c := newCluster(t, 2)

	if status, _ := c.migrate("sess_does_not_exist", "worker-2"); status != http.StatusNotFound {
		t.Fatalf("status %d, wanted 404", status)
	}
}
