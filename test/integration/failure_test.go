package integration

import (
	"testing"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

func TestMigrateToUnknownWorkerKeepsSession(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")

	c.migrateOK(first.sessionID, "worker-does-not-exist")

	if r := client.sendExpectingNext("still-here"); r.worker != first.worker {
		t.Fatalf("session moved to %s after failed migration", r.worker)
	}
}

func TestMigrateToSameWorkerKeepsSession(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")

	c.migrateOK(first.sessionID, first.worker)

	if r := client.sendExpectingNext("still-here"); r.worker != first.worker {
		t.Fatalf("session moved to %s", r.worker)
	}
}

func TestCheckpointFailureKeepsSession(t *testing.T) {
	c := newClusterWith(t, 2, func(sessionID string) worker.State {
		return &failingState{}
	})

	client := c.connect()
	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.migrateOK(first.sessionID, target)

	if r := client.sendExpectingNext("still-here"); r.worker != first.worker {
		t.Fatalf("session moved to %s despite checkpoint failure", r.worker)
	}
}

func TestClientSurvivesWhenOnlyOneWorkerExists(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")

	c.migrateOK(first.sessionID, "worker-does-not-exist")

	for i := 0; i < 5; i++ {
		client.sendExpectingNext("ping")
	}
}
