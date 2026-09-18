package integration

import (
	"fmt"
	"testing"
)

func TestStateSurvivesMigration(t *testing.T) {
	c := newCluster(t, 2)
	client := c.connect()

	first := client.sendExpectingNext("hello")
	client.sendExpectingNext("hello")
	client.sendExpectingNext("hello")

	target := c.otherWorker(first.worker)
	c.migrateOK(first.sessionID, target)

	landed := client.waitForWorker(target)
	if landed.sessionID != first.sessionID {
		t.Fatalf("session id changed: %s -> %s", first.sessionID, landed.sessionID)
	}
}

func TestClientSurvivesRepeatedMigrations(t *testing.T) {
	c := newCluster(t, 2)
	client := c.connect()

	current := client.sendExpectingNext("hello").worker

	for i := 0; i < 3; i++ {
		target := c.otherWorker(current)
		c.migrateOK(client.session, target)
		client.waitForWorker(target)
		current = target
	}

	if client.last < 4 {
		t.Fatalf("expected count to keep climbing, got %d", client.last)
	}
}

func TestMessagesSentDuringMigrationArriveInOrder(t *testing.T) {
	c := newCluster(t, 2)
	client := c.connect()

	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.migrateOK(first.sessionID, target)

	for i := 0; i < 20; i++ {
		client.sendExpectingNext(fmt.Sprintf("msg-%d", i))
	}

	if client.last != 21 {
		t.Fatalf("lost or duplicated messages: count %d, wanted 21", client.last)
	}

	waitFor(t, "session to land on "+target, func() bool {
		return client.sendExpectingNext("ping").worker == target
	})
}
