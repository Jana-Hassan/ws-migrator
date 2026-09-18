package integration

import (
	"sync"
	"testing"
)

func TestManySessionsSurviveDrain(t *testing.T) {
	c := newCluster(t, 5)

	clients := make([]*testClient, 50)
	for i := range clients {
		clients[i] = c.connect()
		clients[i].sendExpectingNext("hello")
	}

	victim := clients[0].sendExpectingNext("ping").worker
	c.worker(victim).worker.Drain("test")

	for _, client := range clients {
		if r := client.sendExpectingNext("after-drain"); r.worker == victim {
			t.Fatalf("session %s still on drained worker", r.sessionID)
		}
	}
}

func TestConcurrentMigrations(t *testing.T) {
	c := newCluster(t, 3)

	clients := make([]*testClient, 20)
	for i := range clients {
		clients[i] = c.connect()
		clients[i].sendExpectingNext("hello")
	}

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(client *testClient) {
			defer wg.Done()
			c.migrate(client.session, c.otherWorker(client.session))
		}(client)
	}
	wg.Wait()

	for _, client := range clients {
		client.sendExpectingNext("after-migrations")
	}
}
