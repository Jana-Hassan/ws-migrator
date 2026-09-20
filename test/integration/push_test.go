package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSendDeliversToClient(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")

	if err := c.worker(first.worker).worker.Send(first.sessionID, []byte("room event")); err != nil {
		t.Fatalf("send: %v", err)
	}

	client.conn.SetReadDeadline(time.Now().Add(ioTimeout))
	_, data, err := client.conn.ReadMessage()
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(data) != "room event" {
		t.Fatalf("client got %q, wanted %q", data, "room event")
	}
}

func TestSendToUnknownSessionFails(t *testing.T) {
	c := newCluster(t, 1)

	if err := c.workers[0].worker.Send("sess_nope", []byte("hi")); err == nil {
		t.Fatal("send to an unknown session should fail")
	}
}

func TestSendFollowsSessionAcrossMigration(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.migrateOK(first.sessionID, target)
	client.waitForWorker(target)

	if err := c.worker(first.worker).worker.Send(first.sessionID, []byte("stale")); err == nil {
		t.Fatal("the old worker should no longer hold the session")
	}

	if err := c.worker(target).worker.Send(first.sessionID, []byte("room event")); err != nil {
		t.Fatalf("send on the new worker: %v", err)
	}

	client.conn.SetReadDeadline(time.Now().Add(ioTimeout))
	_, data, err := client.conn.ReadMessage()
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(data) != "room event" {
		t.Fatalf("client got %q", data)
	}
}

func TestConcurrentSendsAndRepliesDoNotCorrupt(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	w := c.worker(first.worker).worker

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			w.Send(first.sessionID, []byte(fmt.Sprintf("push-%d", n)))
		}(i)
	}

	go func() {
		for i := 0; i < 20; i++ {
			client.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
			client.conn.WriteMessage(1, []byte("ping"))
		}
	}()

	wg.Wait()

	pushes := 0
	for i := 0; i < 40; i++ {
		client.conn.SetReadDeadline(time.Now().Add(ioTimeout))
		_, data, err := client.conn.ReadMessage()
		if err != nil {
			t.Fatalf("client read after %d messages: %v", i, err)
		}
		if len(data) == 0 {
			t.Fatal("empty frame: interleaved writes corrupted the stream")
		}
		if data[0] == 'p' {
			pushes++
		}
	}

	if pushes != 20 {
		t.Fatalf("got %d pushes, wanted 20", pushes)
	}
}

func TestSessionEndFiresOnMigration(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.migrateOK(first.sessionID, target)
	client.waitForWorker(target)

	waitFor(t, "session end on the old worker", func() bool {
		for _, w := range c.endsFor(first.sessionID) {
			if w == first.worker {
				return true
			}
		}
		return false
	})
}

func TestSessionEndFiresOnDisconnect(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")

	client.conn.Close()

	waitFor(t, "session end on disconnect", func() bool {
		return len(c.endsFor(first.sessionID)) == 1
	})
}
