package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
)

func TestMessageSentAsWorkerDiesIsNotLost(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	c.waitForSnapshot(first.sessionID, "1")

	c.worker(first.worker).kill()

	r := client.sendExpectingNext("during-crash")
	if r.worker == first.worker {
		t.Fatalf("reply came from the dead worker %s", r.worker)
	}
	if r.count != 2 {
		t.Fatalf("count %d after recovery, wanted 2", r.count)
	}
}

func TestMessageSwallowedByDeadWorkerIsResent(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	c.waitForSnapshot(first.sessionID, "1")

	blackhole := c.addBlackHoleWorker("blackhole")

	c.migrateOK(first.sessionID, "blackhole")
	c.waitForSessionOff(first.sessionID, first.worker)

	client.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
	if err := client.conn.WriteMessage(websocket.TextMessage, []byte("swallowed")); err != nil {
		t.Fatalf("client write: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	blackhole.kill()

	client.conn.SetReadDeadline(time.Now().Add(ioTimeout))
	_, data, err := client.conn.ReadMessage()
	if err != nil {
		t.Fatalf("no reply after recovery, the message was lost: %v", err)
	}

	r := parseReply(t, string(data))
	if r.worker != first.worker {
		t.Fatalf("reply came from %s, wanted %s", r.worker, first.worker)
	}
	if r.count != 2 {
		t.Fatalf("count %d, wanted 2", r.count)
	}
}

func TestNoDoubleProcessingWhenSnapshotIsStale(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	c.waitForSnapshot(first.sessionID, "1")

	target := c.otherWorker(first.worker)
	c.migrateOK(first.sessionID, target)
	client.waitForWorker(target)

	if client.last < 2 {
		t.Fatalf("count went backwards: %d", client.last)
	}

	for i := 0; i < 5; i++ {
		client.sendExpectingNext("after")
	}
}

func TestContinuousSendAcrossMigrationKeepsOrderAndCount(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.migrateOK(first.sessionID, target)

	for i := 0; i < 50; i++ {
		client.sendExpectingNext(fmt.Sprintf("msg-%d", i))
	}

	if client.last != 51 {
		t.Fatalf("count %d after 51 messages: lost or duplicated", client.last)
	}

	client.waitForWorker(target)
}

func TestDuplicateSequenceIsSkipped(t *testing.T) {
	c := newCluster(t, 1)
	tw := c.workers[0]

	conn, _, err := websocket.DefaultDialer.Dial(tw.wsURL+"?session_id=sess_direct", nil)
	if err != nil {
		t.Fatalf("dial worker: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	send := func(seq uint64, body string) {
		t.Helper()
		conn.SetWriteDeadline(time.Now().Add(ioTimeout))
		if err := conn.WriteMessage(websocket.TextMessage, frame.Encode(seq, []byte(body))); err != nil {
			t.Fatalf("write seq %d: %v", seq, err)
		}
	}

	read := func(timeout time.Duration) (reply, bool) {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(timeout))
		_, data, err := conn.ReadMessage()
		if err != nil {
			return reply{}, false
		}
		return parseReply(t, string(data)), true
	}

	send(1, "first")
	r, ok := read(ioTimeout)
	if !ok || r.count != 1 {
		t.Fatalf("first message: got %+v ok=%v, wanted count 1", r, ok)
	}

	send(1, "duplicate")
	if r, ok := read(300 * time.Millisecond); ok {
		t.Fatalf("duplicate seq 1 was processed: %+v", r)
	}
}

func TestSequenceSurvivesPlacement(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	for i := 0; i < 4; i++ {
		client.sendExpectingNext("hello")
	}

	target := c.otherWorker(first.worker)
	c.migrateOK(first.sessionID, target)
	client.waitForWorker(target)

	c.dropSnapshot(first.sessionID)
	client.sendExpectingNext("after")

	c.waitForSnapshot(first.sessionID, fmt.Sprintf("%d", client.last))
}

func TestMalformedFrameFromClientIsHandled(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	client.sendExpectingNext("hello")

	client.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
	if err := client.conn.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3}); err != nil {
		t.Fatalf("client write: %v", err)
	}

	client.sendExpectingNext("still-serving")
}
