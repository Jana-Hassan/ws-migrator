package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
	"github.com/Jana-Hassan/ws-migrator/worker"
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
		if err := conn.WriteMessage(websocket.TextMessage, frame.EncodeUp(frame.Message, seq, []byte(body))); err != nil {
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

		kind, key, payload, ok := frame.DecodeDown(data)
		if !ok || kind != frame.Reply {
			t.Fatalf("expected a reply frame, got kind=%d ok=%v", kind, ok)
		}

		r := parseReply(t, string(payload))
		if key != uint64(r.count) {
			t.Fatalf("reply tagged with seq %d but answers message %d", key, r.count)
		}
		return r, true
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

func TestRecoveryPastABufferGapDoesNotStall(t *testing.T) {
	c := newEmptyCluster(t)
	c.snapshotInterval = time.Hour
	c.addWorker("worker-1", func(string) worker.State { return &counter{} }, testHealthInterval)
	c.addWorker("worker-2", func(string) worker.State { return &counter{} }, testHealthInterval)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	for i := 0; i < 1100; i++ {
		client.sendExpectingNext("fill")
	}

	if _, ok := c.snapshot(first.sessionID); ok {
		t.Fatal("setup failed: a snapshot exists, so the ring would not be outrun")
	}

	c.worker(first.worker).kill()
	c.waitForSessionOff(first.sessionID, first.worker)

	r := client.send("after-gap")
	if r.worker == first.worker {
		t.Fatalf("reply came from the dead worker")
	}

	stats := c.stats()
	if stats.ResendGaps != 1 {
		t.Fatalf("resend gaps %d, wanted 1", stats.ResendGaps)
	}
	if r.count != 1025 {
		t.Fatalf("first reply after the gap has count %d, wanted 1025: 1024 replayed messages plus this one, with replies to the replay suppressed", r.count)
	}
}

func TestReplayedRepliesAreNotRedelivered(t *testing.T) {
	c := newEmptyCluster(t)
	c.snapshotInterval = time.Hour
	c.addWorker("worker-1", func(string) worker.State { return &counter{} }, testHealthInterval)
	c.addWorker("worker-2", func(string) worker.State { return &counter{} }, testHealthInterval)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	for i := 0; i < 19; i++ {
		client.sendExpectingNext("fill")
	}

	c.worker(first.worker).kill()
	c.waitForSessionOff(first.sessionID, first.worker)

	r := client.sendExpectingNext("after-crash")
	if r.count != 21 {
		t.Fatalf("count %d, wanted 21", r.count)
	}

	if dropped := c.stats().DuplicatesDropped; dropped != 20 {
		t.Fatalf("gateway dropped %d replayed replies, wanted 20", dropped)
	}
}

func TestPlainSendIsNeverDropped(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	w := c.worker(first.worker).worker

	for i := 0; i < 3; i++ {
		if err := w.Send(first.sessionID, []byte("same")); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		client.conn.SetReadDeadline(time.Now().Add(ioTimeout))
		_, data, err := client.conn.ReadMessage()
		if err != nil {
			t.Fatalf("plain send %d never arrived: %v", i+1, err)
		}
		if string(data) != "same" {
			t.Fatalf("got %q, wanted the payload with no header", data)
		}
	}
}

func TestKeyedPushDropsOnlyRepeatedKeys(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	w := c.worker(first.worker).worker

	for _, key := range []uint64{1, 2, 2, 1, 3} {
		err := w.Push(first.sessionID, func(worker.State) (uint64, []byte, bool) {
			return key, []byte(fmt.Sprintf("push=%d", key)), true
		})
		if err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	var got []string
	for i := 0; i < 3; i++ {
		client.conn.SetReadDeadline(time.Now().Add(ioTimeout))
		_, data, err := client.conn.ReadMessage()
		if err != nil {
			t.Fatalf("push %d missing: %v", i+1, err)
		}
		got = append(got, string(data))
	}

	if fmt.Sprint(got) != "[push=1 push=2 push=3]" {
		t.Fatalf("delivered %v, wanted [push=1 push=2 push=3]", got)
	}
	if dropped := c.stats().DuplicatesDropped; dropped != 2 {
		t.Fatalf("dropped %d, wanted 2", dropped)
	}
}

func TestAcksKeepTheOutboxSmall(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	for i := 0; i < 200; i++ {
		client.sendExpectingNext("fill")
	}

	time.Sleep(100 * time.Millisecond)
	client.sendExpectingNext("last")
	c.waitForSnapshot(first.sessionID, "202")

	snap, err := c.store.LoadSnapshot(context.Background(), first.sessionID)
	if err != nil || snap == nil {
		t.Fatalf("load snapshot: %v", err)
	}

	const perFrame = 14 + len("worker=worker-1 session=") + 40 + len(" count=202")
	if frames := len(snap.Outbox) / perFrame; frames > 10 {
		t.Fatalf("snapshot outbox holds about %d unacked frames after 202 acknowledged replies (%d bytes)", frames, len(snap.Outbox))
	}
	t.Logf("snapshot outbox after 202 replies: %d bytes", len(snap.Outbox))
}
