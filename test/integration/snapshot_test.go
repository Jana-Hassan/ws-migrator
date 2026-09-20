package integration

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func (c *cluster) waitForSnapshot(sessionID, want string) {
	c.t.Helper()

	waitFor(c.t, "snapshot "+want+" for "+sessionID, func() bool {
		state, ok := c.snapshot(sessionID)
		return ok && state == want
	})
}

func TestSnapshotIsWritten(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	for i := 0; i < 3; i++ {
		client.sendExpectingNext("hello")
	}

	c.waitForSnapshot(client.session, "3")
}

func TestIdleSessionIsNotSnapshotted(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	client.sendExpectingNext("hello")
	c.waitForSnapshot(client.session, "1")

	c.redis.Del("snapshot:" + client.session)
	time.Sleep(4 * testSnapshotInterval)

	if state, ok := c.snapshot(client.session); ok {
		t.Fatalf("idle session was snapshotted again: %q", state)
	}
}

func TestSessionRecoversFromSnapshotAfterCrash(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	for i := 0; i < 4; i++ {
		client.sendExpectingNext("hello")
	}

	c.waitForSnapshot(first.sessionID, "5")

	c.worker(first.worker).kill()
	recoveredOn := c.waitForSessionOff(first.sessionID, first.worker)

	r := client.sendExpectingNext("after-crash")
	if r.worker != recoveredOn {
		t.Fatalf("reply came from %s, expected %s", r.worker, recoveredOn)
	}
	if r.count != 6 {
		t.Fatalf("count %d after recovery, wanted 6", r.count)
	}
}

func TestRecoveryNeverResetsStateToZero(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	for i := 0; i < 9; i++ {
		client.sendExpectingNext("hello")
	}

	c.waitForSnapshot(first.sessionID, "10")

	c.worker(first.worker).kill()
	c.waitForSessionOff(first.sessionID, first.worker)

	r := client.send("after-crash")
	if r.count <= 1 {
		t.Fatalf("state was lost: count %d after recovery", r.count)
	}
}

func TestCrashWithNoHealthyTargetDropsClient(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")

	c.worker(first.worker).kill()

	client.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, _, err := client.conn.ReadMessage()
	if err == nil {
		t.Fatal("client stayed connected with no worker to recover onto")
	}
	if websocket.IsCloseError(err) == false && !isNetworkClose(err) {
		t.Fatalf("client was not dropped promptly: %v", err)
	}
}

func TestSnapshotDeletedWhenSessionEnds(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	client.sendExpectingNext("hello")
	c.waitForSnapshot(client.session, "1")

	client.conn.Close()

	waitFor(t, "snapshot cleaned up", func() bool {
		_, ok := c.snapshot(client.session)
		return !ok
	})
}

func TestMigrationWritesSnapshot(t *testing.T) {
	c := newCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	client.sendExpectingNext("hello")

	c.redis.Del("snapshot:" + first.sessionID)

	target := c.otherWorker(first.worker)
	c.migrateOK(first.sessionID, target)
	client.waitForWorker(target)

	state, ok := c.snapshot(first.sessionID)
	if !ok {
		t.Fatal("no snapshot written after migration")
	}
	if n, err := strconv.Atoi(state); err != nil || n < 2 {
		t.Fatalf("snapshot %q does not reflect the checkpoint", state)
	}
}

func TestWorkerSurvivesRedisOutage(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	client.sendExpectingNext("hello")

	c.redis.Close()

	for i := 0; i < 3; i++ {
		client.sendExpectingNext("still-serving")
	}
}

func isNetworkClose(err error) bool {
	if err == nil {
		return false
	}
	for _, want := range []string{"closed", "reset", "EOF", "abort"} {
		if strings.Contains(err.Error(), want) {
			return true
		}
	}
	return false
}
