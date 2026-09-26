package integration

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const readClientFrame = "gateway.(*Server).readClient"

func stackDump() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

func countGoroutines(needle string) int {
	return strings.Count(stackDump(), needle)
}

func waitForGoroutines(t *testing.T, needle string, max int) {
	t.Helper()

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if countGoroutines(needle) <= max {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("%d goroutines still in %s, wanted at most %d\n%s", countGoroutines(needle), needle, max, stackDump())
}

func TestNoGoroutineLeakWhenSessionsDieMidMessage(t *testing.T) {
	c := newCluster(t, 1)

	const sessions = 30
	clients := make([]*testClient, sessions)
	for i := range clients {
		clients[i] = c.connect()
		clients[i].sendExpectingNext("hello")
	}

	if n := countGoroutines(readClientFrame); n != sessions {
		t.Fatalf("expected %d reader goroutines while sessions are live, found %d", sessions, n)
	}

	stop := make(chan struct{})
	for _, client := range clients {
		go func(client *testClient) {
			for {
				select {
				case <-stop:
					return
				default:
				}
				client.conn.SetWriteDeadline(time.Now().Add(time.Second))
				if err := client.conn.WriteMessage(websocket.TextMessage, []byte("spam")); err != nil {
					return
				}
			}
		}(client)
	}

	c.workers[0].kill()
	time.Sleep(200 * time.Millisecond)
	close(stop)

	for _, client := range clients {
		client.conn.Close()
	}

	waitForGoroutines(t, readClientFrame, 0)
}
