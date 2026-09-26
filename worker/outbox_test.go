package worker

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
)

type countState struct{ n int }

func (c *countState) Serialize() ([]byte, error) { return []byte(strconv.Itoa(c.n)), nil }

func (c *countState) Restore(data []byte) error {
	n, err := strconv.Atoi(string(data))
	c.n = n
	return err
}

func newCountWorker(t *testing.T, id string) (*Worker, string) {
	t.Helper()

	w := NewWorker(Config{ID: id})
	w.OnSessionCreate(func(string) State { return &countState{} })
	w.OnMessage(func(sessionID string, state State, msg []byte) []byte {
		st := state.(*countState)
		st.n++
		return []byte(fmt.Sprintf("count=%d", st.n))
	})

	srv := httptest.NewServer(w.Handler())
	t.Cleanup(srv.Close)
	return w, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func dialAsGateway(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func sendMessage(t *testing.T, conn *websocket.Conn, seq uint64) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage, frame.EncodeUp(frame.Message, seq, []byte("tick"))); err != nil {
		t.Fatalf("send seq %d: %v", seq, err)
	}
}

func readDown(t *testing.T, conn *websocket.Conn) (byte, uint64, string) {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	kind, key, payload, ok := frame.DecodeDown(data)
	if !ok {
		t.Fatalf("malformed downlink frame %q", data)
	}
	return kind, key, string(payload)
}

func waitOutbox(t *testing.T, s *session, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n := len(s.outbox)
		s.mu.Unlock()
		if n == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("outbox holds %d frames, wanted %d", len(s.outbox), want)
}

func TestSnapshotKeepsRepliesTheGatewayNeverReceived(t *testing.T) {
	a, urlA := newCountWorker(t, "a")
	gateway := dialAsGateway(t, urlA+"?session_id=s1")

	for seq := uint64(1); seq <= 5; seq++ {
		sendMessage(t, gateway, seq)
		readDown(t, gateway)
	}

	if err := gateway.WriteMessage(websocket.BinaryMessage, frame.EncodeAck(3, 0)); err != nil {
		t.Fatalf("ack: %v", err)
	}

	s, ok := a.sessions.get("s1")
	if !ok {
		t.Fatal("session missing on worker a")
	}
	waitOutbox(t, s, 2)

	snap, ok := s.takeSnapshot()
	if !ok {
		t.Fatal("no snapshot taken")
	}
	if snap.AppliedSeq != 5 || string(snap.State) != "5" {
		t.Fatalf("snapshot at seq %d state %q, wanted seq 5 state 5", snap.AppliedSeq, snap.State)
	}

	gateway.Close()

	b, urlB := newCountWorker(t, "b")
	restorer := newGRPCServer("b", b.sessions, b.onCreate, time.Second, b)
	resp, err := restorer.Restore(context.Background(), &migratorpb.RestoreRequest{
		SessionId:  "s1",
		State:      snap.State,
		AppliedSeq: snap.AppliedSeq,
		Outbox:     snap.Outbox,
	})
	if err != nil || !resp.Ok {
		t.Fatalf("restore: %v %+v", err, resp)
	}

	gatewayB := dialAsGateway(t, urlB+"?session_id=s1&resumed=1")

	for _, want := range []uint64{4, 5} {
		kind, key, payload := readDown(t, gatewayB)
		if kind != frame.Reply || key != want || payload != fmt.Sprintf("count=%d", want) {
			t.Fatalf("got kind=%d key=%d %q, wanted the lost reply %d", kind, key, payload, want)
		}
	}

	sendMessage(t, gatewayB, 6)
	kind, key, payload := readDown(t, gatewayB)
	if kind != frame.Reply || key != 6 || payload != "count=6" {
		t.Fatalf("after recovery got kind=%d key=%d %q, wanted reply 6 with count=6", kind, key, payload)
	}
}

func TestPushChangesStateAndOutboxTogether(t *testing.T) {
	w, url := newCountWorker(t, "a")
	gateway := dialAsGateway(t, url+"?session_id=s1")
	sendMessage(t, gateway, 1)
	readDown(t, gateway)

	s, _ := w.sessions.get("s1")

	err := w.Push("s1", func(state State) (uint64, []byte, bool) {
		st := state.(*countState)
		st.n++
		return uint64(st.n), []byte("pushed"), true
	})
	if err != nil {
		t.Fatalf("push: %v", err)
	}

	snap, ok := s.takeSnapshot()
	if !ok {
		t.Fatal("push did not mark the session for a snapshot")
	}

	frames, err := decodeOutbox(snap.Outbox)
	if err != nil {
		t.Fatalf("decode outbox: %v", err)
	}

	if string(snap.State) != "2" {
		t.Fatalf("snapshot state %q, wanted 2", snap.State)
	}
	last := frames[len(frames)-1]
	if last.kind != frame.Push || last.key != 2 || string(last.payload) != "pushed" {
		t.Fatalf("snapshot outbox ends with %+v, wanted push 2", last)
	}
}

func TestPushIsRejectedWhileFrozen(t *testing.T) {
	w, url := newCountWorker(t, "a")
	gateway := dialAsGateway(t, url+"?session_id=s1")
	sendMessage(t, gateway, 1)
	readDown(t, gateway)

	s, _ := w.sessions.get("s1")
	s.mu.Lock()
	s.frozen = true
	s.mu.Unlock()

	called := false
	err := w.Push("s1", func(State) (uint64, []byte, bool) {
		called = true
		return 1, nil, true
	})
	if err != ErrFrozen {
		t.Fatalf("push while frozen returned %v, wanted ErrFrozen", err)
	}
	if called {
		t.Fatal("push changed state while the session was frozen")
	}
}

func TestAckTrimsEachKindByItsOwnWatermark(t *testing.T) {
	frames := []outFrame{
		{kind: frame.Reply, key: 4},
		{kind: frame.Push, key: 1},
		{kind: frame.Reply, key: 5},
		{kind: frame.Push, key: 2},
		{kind: frame.Push, key: 3},
	}

	kept := trimOutbox(frames, 5, 2)
	if len(kept) != 1 || kept[0].kind != frame.Push || kept[0].key != 3 {
		t.Fatalf("kept %+v, wanted only push 3", kept)
	}
}

func TestOutboxRoundTrip(t *testing.T) {
	frames := []outFrame{
		{kind: frame.Reply, key: 7, messageType: websocket.TextMessage, payload: []byte("a")},
		{kind: frame.Push, key: 1 << 40, messageType: websocket.BinaryMessage, payload: nil},
		{kind: frame.Reply, key: 8, messageType: websocket.TextMessage, payload: []byte("longer payload")},
	}

	decoded, err := decodeOutbox(encodeOutbox(frames))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if fmt.Sprint(decoded) != fmt.Sprint(frames) {
		t.Fatalf("round trip changed frames:\n got %+v\nwant %+v", decoded, frames)
	}

	if _, err := decodeOutbox(encodeOutbox(frames)[:5]); err == nil {
		t.Fatal("accepted a truncated outbox")
	}
}
