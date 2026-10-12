package frame

import (
	"bytes"
	"testing"
)

func TestDownlinkRoundTrip(t *testing.T) {
	for _, kind := range []byte{Plain, Reply, Push} {
		kind, key, payload, ok := DecodeDown(EncodeDown(kind, 1<<40+7, []byte("hello")))
		if !ok || key != 1<<40+7 || !bytes.Equal(payload, []byte("hello")) {
			t.Fatalf("kind %d: got key=%d payload=%q ok=%v", kind, key, payload, ok)
		}
	}
}

func TestDownlinkRejectsShortAndUnknownFrames(t *testing.T) {
	if _, _, _, ok := DecodeDown([]byte{Reply, 0, 0}); ok {
		t.Fatal("accepted a truncated frame")
	}
	if _, _, _, ok := DecodeDown(append([]byte{9}, make([]byte, 8)...)); ok {
		t.Fatal("accepted an unknown kind")
	}
}

func TestDownlinkAllowsEmptyPayload(t *testing.T) {
	kind, key, payload, ok := DecodeDown(EncodeDown(Reply, 3, nil))
	if !ok || kind != Reply || key != 3 || len(payload) != 0 {
		t.Fatalf("empty payload: kind=%d key=%d len=%d ok=%v", kind, key, len(payload), ok)
	}
}

func TestUplinkRoundTrip(t *testing.T) {
	kind, seq, payload, ok := DecodeUp(EncodeUp(Message, 42, []byte("hi")))
	if !ok || kind != Message || seq != 42 || string(payload) != "hi" {
		t.Fatalf("got kind=%d seq=%d payload=%q ok=%v", kind, seq, payload, ok)
	}
	if _, _, _, ok := DecodeUp(EncodeDown(Push, 1, nil)); ok {
		t.Fatal("uplink decoder accepted a downlink push kind")
	}
}

func TestAckRoundTrip(t *testing.T) {
	kind, _, payload, ok := DecodeUp(EncodeAck(7, 9))
	if !ok || kind != Ack {
		t.Fatalf("not an ack: kind=%d ok=%v", kind, ok)
	}
	reply, push, ok := DecodeAck(payload)
	if !ok || reply != 7 || push != 9 {
		t.Fatalf("ack decoded as reply=%d push=%d ok=%v", reply, push, ok)
	}
	if _, _, ok := DecodeAck([]byte{1, 2, 3}); ok {
		t.Fatal("accepted a short ack")
	}
}
