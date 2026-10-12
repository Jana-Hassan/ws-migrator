package frame

import "encoding/binary"

const headerSize = 9

const (
	Message byte = iota
	Ack
)

const (
	Plain byte = iota
	Reply
	Push
)

func encode(kind byte, key uint64, payload []byte) []byte {
	out := make([]byte, headerSize+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint64(out[1:headerSize], key)
	copy(out[headerSize:], payload)
	return out
}

func decode(data []byte, maxKind byte) (byte, uint64, []byte, bool) {
	if len(data) < headerSize || data[0] > maxKind {
		return 0, 0, nil, false
	}
	return data[0], binary.BigEndian.Uint64(data[1:headerSize]), data[headerSize:], true
}

func EncodeUp(kind byte, seq uint64, payload []byte) []byte {
	return encode(kind, seq, payload)
}

func DecodeUp(data []byte) (byte, uint64, []byte, bool) {
	return decode(data, Ack)
}

func EncodeDown(kind byte, key uint64, payload []byte) []byte {
	return encode(kind, key, payload)
}

func DecodeDown(data []byte) (byte, uint64, []byte, bool) {
	return decode(data, Push)
}

func EncodeAck(reply, push uint64) []byte {
	payload := make([]byte, 16)
	binary.BigEndian.PutUint64(payload[:8], reply)
	binary.BigEndian.PutUint64(payload[8:], push)
	return EncodeUp(Ack, 0, payload)
}

func DecodeAck(payload []byte) (uint64, uint64, bool) {
	if len(payload) != 16 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(payload[:8]), binary.BigEndian.Uint64(payload[8:]), true
}
