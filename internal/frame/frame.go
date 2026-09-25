package frame

import "encoding/binary"

const headerSize = 8

func Encode(seq uint64, payload []byte) []byte {
	out := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint64(out, seq)
	copy(out[headerSize:], payload)
	return out
}

func Decode(data []byte) (uint64, []byte, bool) {
	if len(data) < headerSize {
		return 0, nil, false
	}
	return binary.BigEndian.Uint64(data[:headerSize]), data[headerSize:], true
}
