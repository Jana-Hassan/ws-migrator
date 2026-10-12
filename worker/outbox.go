package worker

import (
	"encoding/binary"
	"fmt"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
)

type outFrame struct {
	kind        byte
	key         uint64
	messageType byte
	payload     []byte
}

const outFrameHeader = 1 + 8 + 1 + 4

func encodeOutbox(frames []outFrame) []byte {
	size := 0
	for _, f := range frames {
		size += outFrameHeader + len(f.payload)
	}

	out := make([]byte, 0, size)
	for _, f := range frames {
		var header [outFrameHeader]byte
		header[0] = f.kind
		binary.BigEndian.PutUint64(header[1:9], f.key)
		header[9] = f.messageType
		binary.BigEndian.PutUint32(header[10:14], uint32(len(f.payload)))
		out = append(out, header[:]...)
		out = append(out, f.payload...)
	}
	return out
}

func decodeOutbox(data []byte) ([]outFrame, error) {
	var frames []outFrame
	for len(data) > 0 {
		if len(data) < outFrameHeader {
			return nil, fmt.Errorf("outbox truncated: %d bytes left", len(data))
		}

		size := int(binary.BigEndian.Uint32(data[10:14]))
		if len(data) < outFrameHeader+size {
			return nil, fmt.Errorf("outbox frame claims %d bytes, %d left", size, len(data)-outFrameHeader)
		}

		frames = append(frames, outFrame{
			kind:        data[0],
			key:         binary.BigEndian.Uint64(data[1:9]),
			messageType: data[9],
			payload:     append([]byte(nil), data[outFrameHeader:outFrameHeader+size]...),
		})
		data = data[outFrameHeader+size:]
	}
	return frames, nil
}

func trimOutbox(frames []outFrame, ackedReply, ackedPush uint64) []outFrame {
	kept := frames[:0]
	for _, f := range frames {
		switch {
		case f.kind == frame.Reply && f.key <= ackedReply:
		case f.kind == frame.Push && f.key <= ackedPush:
		default:
			kept = append(kept, f)
		}
	}
	return kept
}
