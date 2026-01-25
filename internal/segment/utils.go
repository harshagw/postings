package segment

import (
	"bytes"
	"fmt"
)

// prefixSuccessor returns the lexicographically next prefix after the given one.
func prefixSuccessor(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	succ := bytes.Clone(prefix)

	for i := len(succ) - 1; i >= 0; i-- {
		if succ[i] < 0xff {
			succ[i]++
			return succ[:i+1]
		}
	}

	return nil
}

// byteReader is a simple reader for varint decoding without allocations.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader {
	return &byteReader{data: data, pos: 0}
}

func (r *byteReader) ReadUvarint() (uint64, error) {
	var x uint64
	var s uint
	for {
		if r.pos >= len(r.data) {
			return 0, fmt.Errorf("unexpected EOF")
		}
		b := r.data[r.pos]
		r.pos++
		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}
