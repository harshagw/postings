package segment

import "bytes"

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
