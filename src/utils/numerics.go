package utils

import "encoding/binary"

func IntToBytes(value int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(value))
	return b
}

func BytesToInt(value []byte) int {
	return int(binary.LittleEndian.Uint64(value))
}
