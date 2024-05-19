package utils

import (
	"encoding/binary"
	"strconv"
)

func IntToBytes(value int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(value))
	return b
}

func BytesToInt(value []byte) int {
	return int(binary.LittleEndian.Uint64(value))
}

func Int32ToString(value int32) string {
	return IntToString(int(value))
}

func Int64ToString(value int64) string {
	return IntToString(int(value))
}

func IntToString(value int) string {
	return strconv.Itoa(value)
}
