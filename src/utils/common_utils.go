package utils

import (
	"crypto/rand"
	"math/big"
	"os"
)

func PrintIfEnabled(envVar, msg string) {
	ExecuteIfEnabled(envVar, func() {
		println(msg)
	})
}

func ExecuteIfEnabled(envVar string, f func()) {
	if os.Getenv(envVar) == "true" {
		f()
	}
}

func NonBlockingPut[T any](ch *chan T, toSend T) bool {
	select {
	case *ch <- toSend:
		return true
	default:
		return false
	}
}

func FlushChannel[T any](c chan T) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
