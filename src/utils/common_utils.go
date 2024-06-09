package utils

import (
	"crypto/rand"
	"math/big"
	"os"
	"time"
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

func NonBlockingPut[T any](ch chan T, toSend T) bool {
	select {
	case ch <- toSend:
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
	nMax := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, nMax)
	x := bigx.Int64()
	return x
}

type RpcArgs[T any] interface {
	GetRpcId() int64
}

type RpcReply[T any] interface {
	GetReply() T
}

func ExecuteRPC[R RpcReply[any]](rpcCall func() (bool, R)) (bool, R) {
	var reply R
	success := false
	for i := 0; !success && i < MAX_RPC_RETRIES; i++ {
		rpcCh := make(chan R, 1)
		rpcWrapper := func() {
			// make sure to create your rpc  object
			ok, reply := rpcCall()
			if ok {
				rpcCh <- reply
			}
		}

		go rpcWrapper()
		select {
		case reply = <-rpcCh:
			success = true
		case <-time.After(GetDurationInMillis(RPC_TIMEOUT_MS)):
		}
	}

	return success, reply
}
