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

func GetChunkFromBytes(dataBytes []byte, startOffset *int, chunkLen int) []byte {
	var value []byte
	if chunkLen == -1 {
		value = dataBytes[*startOffset:]
	} else {
		value = dataBytes[*startOffset : *startOffset+chunkLen]
	}
	*startOffset += chunkLen
	return value
}

func GetIntFromBytes(dataBytes []byte, startOffset *int) int {
	value := BytesToInt(GetChunkFromBytes(dataBytes, startOffset, INT_SIZE))
	return value
}

// The number of shards.
const NShards = 10

// which shard is a key in?
// please use this function,
// and please do not change it.
func Key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= NShards
	return shard
}
