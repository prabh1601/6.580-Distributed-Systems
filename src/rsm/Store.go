package rsm

import (
	"6.5840/utils"
	"fmt"
	"github.com/alphadose/haxmap"
	"strconv"
)

type Key interface {
	~int | ~int32 | ~int64 | ~string
}

type Store[key Key, value any] struct {
	// transient field
	mostRecentClientOp *haxmap.Map[int64, int64] // stores most recent client op, required for clean up

	waitCh   *haxmap.Map[string, *chan OpState] // stores wait channels for goroutines waiting on quorum
	ackStore *haxmap.Map[string, OpState]       // stores status of an ongoing/completed operation

	kvStore []*haxmap.Map[key, value] // key value pair store
}

func getAckKey(clientId, opId int64) string {
	return "(" + strconv.Itoa(int(clientId)) + "," + strconv.Itoa(int(opId)) + ")"
}

func getClientAndOpId(ackKey string) (int64, int64) {
	var clientId, opId int64
	_, err := fmt.Sscanf(ackKey, "(%d,%d)", &clientId, &opId)
	if err != nil {
		panic("Received ill-formatted ack key : " + ackKey)
	}

	return clientId, opId
}

func (st *Store[key, value]) marshallStore() ([]byte, error) {
	var bytes []byte
	for shardNum := 0; shardNum < utils.NShards; shardNum++ {
		if shardBytes, err := st.kvStore[shardNum].MarshalJSON(); err != nil {
			return nil, err
		} else {
			bytes = append(bytes, utils.IntToBytes(len(shardBytes))...)
			bytes = append(bytes, shardBytes...)
		}
	}

	return bytes, nil
}

func unmarshallKvStore[key Key, value any](storeBytes []byte) ([]*haxmap.Map[key, value], error) {
	kvStore := make([]*haxmap.Map[key, value], 10)
	offset := 0
	for shardNum := 0; shardNum < utils.NShards; shardNum++ {
		shardMap := haxmap.New[key, value]()
		shardMapSize := utils.BytesToInt(storeBytes[offset : offset+utils.INT_SIZE])
		offset += utils.INT_SIZE
		shardMap.UnmarshalJSON(storeBytes[offset : offset+shardMapSize])
		offset += shardMapSize
		kvStore[shardNum] = shardMap
	}

	return kvStore, nil
}

func (st *Store[Key, Value]) GetValue(key Key) Value {
	// returns zeroValue if not exists
	value, _ := st.GetShardStore(key).Get(key)
	return value
}

func (st *Store[Key, Value]) GetShardStore(key Key) *haxmap.Map[Key, Value] {
	return st.kvStore[utils.Key2shard(string(key))]
}

func (st *Store[Key, Value]) getAckStore() *haxmap.Map[string, OpState] {
	return st.ackStore
}

func (st *Store[Key, Value]) SetValue(key Key, value Value) {
	st.GetShardStore(key).Set(key, value)
}

func (st *Store[Key, Value]) getAckStage(ackKey string) (OpState, bool) {
	return st.ackStore.Get(ackKey)
}

func (st *Store[Key, Value]) getOrCreateWaitChan(ackKey string) *chan OpState {
	waitCh := make(chan OpState, 1)
	ch, _ := st.waitCh.GetOrSet(ackKey, &waitCh)
	return ch
}

func (st *Store[key, value]) markClientOperation(ackKey string) {
	clientId, opId := getClientAndOpId(ackKey)
	st.mostRecentClientOp.Set(clientId, opId)
}

func (st *Store[Key, Value]) cleanRequest(ackKey string) {
	st.ackStore.Del(ackKey)
	st.waitCh.Del(ackKey)
}

func (st *Store[key, value]) cleanPreviousClientRequest(ackKey string) {
	// remove previous request from this client
	clientId, curOpId := getClientAndOpId(ackKey)
	prevOpId, exists := st.mostRecentClientOp.Get(clientId)
	if exists && curOpId != prevOpId {
		st.cleanRequest(getAckKey(clientId, prevOpId))
	}
}

func (st *Store[Key, Value]) createRequest(ackKey string) {
	// order matters here as we first create wait chan and then mark it as started
	st.getOrCreateWaitChan(ackKey)   // create wait object for this key
	st.ackStore.Set(ackKey, STARTED) // mark started
}

func (st *Store[Key, Value]) completeRequest(ackKey string) bool {
	st.cleanPreviousClientRequest(ackKey)
	st.markClientOperation(ackKey)

	st.ackStore.Set(ackKey, COMPLETED) // mark completed
	waitCh := st.getOrCreateWaitChan(ackKey)
	// this will not block if previous ack is not already consumed

	return utils.NonBlockingPut(*waitCh, COMPLETED)
}

func (st *Store[Key, Value]) abortRequest(ackKey string) bool {

	aborted := st.ackStore.CompareAndSwap(ackKey, STARTED, ABORTED) // mark aborted
	if aborted {
		// only clean previous operation
		// dont mark this operation for client, only completed operations will be marked
		// it might happen that a lower client operation is already done, but this server has not yet committed that yet
		// this operation will get marked
		st.cleanPreviousClientRequest(ackKey)
		waitCh := st.getOrCreateWaitChan(ackKey)
		*waitCh <- ABORTED
	}

	return aborted
}
