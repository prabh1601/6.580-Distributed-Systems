package rsm

import (
	"6.5840/labgob"
	"6.5840/utils"
	"bytes"
	"fmt"
	"github.com/alphadose/haxmap"
	"log"
	"strconv"
	"sync/atomic"
)

type Key interface {
	~int | ~int32 | ~int64 | ~string
}

type SeriazableShardData struct {
	State      ShardState
	OngoingOps int64
}

type ShardData struct {
	State      atomic.Pointer[ShardState] // local shard state for the server
	OngoingOps atomic.Int64
}

type Store[key Key, value any] struct {
	// transient field
	mostRecentClientOp *haxmap.Map[int64, string]         // stores most recent client op, required for clean up
	waitCh             *haxmap.Map[string, *chan OpState] // stores wait channels for goroutines waiting on quorum

	ackStore      *haxmap.Map[string, OpState] // stores status of an ongoing/completed operation
	kvStore       []*haxmap.Map[key, value]    // key value pair store
	shardMetadata []ShardData                  // shard level metadata store
}

func getAckKey(clientId, opId, shardNum int64) string {
	return "(" + strconv.Itoa(int(clientId)) + "," + strconv.Itoa(int(opId)) + "," + strconv.Itoa(int(shardNum)) + ")"
}

func getAckKeyConstituents(ackKey string) (int64, int64, int64) {
	var clientId, opId, shardNum int64
	_, err := fmt.Sscanf(ackKey, "(%d,%d,%d)", &clientId, &opId, &shardNum)
	if err != nil {
		log.Panic("Received ill-formatted ack key : ", ackKey)
	}

	return clientId, opId, shardNum
}

func (st *Store[key, value]) marshallKvStore() ([]byte, error) {
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

func (st *Store[key, value]) unmarshallKvStore(storeBytes []byte) ([]*haxmap.Map[key, value], error) {
	kvStore := make([]*haxmap.Map[key, value], 10)
	offset := 0
	for shardNum := 0; shardNum < utils.NShards; shardNum++ {
		shardMap := haxmap.New[key, value]()
		shardMapSize := utils.GetIntFromBytes(storeBytes, &offset)
		if err := shardMap.UnmarshalJSON(utils.GetChunkFromBytes(storeBytes, &offset, shardMapSize)); err != nil {
			log.Panic("Failed to unmarshall json", err)
		}
		kvStore[shardNum] = shardMap
	}

	return kvStore, nil
}

func (st *Store[key, value]) marshallShardMetadata() ([]byte, error) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	serializableMetaData := make([]SeriazableShardData, 0)
	for shard := 0; shard < utils.NShards; shard++ {
		serializableMetaData = append(serializableMetaData, SeriazableShardData{State: st.GetShardState(shard), OngoingOps: st.getOngoingShardOps(shard)})
	}

	if err := e.Encode(serializableMetaData); err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func (st *Store[key, value]) unmarshallShardMetadata(dataBytes []byte) []ShardData {
	r := bytes.NewBuffer(dataBytes)
	d := labgob.NewDecoder(r)

	var serializableMetaData []SeriazableShardData
	if err := d.Decode(&serializableMetaData); err != nil {
		log.Panic("Failed to unmarshall serializableMetaData", err)
	}

	shardMetadata := make([]ShardData, utils.NShards)
	for shard := 0; shard < utils.NShards; shard++ {
		serializedData := serializableMetaData[shard]
		shardMetadata[shard].State.Store(&serializedData.State)
		shardMetadata[shard].OngoingOps.Store(serializedData.OngoingOps)
	}

	return shardMetadata
}

func (st *Store[key, value]) GetShardState(shardNum int) ShardState {
	return *st.shardMetadata[shardNum].State.Load()
}

func (st *Store[key, value]) MarkShardState(shardNum int, state ShardState) {
	st.shardMetadata[shardNum].State.Store(&state)
}

func (st *Store[key, value]) getOngoingShardOps(shardNum int) int64 {
	return st.shardMetadata[shardNum].OngoingOps.Load()
}

func (st *Store[key, value]) addShardOp(shardNum int, opCount int64) {
	st.shardMetadata[shardNum].OngoingOps.Add(opCount)
}

func (st *Store[key, value]) incrementShardOp(shardNum int) {
	st.addShardOp(shardNum, 1)
}

func (st *Store[key, value]) decrementShardOp(shardNum int) {
	st.addShardOp(shardNum, -1)
}

func (st *Store[Key, Value]) GetValue(key Key) Value {
	// returns zeroValue if not exists
	value, _ := st.GetShard(key).Get(key)
	return value
}

func (st *Store[Key, Value]) GetShard(key Key) *haxmap.Map[Key, Value] {
	return st.kvStore[utils.Key2shard(string(key))]
}

func (st *Store[Key, Value]) getAckStore() *haxmap.Map[string, OpState] {
	return st.ackStore
}

func (st *Store[Key, Value]) SetValue(key Key, value Value) {
	st.GetShard(key).Set(key, value)
}

func (st *Store[Key, Value]) getAckStage(ackKey string) (OpState, bool) {
	return st.ackStore.Get(ackKey)
}

func (st *Store[Key, Value]) getOrCreateWaitChan(ackKey string) *chan OpState {
	waitCh := make(chan OpState, 1)
	ch, exists := st.waitCh.GetOrSet(ackKey, &waitCh)
	if !exists {
		//_, _, shardNum := getAckKeyConstituents(ackKey)
		//st.incrementShardOp(shardNum)
	}
	return ch
}

func (st *Store[key, value]) markClientOperation(ackKey string) {
	clientId, _, _ := getAckKeyConstituents(ackKey)
	st.mostRecentClientOp.Set(clientId, ackKey)
}

func (st *Store[Key, Value]) cleanRequest(ackKey string) {
	st.ackStore.Del(ackKey)
	st.waitCh.Del(ackKey)
}

func (st *Store[key, value]) cleanPreviousClientRequest(ackKey string) {
	// remove previous request from this client
	clientId, _, _ := getAckKeyConstituents(ackKey)
	prevAckKey, exists := st.mostRecentClientOp.Get(clientId)
	if exists && ackKey != prevAckKey {
		st.cleanRequest(prevAckKey)
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

	//_, _, shardNum := getAckKeyConstituents(ackKey)
	//st.decrementShardOp(shardNum)

	st.ackStore.Set(ackKey, COMPLETED) // mark completed

	// this will not block if previous ack is not already consumed
	waitCh := st.getOrCreateWaitChan(ackKey)
	return utils.NonBlockingPut(*waitCh, COMPLETED)
}

func (st *Store[Key, Value]) abortRequest(ackKey string) bool {

	aborted := st.ackStore.CompareAndSwap(ackKey, STARTED, ABORTED) // mark aborted
	if aborted {
		// only clean previous operation
		// dont mark this operation for client as most recent operation, only completed operations will be marked
		// it might happen that a lower client operation is already done, but this server has not yet committed that yet
		// this operation will get marked
		st.cleanPreviousClientRequest(ackKey)
		waitCh := st.getOrCreateWaitChan(ackKey)
		*waitCh <- ABORTED
	}

	return aborted
}
