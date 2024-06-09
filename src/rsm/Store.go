package rsm

import (
	"6.5840/utils"
	"github.com/alphadose/haxmap"
)

type Key interface {
	~int | ~int32 | ~int64 | ~string
}

type Store[key Key, value any] struct {
	waitCh   *haxmap.Map[string, *chan OpState] // stores wait channels for goroutines waiting on quorum
	ackStore *haxmap.Map[string, OpState]       // stores status of an ongoing/completed operation
	kvStore  *haxmap.Map[key, value]            // key value pair store
}

func (st *Store[Key, Value]) GetValue(key Key) Value {
	// returns zeroValue if not exists
	value, _ := st.kvStore.Get(key)
	return value
}

func (st *Store[Key, Value]) getKvStore() *haxmap.Map[Key, Value] {
	return st.kvStore
}

func (st *Store[Key, Value]) getAckStore() *haxmap.Map[string, OpState] {
	return st.ackStore
}

func (st *Store[Key, Value]) SetValue(key Key, value Value) {
	st.kvStore.Set(key, value)
}

func (st *Store[Key, Value]) getAckStage(ackKey string) (OpState, bool) {
	return st.ackStore.Get(ackKey)
}

func (st *Store[Key, Value]) getOrCreateWaitChan(ackKey string) *chan OpState {
	waitCh := make(chan OpState, 1)
	ch, _ := st.waitCh.GetOrSet(ackKey, &waitCh)
	return ch
}

func (st *Store[Key, Value]) cleanRequest(ackKey string) {
	st.ackStore.Del(ackKey)
	st.waitCh.Del(ackKey)
}

func (st *Store[Key, Value]) createRequest(ackKey string) {
	// order matters here as we first create wait chan and then mark it as started
	st.getOrCreateWaitChan(ackKey)   // create wait object for this key
	st.ackStore.Set(ackKey, STARTED) // mark started
}

func (st *Store[Key, Value]) completeRequest(ackKey string) bool {
	st.ackStore.Set(ackKey, COMPLETED) // mark completed
	waitCh := st.getOrCreateWaitChan(ackKey)
	// this will not block if previous ack is not already consumed
	return utils.NonBlockingPut(*waitCh, COMPLETED)
}

func (st *Store[Key, Value]) abortRequest(ackKey string) bool {
	aborted := st.ackStore.CompareAndSwap(ackKey, STARTED, ABORTED) // mark aborted
	if aborted {
		waitCh := st.getOrCreateWaitChan(ackKey)
		*waitCh <- ABORTED
	}

	return aborted
}
