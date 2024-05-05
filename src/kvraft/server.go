package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/utils"
	"fmt"
	"github.com/alphadose/haxmap"
	"strconv"
	"sync/atomic"
)

type Store struct {
	waitCh   *haxmap.Map[string, *chan OpState] // stores wait channels for goroutines waiting on quorum
	ackStore *haxmap.Map[string, OpState]       // stores status of an ongoing/completed operation
	kvStore  *haxmap.Map[string, string]        // key value pair store
}

func (st *Store) getValue(key string) string {
	value, exists := st.kvStore.Get(key)
	if !exists {
		value = ""
	}
	return value
}

func (st *Store) getKvStore() *haxmap.Map[string, string] {
	return st.kvStore
}

func (st *Store) getAckStore() *haxmap.Map[string, OpState] {
	return st.ackStore
}

func (st *Store) setValue(key, value string) {
	st.kvStore.Set(key, value)
}

func (st *Store) getAckStage(ackKey string) (OpState, bool) {
	return st.ackStore.Get(ackKey)
}

func (st *Store) getOrCreateWaitChan(ackKey string) *chan OpState {
	waitCh := make(chan OpState, 1)
	ch, _ := st.waitCh.GetOrSet(ackKey, &waitCh)
	return ch
}

func (st *Store) cleanRequest(ackKey string) {
	st.ackStore.Del(ackKey)
	st.waitCh.Del(ackKey)
}

func (st *Store) createRequest(ackKey string) {
	// order matters here as we first create wait chan and then mark it as started
	st.getOrCreateWaitChan(ackKey)   // create wait object for this key
	st.ackStore.Set(ackKey, STARTED) // mark started
}

func (st *Store) completeRequest(ackKey string) bool {
	st.ackStore.Set(ackKey, COMPLETED) // mark completed
	waitCh := st.getOrCreateWaitChan(ackKey)
	// this will not block if previous ack is not already consumed
	select {
	case *waitCh <- COMPLETED:
		return true
	default:
		return false
	}
}

func (st *Store) abortRequest(ackKey string) bool {
	aborted := st.ackStore.CompareAndSwap(ackKey, STARTED, ABORTED) // mark aborted
	if aborted {
		waitCh := st.getOrCreateWaitChan(ackKey)
		*waitCh <- ABORTED
	}

	return aborted
}

type KVServer struct {
	utils.Logger
	store          atomic.Pointer[Store]
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	dead           int32 // set by Kill()
	maxRaftState   int   // snapshot if log grows this big
	lastAppliedIdx int   // raftLog index of last applied ApplyMsg
}

func (kv *KVServer) MakeStore(kvStore *haxmap.Map[string, string], ackStore *haxmap.Map[string, OpState], waitChan *haxmap.Map[string, *chan OpState]) {
	newStore := &Store{
		kvStore:  kvStore,
		ackStore: ackStore,
		waitCh:   waitChan,
	}

	kv.store.Store(newStore)
}

func (kv *KVServer) getStore() *Store {
	return kv.store.Load()
}

func (kv *KVServer) getLastAppliedIdx() int {
	return kv.lastAppliedIdx
}

func (kv *KVServer) setLastAppliedIdx(newIdx int) {
	if newIdx <= kv.lastAppliedIdx {
		kv.LogPanic("Reapplied already applied idx:", newIdx, "over lastApplied idx:", kv.lastAppliedIdx)
	}
	kv.lastAppliedIdx = newIdx
}

func (kv *KVServer) getAckKey(clientId, opId int64) string {
	return strconv.Itoa(int(clientId)) + "," + strconv.Itoa(int(opId))
}

func (kv *KVServer) getPreviousAckKey(key string) string {
	var clientId, opId int64
	_, err := fmt.Sscanf(key+"~", "%d,%d~", &clientId, &opId)
	if err != nil {
		kv.LogPanic("Received ill-formated ack key :", key)
	}
	return kv.getAckKey(clientId, opId-1)
}

func (kv *KVServer) convertToRaftCommandForPut(args *PutAppendArgs) RaftCommand {
	return RaftCommand{
		OpType:   args.Op,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Key:      args.Key,
		Value:    args.Value,
	}
}

func (kv *KVServer) convertToRaftCommandForGet(args *GetArgs) RaftCommand {
	return RaftCommand{
		OpType:   GET,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Key:      args.Key,
	}
}

func (kv *KVServer) HandleGet(args *GetArgs, reply *GetReply) {
	kv.LogDebug("Received Get for args", *args)

	_, err := kv.startQuorum(kv.convertToRaftCommandForGet(args))
	reply.Err = err
	reply.LeaderId = kv.rf.GetLeaderPeerIndex()
	if err == OK {
		reply.Value = kv.getStore().getValue(args.Key)
	}
}

func (kv *KVServer) HandlePutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.LogDebug("Received PutAppend for args", *args)

	_, err := kv.startQuorum(kv.convertToRaftCommandForPut(args))
	reply.Err = err
	reply.LeaderId = kv.rf.GetLeaderPeerIndex()
}

func (kv *KVServer) startQuorum(command RaftCommand) (bool, Err) {
	ackKey := kv.getAckKey(command.ClientId, command.OpId)
	// check if this command is already ack-ed
	if stage, ackExists := kv.getStore().getAckStage(ackKey); ackExists && stage == COMPLETED {
		return true, OK
	}

	// submit to raft for reaching quorum
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		return false, WRONG_LEADER
	}

	kv.getStore().createRequest(ackKey)
	kv.LogInfo("Initiated quorum for key", ackKey, "command:", *(&command), "submitted at index:", index)
	return kv.finishQuorum(ackKey, term)
}

func (kv *KVServer) finishQuorum(ackKey string, quorumTerm int) (bool, Err) {
	kv.LogDebug("Started wait for quorum on key:", ackKey)

	currentTerm, _ := kv.rf.GetStatus()
	if quorumTerm != currentTerm && kv.getStore().abortRequest(ackKey) {
		kv.LogDebug("Aborting quorum on key:", ackKey)
	}

	waitCh := kv.getStore().getOrCreateWaitChan(ackKey)
	operationStage := <-*waitCh
	kv.LogDebug("Finished wait for quorum on key:", ackKey, "with stage", operationStage)
	if operationStage != COMPLETED {
		return false, WRONG_LEADER
	}

	return true, OK
}

func (kv *KVServer) shouldSnapshot() bool {
	return kv.maxRaftState != -1 && kv.rf.HasReachedSizeThreshold(kv.maxRaftState)
}

func (kv *KVServer) triggerSnapshot() bool {
	snapshot := utils.IntToBytes(kv.getLastAppliedIdx())

	if kvStoreBytes, err := kv.getStore().getKvStore().MarshalJSON(); err != nil {
		kv.LogPanic("Failed to serialize current kvStore", err)
	} else {
		snapshot = append(snapshot, utils.IntToBytes(len(kvStoreBytes))...)
		snapshot = append(snapshot, kvStoreBytes...)
	}

	if ackStoreBytes, err := kv.getStore().getAckStore().MarshalJSON(); err != nil {
		kv.LogPanic("Failed to serialize current ackStore", err)
	} else {
		snapshot = append(snapshot, ackStoreBytes...)
	}

	kv.LogInfo("Triggering snapshot till index", kv.getLastAppliedIdx())
	return kv.rf.Snapshot(kv.getLastAppliedIdx(), snapshot)
}

func (kv *KVServer) processSnapshot(msg raft.ApplyMsg) {
	snapshotBytes := msg.Snapshot
	intSize := 8

	snapshotIdx := utils.BytesToInt(snapshotBytes[:intSize])
	if snapshotIdx < kv.getLastAppliedIdx() {
		kv.LogError("Trying to install state snapshot till index:", snapshotIdx, "whereas RSM has applied indexes upto:", kv.getLastAppliedIdx())
		return
	}

	kvStoreSize := utils.BytesToInt(snapshotBytes[intSize : 2*intSize])
	kv.LogInfo("Applying snapshot till index:", snapshotIdx)

	kvStoreBytes := snapshotBytes[2*intSize : (2*intSize)+kvStoreSize]
	kvStore := haxmap.New[string, string]()
	if err := kvStore.UnmarshalJSON(kvStoreBytes); err != nil {
		kv.LogPanic("Failed to deserialize kvStore", err)
	}

	ackStoreBytes := snapshotBytes[(2*intSize)+kvStoreSize:]
	ackStore := haxmap.New[string, OpState]()
	if err := ackStore.UnmarshalJSON(ackStoreBytes); err != nil {
		kv.LogPanic("Failed to deserialize ackStore", err)
	}

	// use currently existing waitChan map, as there might be few requests that might still be waiting
	waitCh := kv.getStore().waitCh
	ackStore.ForEach(func(ackKey string, state OpState) bool {
		if state != STARTED {
			waitChan := kv.getStore().getOrCreateWaitChan(ackKey)
			*waitChan <- state
		}
		return true
	})

	kv.MakeStore(kvStore, ackStore, waitCh)
}

func (kv *KVServer) processCommand(msg raft.ApplyMsg) {
	command := msg.Command.(RaftCommand)
	ackKey := kv.getAckKey(command.ClientId, command.OpId)

	if stage, exists := kv.getStore().getAckStage(ackKey); exists && stage == COMPLETED {
		kv.LogWarn("Skipping completed key", ackKey, "command :", command)
	} else {
		kv.LogDebug("Applying command with key:", ackKey, "to state Machine. Msg :", command)
		switch command.OpType {
		case PUT:
			kv.getStore().setValue(command.Key, command.Value)
		case APPEND:
			value := kv.getStore().getValue(command.Key)
			value += command.Value
			kv.getStore().setValue(command.Key, value)
		case GET:
			// do nothing
		}
	}

	if !kv.getStore().completeRequest(ackKey) { // complete current request
		kv.LogDebug("Failed to complete key:", ackKey, "as previous ack event is not consumed yet")
	}

	kv.getStore().cleanRequest(kv.getPreviousAckKey(ackKey)) // clean up previous request meta data from same client
}

func (kv *KVServer) processCommittedMsg() {

	for kv.killed() == false {
		applyMsg := <-kv.applyCh

		if applyMsg.SnapshotValid {
			kv.processSnapshot(applyMsg)
			kv.setLastAppliedIdx(applyMsg.SnapshotIndex)
		}

		if applyMsg.CommandValid {
			kv.processCommand(applyMsg)
			kv.setLastAppliedIdx(applyMsg.CommandIndex)
		}

		if kv.shouldSnapshot() && !kv.triggerSnapshot() {
			kv.LogError("Failed to snapshot current state to raft")
		}
	}
}

func (kv *KVServer) listenRaftState() {
	for {
		term := <-kv.rf.ListenTermChanges()
		kv.LogDebug("Raft term changed to:", term, "Aborting all awaited client request")
		// term changed : abort all ongoing client requests
		kv.getStore().getAckStore().ForEach(func(ackKey string, state OpState) bool {
			if state == STARTED && kv.getStore().abortRequest(ackKey) {
				kv.LogDebug("Aborted command :", ackKey)
			}
			return true
		})
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should kvStore snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(RaftCommand{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.MakeStore(haxmap.New[string, string](), haxmap.New[string, OpState](), haxmap.New[string, *chan OpState]())
	kv.Logger = utils.GetLogger("server_logLevel", func() string {
		return "[SERVER] [Peer : " + strconv.Itoa(me) + "] "
	})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.processCommittedMsg()

	go kv.listenRaftState()

	return kv
}
