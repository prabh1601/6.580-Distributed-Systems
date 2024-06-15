package rsm

import (
	"6.5840/labgob"
	"6.5840/raft"
	"6.5840/utils"
	"fmt"
	"github.com/alphadose/haxmap"
	"strconv"
	"strings"
	"sync/atomic"
)

type ReplicatedStateMachine[key Key, value any, RaftCommandValue any] struct {
	utils.Logger
	store            atomic.Pointer[Store[key, value]]
	rf               *raft.Raft
	dead             int32 // set by Kill()
	maxRaftState     int   // snapshot if log grows this big
	lastAppliedIdx   int   // raftLog index of last applied ApplyMsg
	commandProcessor CommandProcessor[key, RaftCommandValue]
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) GetLeaderPeerIndex() int {
	return rsm.rf.GetLeaderPeerIndex()
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) MakeStore(kvStore *haxmap.Map[Key, Value], ackStore *haxmap.Map[string, OpState], waitChan *haxmap.Map[string, *chan OpState]) {
	newStore := &Store[Key, Value]{
		kvStore:  kvStore,
		ackStore: ackStore,
		waitCh:   waitChan,
	}

	rsm.store.Store(newStore)
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) GetStore() *Store[Key, Value] {
	return rsm.store.Load()
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) getLastAppliedIdx() int {
	return rsm.lastAppliedIdx
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) setLastAppliedIdx(newIdx int) {
	if newIdx <= rsm.lastAppliedIdx {
		rsm.LogPanic("Reapplied already applied idx:", newIdx, "over lastApplied idx:", rsm.lastAppliedIdx)
	}
	rsm.lastAppliedIdx = newIdx
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) getAckKey(clientId, opId int64) string {
	return strconv.Itoa(int(clientId)) + "," + strconv.Itoa(int(opId))
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) getPreviousAckKey(key string) string {
	var clientId, opId int64
	_, err := fmt.Sscanf(key+"~", "%d,%d~", &clientId, &opId)
	if err != nil {
		rsm.LogPanic("Received ill-formatted ack key :", key)
	}
	return rsm.getAckKey(clientId, opId-1)
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) StartQuorum(command RaftCommand[Key, RaftCommandValue]) (bool, Err) {
	ackKey := rsm.getAckKey(command.ClientId, command.OpId)
	// check if this command is already ack-ed
	if stage, ackExists := rsm.GetStore().getAckStage(ackKey); ackExists && stage == COMPLETED {
		return true, Ok
	}

	// submit to raft for reaching quorum
	index, term, isLeader := rsm.rf.Start(command)
	if !isLeader {
		return false, WrongLeader
	}

	rsm.GetStore().createRequest(ackKey)
	rsm.LogInfo("Initiated quorum for key", ackKey, "command:", *(&command), "submitted at index:", index)
	return rsm.finishQuorum(ackKey, term)
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) finishQuorum(ackKey string, quorumTerm int) (bool, Err) {
	rsm.LogDebug("Started wait for quorum on key:", ackKey)

	currentTerm, _ := rsm.rf.GetStatus()
	if quorumTerm != currentTerm && rsm.GetStore().abortRequest(ackKey) {
		rsm.LogDebug("Aborting quorum on key:", ackKey)
	}

	waitCh := rsm.GetStore().getOrCreateWaitChan(ackKey)
	operationStage := <-*waitCh
	rsm.LogDebug("Finished wait for quorum on key:", ackKey, "with stage", operationStage)
	if operationStage != COMPLETED {
		return false, WrongLeader
	}

	return true, Ok
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) shouldSnapshot() bool {
	return rsm.maxRaftState != -1 && rsm.rf.HasReachedSizeThreshold(rsm.maxRaftState)
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) triggerSnapshot() bool {
	snapshot := utils.IntToBytes(rsm.getLastAppliedIdx())

	if kvStoreBytes, err := rsm.GetStore().GetKvStore().MarshalJSON(); err != nil {
		rsm.LogPanic("Failed to serialize current kvStore", err)
	} else {
		snapshot = append(snapshot, utils.IntToBytes(len(kvStoreBytes))...)
		snapshot = append(snapshot, kvStoreBytes...)
	}

	if ackStoreBytes, err := rsm.GetStore().getAckStore().MarshalJSON(); err != nil {
		rsm.LogPanic("Failed to serialize current ackStore", err)
	} else {
		snapshot = append(snapshot, ackStoreBytes...)
	}

	rsm.LogInfo("Triggering snapshot till index", rsm.getLastAppliedIdx())
	return rsm.rf.Snapshot(rsm.getLastAppliedIdx(), snapshot)
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) processSnapshot(msg raft.ApplyMsg) {
	snapshotBytes := msg.Snapshot
	intSize := 8

	snapshotIdx := utils.BytesToInt(snapshotBytes[:intSize])
	if snapshotIdx < rsm.getLastAppliedIdx() {
		rsm.LogError("Trying to install state snapshot till index:", snapshotIdx, "whereas RSM has applied indexes upto:", rsm.getLastAppliedIdx())
		return
	}

	kvStoreSize := utils.BytesToInt(snapshotBytes[intSize : 2*intSize])
	rsm.LogInfo("Applying snapshot till index:", snapshotIdx)

	kvStoreBytes := snapshotBytes[2*intSize : (2*intSize)+kvStoreSize]
	kvStore := haxmap.New[Key, Value]()
	if err := kvStore.UnmarshalJSON(kvStoreBytes); err != nil {
		rsm.LogPanic("Failed to deserialize kvStore", err)
	}

	ackStoreBytes := snapshotBytes[(2*intSize)+kvStoreSize:]
	ackStore := haxmap.New[string, OpState]()
	if err := ackStore.UnmarshalJSON(ackStoreBytes); err != nil {
		rsm.LogPanic("Failed to deserialize ackStore", err)
	}

	// use currently existing waitChan map, as there might be few requests that might still be waiting
	waitCh := rsm.GetStore().waitCh
	ackStore.ForEach(func(ackKey string, state OpState) bool {
		if state != STARTED {
			waitChan := rsm.GetStore().getOrCreateWaitChan(ackKey)
			*waitChan <- state
		}
		return true
	})

	rsm.MakeStore(kvStore, ackStore, waitCh)
}

func _postSnapshotProcess[key Key, value any](processor CommandProcessor[key, value]) {
	processor.PostSnapshotProcess()
}

func _processCommand[key Key, value any](processor CommandProcessor[key, value], command RaftCommand[key, value]) {
	processor.ProcessCommandInternal(command)
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) processCommand(msg raft.ApplyMsg) {
	command := msg.Command.(RaftCommand[Key, RaftCommandValue])
	ackKey := rsm.getAckKey(command.ClientId, command.OpId)

	if stage, exists := rsm.GetStore().getAckStage(ackKey); exists && stage == COMPLETED {
		rsm.LogWarn("Skipping completed key", ackKey, "command :", command)
	} else {
		rsm.LogDebug("Applying command with key:", ackKey, "to state Machine. Msg :", command)
		_processCommand[Key, RaftCommandValue](rsm.commandProcessor, command)
	}

	if !rsm.GetStore().completeRequest(ackKey) { // complete current request
		rsm.LogDebug("Failed to complete key:", ackKey, "as previous ack event is not consumed yet")
	}

	rsm.GetStore().cleanRequest(rsm.getPreviousAckKey(ackKey)) // clean up previous request meta data from same client
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) processCommittedMsg() {

	for rsm.killed() == false {
		applyMsg := <-rsm.rf.GetAppliedChan()
		rsm.LogDebug("Processing committed msg :", applyMsg)

		if applyMsg.SnapshotValid {
			rsm.processSnapshot(applyMsg)
			rsm.setLastAppliedIdx(applyMsg.SnapshotIndex)
		}

		if applyMsg.CommandValid {
			rsm.processCommand(applyMsg)
			rsm.setLastAppliedIdx(applyMsg.CommandIndex)
		}

		if rsm.shouldSnapshot() && !rsm.triggerSnapshot() {
			rsm.LogError("Failed to snapshot current state to raft")
		}
	}
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) listenRaftState() {
	for {
		term := <-rsm.rf.ListenTermChanges()
		rsm.LogDebug("Raft term changed to:", term, "Aborting all awaited client request")
		// term changed : abort all ongoing client requests
		rsm.GetStore().getAckStore().ForEach(func(ackKey string, state OpState) bool {
			if state == STARTED && rsm.GetStore().abortRequest(ackKey) {
				rsm.LogDebug("Aborted command :", ackKey)
			}
			return true
		})
	}
}

// Kill the tester calls Kill() when a ReplicatedStateMachine[Key, Value, RaftCommandValue] instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)
	rsm.rf.Kill()
	// Your code here, if desired.
}

func (rsm *ReplicatedStateMachine[Key, Value, RaftCommandValue]) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

// StartReplicatedStateMachine[Key, Value, RaftCommandValue] servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// 'me' is the index of the current server in servers[].
// the k/v server should kvStore snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartReplicatedStateMachine[Key, Value, RaftCommandValue]() must return quickly, so it should start goroutines
// for any long-running work.
func StartReplicatedStateMachine[key Key, value any, raftCommandValue any](name string, me int, maxRaftState int, rf *raft.Raft, cmdProcessor CommandProcessor[key, raftCommandValue]) *ReplicatedStateMachine[key, value, raftCommandValue] {
	labgob.Register(RaftCommand[key, raftCommandValue]{})

	rsm := new(ReplicatedStateMachine[key, value, raftCommandValue])
	rsm.maxRaftState = maxRaftState
	rsm.commandProcessor = cmdProcessor
	rsm.MakeStore(haxmap.New[key, value](), haxmap.New[string, OpState](), haxmap.New[string, *chan OpState]())
	rsm.Logger = utils.GetLogger("rsm_logLevel", func() string {
		return "[" + strings.ToUpper(name) + "] [RSM] [Peer : " + strconv.Itoa(me) + "] "
	})

	rsm.rf = rf

	go rsm.processCommittedMsg()
	go rsm.listenRaftState()

	return rsm
}
