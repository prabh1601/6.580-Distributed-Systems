package rsm

import (
	"6.5840/labgob"
	"6.5840/raft"
	"6.5840/utils"
	"github.com/alphadose/haxmap"
	"strconv"
	"strings"
	"sync/atomic"
)

type ReplicatedStateMachine[key Key, value any] struct {
	utils.Logger
	store            atomic.Pointer[Store[key, value]]
	rf               *raft.Raft
	dead             int32 // set by Kill()
	maxRaftState     int   // snapshot if log grows this big
	lastAppliedIdx   int   // raftLog index of last applied ApplyMsg
	commandProcessor CommandProcessor[key]
}

func (rsm *ReplicatedStateMachine[Key, Value]) GetLeaderPeerIndex() int {
	return rsm.rf.GetLeaderPeerIndex()
}

func (rsm *ReplicatedStateMachine[Key, Value]) MakeStore(kvStore []*haxmap.Map[Key, Value], ackStore *haxmap.Map[string, OpState], waitChan *haxmap.Map[string, *chan OpState], shardMetadata []ShardData) {
	newStore := &Store[Key, Value]{
		kvStore:            kvStore,
		ackStore:           ackStore,
		waitCh:             waitChan,
		shardMetadata:      shardMetadata,
		mostRecentClientOp: haxmap.New[int64, string](),
	}

	rsm.store.Store(newStore)
}

func (rsm *ReplicatedStateMachine[Key, Value]) GetStore() *Store[Key, Value] {
	return rsm.store.Load()
}

func (rsm *ReplicatedStateMachine[Key, Value]) getLastAppliedIdx() int {
	return rsm.lastAppliedIdx
}

func (rsm *ReplicatedStateMachine[Key, Value]) setLastAppliedIdx(newIdx int) {
	if newIdx <= rsm.lastAppliedIdx {
		rsm.LogPanic("Reapplied already applied idx:", newIdx, "over lastApplied idx:", rsm.lastAppliedIdx)
	}
	rsm.lastAppliedIdx = newIdx
}

func (rsm *ReplicatedStateMachine[Key, Value]) SubmitInternalReconfigurations(command RaftCommand[Key]) (bool, Err) {
	// submit to raft for reaching quorum, dont care if this command gets lost
	_, _, isLeader := rsm.rf.Start(command)
	if !isLeader {
		return false, WrongLeader
	}
	return true, Ok
}

func (rsm *ReplicatedStateMachine[Key, Value]) StartQuorum(command RaftCommand[Key]) (bool, Err) {
	shardNum := int64(utils.Key2shard(string(command.Key)))
	ackKey := getAckKey(command.ClientId, command.OpId, shardNum)
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

func (rsm *ReplicatedStateMachine[Key, Value]) finishQuorum(ackKey string, quorumTerm int) (bool, Err) {
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

func (rsm *ReplicatedStateMachine[Key, Value]) shouldSnapshot() bool {
	return rsm.maxRaftState != -1 && rsm.rf.HasReachedSizeThreshold(rsm.maxRaftState)
}

func (rsm *ReplicatedStateMachine[Key, Value]) triggerSnapshot() bool {

	// marshall last applied idx
	snapshotIdx := rsm.getLastAppliedIdx()
	snapshot := utils.IntToBytes(snapshotIdx)

	// marshall ack store
	if ackStoreBytes, err := rsm.GetStore().getAckStore().MarshalJSON(); err != nil {
		rsm.LogPanic("Failed to serialize current ackStore", err)
	} else {
		snapshot = append(snapshot, utils.IntToBytes(len(ackStoreBytes))...)
		snapshot = append(snapshot, ackStoreBytes...)
	}

	// marshall shardMetadata
	if metadataBytes, err := rsm.GetStore().marshallShardMetadata(); err != nil {
		rsm.LogPanic("Failed to serialize current shardMetadata", err)
	} else {
		snapshot = append(snapshot, utils.IntToBytes(len(metadataBytes))...)
		snapshot = append(snapshot, metadataBytes...)
	}

	// marshall kv store
	if kvStoreBytes, err := rsm.GetStore().marshallKvStore(); err != nil {
		rsm.LogPanic("Failed to serialize current kvStore", err)
	} else {
		snapshot = append(snapshot, kvStoreBytes...)
	}

	rsm.LogInfo("Triggering snapshot till index", snapshot)
	return rsm.rf.Snapshot(snapshotIdx, snapshot)
}

func _postSnapshotProcess[key Key](processor CommandProcessor[key]) {
	processor.PostSnapshotProcess()
}

func (rsm *ReplicatedStateMachine[Key, Value]) processSnapshot(msg raft.ApplyMsg) {
	snapshotBytes := msg.Snapshot
	startOffset := 0

	// unmarshall snapshot idx
	snapshotIdx := utils.GetIntFromBytes(snapshotBytes, &startOffset)
	lastAppliedIdx := rsm.getLastAppliedIdx()
	if snapshotIdx < lastAppliedIdx {
		rsm.LogError("Trying to install state snapshot till index:", snapshotIdx, "whereas RSM has applied indexes upto:", lastAppliedIdx)
		return
	}

	// unmarshall ack store
	ackStoreSize := utils.GetIntFromBytes(snapshotBytes, &startOffset)
	ackStoreBytes := utils.GetChunkFromBytes(snapshotBytes, &startOffset, ackStoreSize)
	ackStore := haxmap.New[string, OpState]()
	if err := ackStore.UnmarshalJSON(ackStoreBytes); err != nil {
		rsm.LogPanic("Failed to deserialize ackStore", err)
	}

	// unmarshall shardmetadata
	shardMetaDataSize := utils.GetIntFromBytes(snapshotBytes, &startOffset)
	shardmetadata := rsm.GetStore().unmarshallShardMetadata(utils.GetChunkFromBytes(snapshotBytes, &startOffset, shardMetaDataSize))

	// unmarshall kv store
	kvStore, err := rsm.GetStore().unmarshallKvStore(utils.GetChunkFromBytes(snapshotBytes, &startOffset, -1))
	if err != nil {
		rsm.LogPanic("Failed to deserialize kvStore", err)
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

	rsm.LogInfo("Applying snapshot till index:", snapshotIdx)
	rsm.MakeStore(kvStore, ackStore, waitCh, shardmetadata)
	_postSnapshotProcess(rsm.commandProcessor)
}

func _processCommand[key Key](processor CommandProcessor[key], command RaftCommand[key]) {
	processor.ProcessCommandInternal(command)
}

func (rsm *ReplicatedStateMachine[Key, Value]) processCommand(msg raft.ApplyMsg) {
	command := msg.Command.(RaftCommand[Key])
	shardNum := int64(utils.Key2shard(string(command.Key)))
	ackKey := getAckKey(command.ClientId, command.OpId, shardNum)

	if stage, exists := rsm.GetStore().getAckStage(ackKey); exists && stage == COMPLETED {
		rsm.LogWarn("Skipping completed key", ackKey, "command :", command)
	} else {
		rsm.LogInfo("Applying command with key:", ackKey, "to state Machine. Msg :", command)
		_processCommand[Key](rsm.commandProcessor, command)
	}

	if !rsm.GetStore().completeRequest(ackKey) { // complete current request
		rsm.LogDebug("Failed to complete key:", ackKey, "as previous ack event is not consumed yet")
	}
}

func (rsm *ReplicatedStateMachine[Key, Value]) processCommittedMsg() {

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

func (rsm *ReplicatedStateMachine[Key, Value]) listenRaftState() {
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

// Kill the tester calls Kill() when a ReplicatedStateMachine[Key, Value] instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (rsm *ReplicatedStateMachine[Key, Value]) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)
	rsm.rf.Kill()
	// Your code here, if desired.
}

func (rsm *ReplicatedStateMachine[Key, Value]) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

// StartReplicatedStateMachine servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// 'me' is the index of the current server in servers[].
// the k/v server should kvStore snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartReplicatedStateMachine[Key, Value]() must return quickly, so it should start goroutines
// for any long-running work.
func StartReplicatedStateMachine[key Key, value any](serverName string, me int, gid int, maxRaftState int, rf *raft.Raft, cmdProcessor CommandProcessor[key]) *ReplicatedStateMachine[key, value] {
	labgob.Register(RaftCommand[key]{})
	shardStore := make([]*haxmap.Map[key, value], utils.NShards)
	shardMetadata := make([]ShardData, utils.NShards)
	defaultShardState := NOT_SERVING
	for shardNum := 0; shardNum < utils.NShards; shardNum++ {
		shardStore[shardNum] = haxmap.New[key, value]()
		shardMetadata[shardNum].OngoingOps.Store(0)
		shardMetadata[shardNum].State.Store(&defaultShardState)
	}

	rsm := new(ReplicatedStateMachine[key, value])
	rsm.maxRaftState = maxRaftState
	rsm.commandProcessor = cmdProcessor
	rsm.MakeStore(shardStore, haxmap.New[string, OpState](), haxmap.New[string, *chan OpState](), shardMetadata)
	rsm.Logger = utils.GetLogger(serverName+"_rsm", func() string {
		return "[" + strings.ToUpper(serverName) + "] [RSM] [Gid : " + strconv.Itoa(gid) + "] [Peer : " + strconv.Itoa(me) + "] "
	})

	rsm.rf = rf

	go rsm.processCommittedMsg()
	go rsm.listenRaftState()

	return rsm
}
