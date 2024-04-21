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

type AckManager struct {
	waitCh   *haxmap.Map[string, *chan OpState]
	ackStore *haxmap.Map[string, OpState] // stores status of an ongoing/completed operation
}

type KVServer struct {
	utils.Logger
	AckManager
	kvStore      *haxmap.Map[string, string] // key value pair store
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
}

// return waitCh, existing
func (am *AckManager) getOrCreateWaitObject(ackKey string) (*chan OpState, bool) {
	waitCh := make(chan OpState, 1)
	return am.waitCh.GetOrSet(ackKey, &waitCh)
}

func (am *AckManager) cleanRequestMetadata(ackKey string) {
	am.ackStore.Del(ackKey)
	am.waitCh.Del(ackKey)
}

func (am *AckManager) createRequestMetadata(ackKey string) {
	am.getOrCreateWaitObject(ackKey) // create wait object for this key
	am.ackStore.Set(ackKey, STARTED) // mark started
}

func (am *AckManager) completeRequestMetadata(ackKey string) bool {
	am.ackStore.Set(ackKey, COMPLETED) // mark completed
	waitCh, _ := am.getOrCreateWaitObject(ackKey)
	// this will not block if previous ack is not already consumed
	select {
	case *waitCh <- COMPLETED:
		return true
	default:
		return false
	}
}

func (am *AckManager) abortRequestMetadata(ackKey string) bool {
	aborted := am.ackStore.CompareAndSwap(ackKey, STARTED, ABORTED) // mark aborted
	if aborted {
		waitCh, _ := am.getOrCreateWaitObject(ackKey)
		*waitCh <- ABORTED
	}

	return aborted
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
		OpId:     args.OpId,
		ClientId: args.ClientId,
		OpType:   args.Op,
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
		value, exists := kv.kvStore.Get(args.Key)
		if !exists {
			reply.Err = NO_KEY
		} else {
			reply.Value = value
		}
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
	if stage, ackExists := kv.ackStore.Get(ackKey); ackExists && stage == COMPLETED {
		return true, OK
	}

	// submit to raft for reaching quorum
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		return false, WRONG_LEADER
	}

	kv.createRequestMetadata(ackKey)
	kv.LogInfo("Initiated quorum for", *(&command), "submitted at index:", index)
	return kv.finishQuorum(ackKey, term)
}

func (kv *KVServer) finishQuorum(ackKey string, quorumTerm int) (bool, Err) {
	kv.LogDebug("Started wait for quorum on key:", ackKey)

	currentTerm, _ := kv.rf.GetStatus()
	if quorumTerm != currentTerm && kv.abortRequestMetadata(ackKey) {
		kv.LogDebug("Aborting quorum on key:", ackKey)
	}

	waitCh, _ := kv.getOrCreateWaitObject(ackKey)
	operationStage := <-*waitCh
	kv.LogDebug("Finished wait for quorum on key:", ackKey)
	if operationStage != COMPLETED {
		return false, WRONG_LEADER
	}

	return true, OK
}

func (kv *KVServer) processCommittedMsg() {

	for kv.killed() == false {
		applyMsg := <-kv.applyCh

		command := applyMsg.Command.(RaftCommand)
		ackKey := kv.getAckKey(command.ClientId, command.OpId)
		if stage, exists := kv.ackStore.Get(ackKey); exists && stage == COMPLETED {
			kv.LogWarn("Skipping completed command :", applyMsg)
		} else {
			kv.LogDebug("Applying Command to state Machine. Msg :", applyMsg)

			if applyMsg.SnapshotValid {
				kv.processSnapshot(applyMsg)
			}

			if applyMsg.CommandValid {
				kv.processCommand(ackKey, command)
			}
		}

		if !kv.completeRequestMetadata(ackKey) { // complete current request
			kv.LogDebug("Failed to complete key:", ackKey, "as previous ack event is not consumed yet")

		}
		kv.cleanRequestMetadata(kv.getPreviousAckKey(ackKey)) // clean up previous request meta data from same client
	}
}

func (kv *KVServer) processSnapshot(msg raft.ApplyMsg) {

}

func (kv *KVServer) processCommand(ackKey string, command RaftCommand) {
	switch command.OpType {
	case PUT:
		kv.kvStore.Set(command.Key, command.Value)
	case APPEND:
		value, exists := kv.kvStore.Get(command.Key)
		if !exists {
			value = command.Value
		} else {
			value += command.Value
		}
		kv.kvStore.Set(command.Key, value)
	case GET:
		// do nothing
	}
}

func (kv *KVServer) listenRaftState() {
	for {
		term := <-kv.rf.ListenTermChanges()
		kv.LogDebug("Raft term changed to:", term, "Aborting all awaited client request")
		// term changed : abort all ongoing client requests
		kv.waitCh.ForEach(func(ackKey string, ch *chan OpState) bool {
			if kv.abortRequestMetadata(ackKey) {
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
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(RaftCommand{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kvStore = haxmap.New[string, string]()
	kv.ackStore = haxmap.New[string, OpState]()
	kv.waitCh = haxmap.New[string, *chan OpState]()
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
