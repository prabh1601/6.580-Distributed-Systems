package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/utils"
	"github.com/alphadose/haxmap"
	"strconv"
	"sync"
	"sync/atomic"
)

type KVServer struct {
	utils.Logger
	kvStore        *haxmap.Map[string, string] // key value pair store
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	dead           int32 // set by Kill()
	maxraftstate   int   // snapshot if log grows this big
	commandWaitGrp *haxmap.Map[int, *sync.WaitGroup]
	ackStore       *haxmap.Map[string, OpState]      // stores status of an ongoing/completed operation
	mostRecentReq  *haxmap.Map[int64, ClientRequest] // stores reference of most request request by a client
}

// return waitGroup, existing
func (kv KVServer) getOrCreateWaitObject(index int) (*sync.WaitGroup, bool) {
	waitGrp := &sync.WaitGroup{}
	waitGrp.Add(1)
	return kv.commandWaitGrp.GetOrSet(index, waitGrp)
}

func getAckKey(clientId, opId int64) string {
	return strconv.Itoa(int(clientId)) + ":" + strconv.Itoa(int(opId))
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
	kv.LogDebug("Recieved Get for args", *args)

	_, err := kv.startQuorum(kv.convertToRaftCommandForGet(args))
	reply.LeaderId = kv.rf.GetLeaderPeerIndex()
	reply.Err = err
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
	// Your code here.
	kv.LogDebug("Recieved PutAppend for args", *args)

	_, err := kv.startQuorum(kv.convertToRaftCommandForPut(args))
	reply.Err = err
	reply.LeaderId = kv.rf.GetLeaderPeerIndex()
}

func (kv *KVServer) createRequestMetadata(request ClientRequest) {
	// cleanup previous request
	previousRequest, existing := kv.mostRecentReq.Get(request.clientId)
	if existing {
		kv.LogInfo("Deleting metadata for index:", previousRequest.index, "key:", getAckKey(previousRequest.clientId, previousRequest.opId))
		kv.ackStore.Del(getAckKey(previousRequest.clientId, previousRequest.opId))
		kv.commandWaitGrp.Del(previousRequest.index)
	}

	kv.mostRecentReq.Set(request.clientId, request)
}

func (kv *KVServer) startQuorum(command RaftCommand) (bool, Err) {
	kv.LogInfo("Starting Quorom for", *(&command))
	ackKey := getAckKey(command.ClientId, command.OpId)

	// check if this command is already started
	if request, reqExists := kv.mostRecentReq.Get(command.ClientId); reqExists && request.opId == command.OpId {
		stage, ackExits := kv.ackStore.Get(ackKey)
		if !ackExits {
			kv.LogError("Cannot find ack for client request :", request)
			panic("Failed to find ack for request")
		}

		switch stage {
		case COMPLETED:
			return true, OK
		case STARTED:
			return kv.finishQuorum(request.index, ackKey)
		}
	}

	// submit to raft for reaching quorum
	kv.ackStore.Set(ackKey, STARTED)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return false, WRONG_LEADER
	}

	kv.createRequestMetadata(ClientRequest{index: index, clientId: command.ClientId, opId: command.OpId})
	return kv.finishQuorum(index, ackKey)
}

func (kv *KVServer) finishQuorum(index int, ackKey string) (bool, Err) {
	kv.waitForCompletion(index)
	operationStage, _ := kv.ackStore.Get(ackKey)
	if operationStage != COMPLETED {
		return false, WRONG_LEADER
	}

	return true, OK
}

func (kv *KVServer) waitForCompletion(index int) {
	waitGrp, _ := kv.getOrCreateWaitObject(index)
	kv.LogInfo("Started wait for quorum on index:", index)
	waitGrp.Wait()
	kv.LogInfo("Quorum reached for index:", index)
}

func (kv *KVServer) processCommittedMsg() {

	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		kv.LogInfo("Applying Command to state Machine. Msg :", applyMsg)

		if applyMsg.SnapshotValid {
			kv.processSnapshot(applyMsg)
		}

		if applyMsg.CommandValid {
			kv.processCommand(applyMsg.CommandIndex, applyMsg)
		}
	}
}

func (kv *KVServer) processSnapshot(msg raft.ApplyMsg) {

}

func (kv *KVServer) processCommand(raftLogIdx int, msg raft.ApplyMsg) {
	command := msg.Command.(RaftCommand)
	switch command.OpType {
	case PUT:
		kv.kvStore.Set(command.Key, command.Value)
	case APPEND:
		value, ok := kv.kvStore.Get(command.Key)
		if !ok {
			value = command.Value
		} else {
			value += command.Value
		}
		kv.kvStore.Set(command.Key, value)
	case GET:
		// do nothing
	}

	// todo : check for case when leader losses connect
	_, leader := kv.rf.GetStatus()
	if leader {
		kv.ackStore.Set(getAckKey(command.ClientId, command.OpId), COMPLETED)
		waitGrp, _ := kv.getOrCreateWaitObject(raftLogIdx)
		kv.LogInfo("Finishing wait for index:", raftLogIdx, "key:", getAckKey(command.ClientId, command.OpId))
		waitGrp.Done()
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
	kv.mostRecentReq = haxmap.New[int64, ClientRequest]()
	kv.commandWaitGrp = haxmap.New[int, *sync.WaitGroup]()
	kv.Logger = utils.GetLogger("server_logLevel", func() string {
		return "[SERVER] [Peer : " + strconv.Itoa(me) + "] "
	})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.processCommittedMsg()

	return kv
}
