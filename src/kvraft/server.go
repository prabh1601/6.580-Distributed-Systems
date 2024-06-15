package kvraft

import (
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/rsm"
	"6.5840/utils"
	"strconv"
	"sync/atomic"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rf   *raft.Raft
	utils.Logger
	*rsm.ReplicatedStateMachine[string, string, string]
}

func (kv *KVServer) HandleGet(args *GetArgs, reply *GetReply) {
	kv.LogDebug("Received Get for args", *args)

	_, err := kv.StartQuorum(args.ConvertToRaftCommand())
	reply.Err = err
	if err == rsm.Ok {
		reply.Value = kv.GetStore().GetValue(args.Key)
	}
}

func (kv *KVServer) HandlePutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.LogDebug("Received PutAppend for args", *args)

	_, err := kv.StartQuorum(args.ConvertToRaftCommand())
	reply.Err = err
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.ReplicatedStateMachine.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) PostSnapshotProcess() {
	// no-op
}

func (kv *KVServer) ProcessCommandInternal(command rsm.RaftCommand[string, string]) {
	switch command.OpType {
	case rsm.PUT:
		kv.GetStore().SetValue(command.Key, command.Value)
	case rsm.APPEND:
		value := kv.GetStore().GetValue(command.Key)
		value += command.Value
		kv.GetStore().SetValue(command.Key, value)
	case rsm.GET:
		// do nothing
	}
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// 'me' is the index of the current server in servers[].
// the k/v server should kvStore snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	kv := new(KVServer)
	kv.rf = raft.Make(servers, me, persister, make(chan raft.ApplyMsg))
	kv.me = me
	kv.Logger = utils.GetLogger("kv_logLevel", func() string {
		return "[KV] [Peer : " + strconv.Itoa(me) + "] "
	})

	kv.ReplicatedStateMachine = rsm.StartReplicatedStateMachine[string, string, string]("KVServer", me, maxRaftState, kv.rf, kv)
	return kv
}
