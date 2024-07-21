package shardkv

import (
	"6.5840/labrpc"
	"6.5840/rsm"
	"6.5840/shardctrler"
	"6.5840/utils"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)
import "6.5840/raft"

type ShardKV struct {
	shardCtrl   *shardctrler.Clerk
	shardConfig atomic.Pointer[shardctrler.Config]
	me          int
	rf          *raft.Raft
	make_end    func(string) *labrpc.ClientEnd
	gid         int
	utils.Logger
	*rsm.ReplicatedStateMachine[string, string, string]
}

func (kv *ShardKV) ProcessCommandInternal(command rsm.RaftCommand[string, string]) {
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

func (kv *ShardKV) processShardConfigChanges() {
	for {
		newConfig := kv.shardCtrl.Query(-1)
		curConfig := kv.shardConfig.Load()

		if (curConfig == nil || newConfig.Num != curConfig.Num) && kv.shardConfig.CompareAndSwap(curConfig, &newConfig) {
			kv.LogWarn("Found new shard configuration :", newConfig)

			// process config changes
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) PostSnapshotProcess() {
	// no-op
}

func (kv *ShardKV) isShardPresent(cmd rsm.RaftCommand[string, string]) bool {
	curConfig := kv.shardConfig.Load()
	if curConfig == nil {
		return false
	}

	numShard := key2shard(cmd.Key)
	shardGid := curConfig.Shards[numShard]
	return shardGid == kv.gid
}

func (kv *ShardKV) HandleGet(args *GetArgs, reply *GetReply) {
	kv.LogDebug("Received Get for args", *args)
	command := args.ConvertToRaftCommand()
	if !kv.isShardPresent(command) {
		reply.Err = rsm.WrongGroup
	}

	_, err := kv.StartQuorum(command)
	reply.Err = err
	if err == rsm.Ok {
		reply.Value = kv.GetStore().GetValue(args.Key)
	}
}

func (kv *ShardKV) HandlePutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.LogDebug("Received PutAppend for args", *args)
	command := args.ConvertToRaftCommand()
	if !kv.isShardPresent(command) {
		reply.Err = rsm.WrongGroup
	}

	_, err := kv.StartQuorum(args.ConvertToRaftCommand())
	reply.Err = err
}

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// StartServer servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	serverName := "shardkv"

	kv := new(ShardKV)
	kv.me = me
	kv.make_end = make_end
	kv.gid = gid
	kv.shardCtrl = shardctrler.MakeClerk(ctrlers)
	kv.rf = raft.Make(serverName, servers, me, persister, make(chan raft.ApplyMsg))
	kv.Logger = utils.GetLogger(serverName, func() string {
		return "[" + strings.ToUpper(serverName) + "] [Peer : " + strconv.Itoa(me) + "] "
	})

	kv.ReplicatedStateMachine = rsm.StartReplicatedStateMachine[string, string, string]("KVServer", me, maxRaftState, kv.rf, kv)

	// start go-routine to check for configuration changes
	go kv.processShardConfigChanges()

	return kv
}
