package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
	"6.5840/rsm"
	"strconv"
	"time"
)
import "6.5840/shardctrler"

// ShardAwareClerk Group Aware ShardAwareClerk
type ShardAwareClerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	rsm.BaseClerk[string, string]
}

// MakeClerk the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardAwareClerk {
	ck := new(ShardAwareClerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.BaseClerk = rsm.MakeBaseClerk[string, string]("ShardKV", ck.getServerMapping)
	return ck
}

func (ck *ShardAwareClerk) getServerMapping(args rsm.ServerArgs[string, string]) []*labrpc.ClientEnd {
	gid := ck.config.Shards[args.GetShardNum()]
	servers, ok := ck.config.Groups[gid]
	if !ok {
		panic("The config is in invalid state. Failed to find servers for gid : " + strconv.Itoa(args.GetShardNum()))
	}

	serverEnds := make([]*labrpc.ClientEnd, 0)
	for _, srvStr := range servers {
		serverEnds = append(serverEnds, ck.make_end(srvStr))
	}

	return serverEnds
}

func (ck *ShardAwareClerk) sendShardAwareRequest(args rsm.ServerArgs[string, string], reply rsm.ServerReply, requestType string) {
	for {
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		ck.LogDebug("Current Shard Config :", ck.config)

		// send request
		ck.SendRequest(args, reply, requestType)
		if reply.GetErr() == rsm.Ok {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *ShardAwareClerk) Get(key string) string {
	args := ck.getGetArgs(key)
	reply := &GetReply{}
	ck.sendShardAwareRequest(args, reply, "Get")
	return reply.Value
}

func (ck *ShardAwareClerk) PutAppend(key string, value string, op rsm.OpType) {
	args := ck.getPutAppendArgs(key, value, op)
	reply := &PutAppendReply{}
	ck.sendShardAwareRequest(args, reply, "PutAppend")
}

func (ck *ShardAwareClerk) Put(key string, value string) {
	ck.PutAppend(key, value, rsm.PUT)
}
func (ck *ShardAwareClerk) Append(key string, value string) {
	ck.PutAppend(key, value, rsm.APPEND)
}

func (ck *ShardAwareClerk) getGetArgs(key string) *GetArgs {
	return &GetArgs{
		BaseArgs: ck.GetBaseArgsWithGid(rsm.GET, key2shard(key)),
		Key:      key,
	}
}

func (ck *ShardAwareClerk) getPutAppendArgs(key, value string, op rsm.OpType) *PutAppendArgs {
	return &PutAppendArgs{
		BaseArgs: ck.GetBaseArgsWithGid(op, key2shard(key)),
		Key:      key,
		Value:    value,
	}
}
