package shardkv

import (
	"6.5840/rsm"
	"6.5840/shardctrler"
	"fmt"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	rsm.BaseArgs
	Key   string
	Value string
}

func (args PutAppendArgs) ConvertToRaftCommand() rsm.RaftCommand[string, string] {
	return rsm.RaftCommand[string, string]{
		OpType:   args.Op,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Key:      args.Key,
		Value:    args.Value,
	}
}

func (args PutAppendArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

type PutAppendReply struct {
	rsm.BaseReply
}

func (reply PutAppendReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}

type GetArgs struct {
	rsm.BaseArgs
	Key string
}

func (args GetArgs) ConvertToRaftCommand() rsm.RaftCommand[string, string] {
	return rsm.RaftCommand[string, string]{
		OpType:   args.Op,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Key:      args.Key,
	}
}

func (args GetArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

type GetReply struct {
	rsm.BaseReply
	Value string
}

func (reply GetReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}
