package kvraft

import (
	"6.5840/rsm"
	"fmt"
)

// Put or Append
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
