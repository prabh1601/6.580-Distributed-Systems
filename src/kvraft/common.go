package kvraft

import (
	"6.5840/rsm"
	"fmt"
)

// Put or Append
type PutAppendArgs struct {
	rsm.ArgBase
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

func (args PutAppendArgs) GetOpId() int64 {
	return args.OpId
}

func (args PutAppendArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

type PutAppendReply struct {
	rsm.ReplyBase
}

func (reply PutAppendReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}

func (reply PutAppendReply) GetLeaderId() int {
	return reply.LeaderId
}

func (reply PutAppendReply) GetErr() rsm.Err {
	return reply.Err
}

type GetArgs struct {
	rsm.ArgBase
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

func (args GetArgs) GetOpId() int64 {
	return args.OpId
}

func (args GetArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

type GetReply struct {
	rsm.ReplyBase
	Value string
}

func (reply GetReply) GetLeaderId() int {
	return reply.LeaderId
}

func (reply GetReply) GetErr() rsm.Err {
	return reply.Err
}

func (reply GetReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}
