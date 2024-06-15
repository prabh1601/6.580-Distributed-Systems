package kvraft

import (
	"6.5840/labrpc"
	"6.5840/rsm"
)

type Clerk struct {
	rsm.BaseClerk[string, string]
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.BaseClerk = rsm.MakeBaseClerk[string, string]("KVServer", servers)
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := ck.getGetArgs(key)
	reply := &GetReply{}
	ck.SendRequest(args, reply, "Get")
	return reply.Value
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.HandlePutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op rsm.OpType) {
	args := ck.getPutAppendArgs(key, value, op)
	reply := &PutAppendReply{}
	ck.SendRequest(args, reply, "PutAppend")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, rsm.PUT)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, rsm.APPEND)
}

func (ck *Clerk) getGetArgs(key string) *GetArgs {
	return &GetArgs{
		BaseArgs: ck.GetArgBase(rsm.GET),
		Key:      key,
	}
}

func (ck *Clerk) getPutAppendArgs(key, value string, op rsm.OpType) *PutAppendArgs {
	return &PutAppendArgs{
		BaseArgs: ck.GetArgBase(op),
		Key:      key,
		Value:    value,
	}
}
