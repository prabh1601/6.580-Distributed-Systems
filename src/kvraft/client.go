package kvraft

import (
	"6.5840/labrpc"
	"6.5840/utils"
	"strconv"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	leaderId    int
	clientId    int64
	opsExecuted int64
	utils.Logger
	// You will have to modify this struct.
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = utils.Nrand()
	ck.leaderId = 0 // randomly assigning as it will be fixed eventually
	ck.Logger = utils.GetLogger("client_logLevel", func() string {
		return "[CLIENT] [" + strconv.Itoa(int(ck.clientId)) + "] "
	})

	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
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
	for {
		knownLeader := ck.leaderId
		ck.LogInfo("Sending Get Request to server:", knownLeader, "with args", *args)
		ok := ck.servers[knownLeader].Call("KVServer.HandleGet", args, reply)
		ck.leaderId = reply.LeaderId
		if !ok {
			ck.LogError("Failed to execute Get with args", *args)
		} else if reply.Err == WRONG_LEADER {
			ck.LogInfo("Wrong Leader, retrying request to server", reply.LeaderId)
		} else {
			ck.LogInfo("Get Successful")
			break
		}
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.HandlePutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {

	args := ck.getPutAppendArgs(key, value, op)
	reply := &PutAppendReply{}
	numFailures := 0
	waitTime := utils.BASE_CLIENT_RETRY_WAIT_MS * time.Millisecond
	for {
		knownLeader := ck.leaderId
		ck.LogInfo("Sending PutAppend Request to server:", knownLeader, "with args", *args)
		ok := ck.servers[knownLeader].Call("KVServer.HandlePutAppend", args, reply)
		ck.LogDebug("Server Args:", *args, "Reply:", *reply)
		ck.leaderId = reply.LeaderId
		if !ok {
			ck.LogError("Failed to execute PutAppend with args", *args)
		} else if reply.Err == WRONG_LEADER {
			ck.LogInfo("Wrong Leader, retrying request to server", reply.LeaderId)
		} else {
			ck.LogInfo("PutAppend Successful")
			break
		}

		// sleep before retrying
		time.Sleep(waitTime)
		// exponential backoff
		waitTime *= utils.BACKOFF_EXPONENT
		numFailures++
		if numFailures >= utils.RANDOMIZE_AFTER_RETRY_COUNT {
			ck.leaderId = int(utils.Nrand() % int64(len(ck.servers)))
			numFailures = 0
			waitTime = utils.BASE_CLIENT_RETRY_WAIT_MS * time.Millisecond
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) getNextOperationId() int64 {
	return atomic.AddInt64(&ck.opsExecuted, 1)
}

func (ck *Clerk) getGetArgs(key string) *GetArgs {
	return &GetArgs{
		OpId:     ck.getNextOperationId(),
		ClientId: ck.clientId,
		Key:      key,
	}
}

func (ck *Clerk) getPutAppendArgs(key, value string, op OpType) *PutAppendArgs {
	return &PutAppendArgs{
		OpId:     ck.getNextOperationId(),
		ClientId: ck.clientId,
		Key:      key,
		Value:    value,
		Op:       op,
	}
}
