package rsm

import (
	"6.5840/labrpc"
	"6.5840/utils"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type BaseClerk[key Key, Value any] struct {
	ServerName  string
	LeaderId    int
	ClientId    int64
	OpsExecuted int64
	Servers     []*labrpc.ClientEnd
	utils.Logger
}

func MakeBaseClerk[key Key, value any](serverName string, servers []*labrpc.ClientEnd) BaseClerk[key, value] {
	var clerk BaseClerk[key, value]
	clerk.ServerName = serverName
	clerk.LeaderId = 0 // random for start, it will eventually get corrected after retries
	clerk.ClientId = utils.Nrand()
	clerk.Servers = servers
	clerk.Logger = utils.GetLogger("client_logLevel", func() string {
		return "[" + strings.ToUpper(serverName) + "] [CLIENT] [Client Id: " + strconv.Itoa(int(clerk.ClientId)) + "] "
	})

	return clerk
}

func (ck *BaseClerk[Key, Value]) GetArgBase(opType OpType) BaseArgs {
	return BaseArgs{
		Op:       opType,
		ClientId: ck.ClientId,
		OpId:     ck.GetNextOperationId(),
	}
}

func (ck *BaseClerk[Key, Value]) SendRequest(args ServerArgs[Key, Value], reply ServerReply, requestType string) {
	rpcName := ck.ServerName + ".Handle" + requestType
	numRetries := 0
	backoff := utils.BASE_CLIENT_RETRY_WAIT_MS

	for {
		for i := 0; i < len(ck.Servers); i++ {
			serverId := ck.LeaderId + i%len(ck.Servers)
			ok := ck.sendRequestToServer(0, serverId, args, reply, rpcName)
			if ok {
				ck.LeaderId = serverId
				return
			}
		}

		numRetries++
		// sleep before retrying again
		ck.LogInfo("Failed to find leader server. Sleeping for", backoff, "ms before retry")
		time.Sleep(time.Duration(backoff) * time.Millisecond)
		backoff *= utils.BACKOFF_EXPONENT

		if numRetries > utils.MAX_RPC_RETRIES {
			backoff = utils.BASE_CLIENT_RETRY_WAIT_MS
			numRetries = 0
		}
	}
}

// SendRequest sends a request to the server and handles retries.
func (ck *BaseClerk[Key, Value]) sendRequestToServer(numRetries int, serverId int, args ServerArgs[Key, Value], reply ServerReply, rpcName string) bool {

	ck.LogInfo("Sending", rpcName, "request to server:", serverId, "with args", args.ToString())
	ok := ck.Servers[serverId].Call(rpcName, args, reply)

	ck.LogDebug("Server Args:", args.ToString(), "Reply:", reply.ToString())

	if !ok {
		ck.LogError("Failed to execute request with args", args.ToString())
		if numRetries < utils.MAX_RPC_RETRIES {
			ck.LogError("Retrying request again with args", args.ToString())
			ok = ck.sendRequestToServer(numRetries+1, serverId, args, reply, rpcName)
		}
	} else if reply.GetErr() == WrongLeader {
		ck.LogInfo("Wrong Leader :", serverId)
	} else {
		ck.LogInfo(rpcName+" Successful for OpId:", args.GetOpId())
	}

	return ok && reply.GetErr() == Ok
}

func (ck *BaseClerk[Key, Value]) GetNextOperationId() int64 {
	return atomic.AddInt64(&ck.OpsExecuted, 1)
}
