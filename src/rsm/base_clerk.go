package rsm

import (
	"6.5840/labrpc"
	"6.5840/utils"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type BaseClerk[key Key] struct {
	ServerName        string
	shardVsLeaderId   map[int]int
	ClientId          int64
	OpsExecuted       int64
	serverConnFetcher func(args ServerArgs[key]) []*labrpc.ClientEnd // not including this into args as this is not a fixed parameter
	utils.Logger
}

func MakeBaseClerk[key Key](serverName string, serverMapper func(args ServerArgs[key]) []*labrpc.ClientEnd) BaseClerk[key] {
	var clerk BaseClerk[key]
	clerk.ServerName = serverName
	clerk.ClientId = utils.Nrand()
	clerk.serverConnFetcher = serverMapper
	clerk.shardVsLeaderId = make(map[int]int)
	clerk.Logger = utils.GetLogger(serverName+"_client", func() string {
		return "[" + strings.ToUpper(serverName) + "] [CLIENT] [Client Id: " + strconv.Itoa(int(clerk.ClientId)) + "] "
	})

	return clerk
}

func (ck *BaseClerk[Key]) GetBaseArgs(opType OpType) BaseArgs {
	return ck.GetBaseArgsWithGid(opType, 0)
}

func (ck *BaseClerk[Key]) GetBaseArgsWithGid(opType OpType, gid int) BaseArgs {
	return BaseArgs{
		Op:       opType,
		ClientId: ck.ClientId,
		OpId:     ck.GetNextOperationId(),
		Shard:    gid,
	}
}

func (ck *BaseClerk[Key]) SendRequest(args ServerArgs[Key], reply ServerReply, requestType string) {
	numRetries := 0
	backoff := utils.BASE_CLIENT_RETRY_WAIT_MS

	servers := ck.serverConnFetcher(args)
	for {
		for i := 0; i < len(servers); i++ {
			serverId := (ck.shardVsLeaderId[args.GetShardNum()] + i) % len(servers)
			ok := ck.sendRequestToServer(0, servers[serverId], serverId, args, reply, requestType)

			// if wrong group, reset request and let the callee refresh config
			if reply.GetErr() == WrongGroup {
				return
			}

			if ok {
				ck.shardVsLeaderId[args.GetShardNum()] = serverId
				return
			}
		}

		numRetries++
		// sleep before retrying again
		ck.LogInfo("Failed to find leader. Sleeping for", backoff, "ms before retry")
		time.Sleep(time.Duration(backoff) * time.Millisecond)
		backoff *= utils.BACKOFF_EXPONENT

		if numRetries > utils.MAX_RPC_RETRIES {
			backoff = utils.BASE_CLIENT_RETRY_WAIT_MS
			numRetries = 0
		}
	}
}

// SendRequest sends a request to the server and handles retries.
func (ck *BaseClerk[Key]) sendRequestToServer(numRetries int, serverEnd *labrpc.ClientEnd, serverId int, args ServerArgs[Key], reply ServerReply, requestType string) bool {
	rpcName := ck.ServerName + ".Handle" + requestType
	ck.LogInfo("Sending", requestType, "request to server:", serverId, "with args", args.ToString())
	ok := serverEnd.Call(rpcName, args, reply)
	ck.LogDebug("Server Args:", args.ToString(), "Reply:", reply.ToString())

	if !ok {
		ck.LogDebug("Failed to execute request to server", serverId, "with args", args.ToString())
		if numRetries < utils.MAX_RPC_RETRIES {
			ck.LogDebug("Retrying request again to server", serverId, "with args", args.ToString())
			ok = ck.sendRequestToServer(numRetries+1, serverEnd, serverId, args, reply, requestType)
		}
	} else if reply.GetErr() == WrongGroup {
		ck.LogInfo("Wrong Group :", args.GetShardNum())
	} else if reply.GetErr() == WrongLeader {
		ck.LogInfo("Wrong Leader :", serverId)
	} else {
		ck.LogInfo(rpcName+" Successful for OpId:", args.GetOpId())
	}

	return ok && reply.GetErr() == Ok
}

func (ck *BaseClerk[Key]) GetNextOperationId() int64 {
	return atomic.AddInt64(&ck.OpsExecuted, 1)
}
