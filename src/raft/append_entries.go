package raft

import (
	"6.5840/utils"
)

type Result int32

const (
	SUCCESS Result = iota
	STALE_STATE
	LOG_INCONSISTENCY
)

func (r Result) String() string {
	switch r {
	case SUCCESS:
		return "Success"
	case STALE_STATE:
		return "Stale Leader State"
	case LOG_INCONSISTENCY:
		return "Inconsistent Logs"
	default:
		return "Invalid State"
	}
}

type AppendEntriesArgs struct {
	RpcId        int64
	Term         int32            // leader's Term
	LeaderId     int              // peer index of leader
	PrevLogIndex int32            // index of log entry immediately preceding new ones
	PrevLogTerm  int32            // Term of prevLogIndex entry
	LogEntries   []utils.LogEntry // log entries to store
	LeaderCommit int32            // leader's commit index
}

func (ae AppendEntriesArgs) GetRpcId() int64 {
	return ae.RpcId
}

type AppendEntriesReply struct {
	RpcId  int64
	Term   int32 // Term of the receiver of request, in order to update requester if he is behind
	XTerm  int32
	XIdx   int32
	Status Result // shows
}

func (ae AppendEntriesReply) GetRpcId() int64 {
	return ae.RpcId
}

func (ae AppendEntriesReply) GetReply() any {
	return ae
}

func (rf *Raft) applyIfValidEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) Result {
	for i := 0; i < 100; i++ {
		if i == 99 {
			rf.LogInfo("Log :", rf.stable.LogArray)
			rf.LogInfo("Args :", *args)
			rf.LogPanic("Stuck in infinite loop while applying entries", rf.stable.GetLastLogEntry())
		}

		if rf.getTerm() > args.Term {
			return STALE_STATE
		}

		lastLogEntry := rf.stable.GetLastLogEntry()
		rf.LogDebug("Checking for valid entry. Self lastLogEntry :", lastLogEntry)
		if lastLogEntry.LogIndex < args.PrevLogIndex || rf.stable.GetLogEntry(args.PrevLogIndex).LogTerm != args.PrevLogTerm {
			reply.XTerm = rf.stable.GetLogEntry(min(rf.stable.GetLogLength()-1, args.PrevLogIndex)).LogTerm
			reply.XIdx = rf.stable.GetFirstLogIdxInTerm(reply.XTerm)
			return LOG_INCONSISTENCY
		}

		if rf.stable.AppendMultipleEntries(rf.getCommitIndex(), args.LogEntries) {
			break
		}
	}

	return SUCCESS
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.LogInfo("Received AppendEntries request from", args.LeaderId)
	reply.Status = rf.applyIfValidEntries(args, reply)
	reply.Term = rf.getTerm()
	if reply.Status != SUCCESS {
		rf.LogWarn("Denied append entries to", args.LeaderId, ".Reason:", reply.Status)
		return
	}

	rf.LogDebug("Received Valid append entries from", args.LeaderId, "Args:", *args)
	rf.transitToNewRaftStateWithTerm(FOLLOWER, args.Term)
	rf.setLeaderPeerIndex(args.LeaderId)
	rf.setCommitIndexIfValid(min(args.LeaderCommit, rf.stable.GetLastLogIndex()))
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, leaderCommit int32, entries []utils.LogEntry, prevLogEntry utils.LogEntry) (bool, AppendEntriesReply) {
	rpcId := utils.Nrand()
	ok, reply := utils.ExecuteRPC[AppendEntriesReply](func() (bool, AppendEntriesReply) {
		args := rf.getAppendEntriesArgs(rpcId, entries, leaderCommit, prevLogEntry)
		reply := &AppendEntriesReply{RpcId: args.GetRpcId()}
		rf.LogDebug("Append Entries - server:", server, "args:", *args)
		ok := false
		if rf.is(LEADER) {
			ok = rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
		}
		return ok, *reply
	})

	if !ok {
		rf.LogError("AppendEntries Rpc to", server, "failed")
	}
	return ok, reply
}

func (rf *Raft) getAppendEntriesArgs(rpcId int64, logEntries []utils.LogEntry, leaderCommit int32, prevLogEntry utils.LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		RpcId:        rpcId,
		Term:         rf.getTerm(),
		LeaderId:     rf.getSelfPeerIndex(),
		PrevLogIndex: prevLogEntry.LogIndex,
		PrevLogTerm:  prevLogEntry.LogTerm,
		LogEntries:   logEntries,
		LeaderCommit: leaderCommit,
	}
}
