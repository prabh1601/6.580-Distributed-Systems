package raft

import (
	"6.5840/utils"
)

type AppendEntriesArgs struct {
	Term         int32            // leader's term
	LeaderId     int              // peer index of leader
	PrevLogIndex int32            // index of log entry immediately preceding new ones
	PrevLogTerm  int32            // term of prevLogIndex entry
	LogEntries   []utils.LogEntry // log entries to store
	LeaderCommit int32            // leader's commit index
}

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

type AppendEntriesReply struct {
	Term   int32  // term of the receiver of request, in order to update requester if he is behind
	Status Result // shows
}

func (rf *Raft) areValidAppendEntries(args *AppendEntriesArgs) Result {
	lastLogEntry := rf.GetLastLogEntry()
	if rf.GetTermManager().GetTerm() > args.Term {
		return STALE_STATE
	}

	if lastLogEntry.LogIndex < args.PrevLogIndex || rf.GetLogEntry(args.PrevLogIndex).LogTerm != args.PrevLogTerm {
		return LOG_INCONSISTENCY
	}

	return SUCCESS
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.LogInfo("Received AppendEntries request from", args.LeaderId)
	reply.Term = rf.GetTermManager().GetTerm()
	reply.Status = rf.areValidAppendEntries(args)
	if reply.Status != SUCCESS {
		rf.LogWarn("Denied append entries to", args.LeaderId, ".Reason:", reply.Status)
		return
	}

	currentCommitIndex := rf.GetCommitIndex()
	rf.updateHeartBeat()
	rf.transitToNewRaftStateWithTerm(FOLLOWER, args.Term)
	rf.AppendMultipleEntries(currentCommitIndex, args.LogEntries)
	rf.SetCommitIndexIfValid(min(args.LeaderCommit, rf.GetLastLogIndex()))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	executionResult := utils.DoExecutionWithTimeout(func() bool {
		ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
		if !ok {
			rf.LogError("AppendEntries Rpc to", server, "failed")
		}
		return ok
	}, func() { rf.LogError("RequestVote Rpc to", server, "timed out") })

	return executionResult == utils.SUCCESS
}

func (rf *Raft) sendEntry(server int, entries []utils.LogEntry, reply *AppendEntriesReply) bool {
	prevLogEntry := rf.GetLogEntry(rf.nextIndex[server].Load() - 1)
	args := rf.getAppendEntriesArgs(entries, prevLogEntry)
	ok := rf.sendAppendEntries(server, args, reply)
	return ok
}

func (rf *Raft) getAppendEntriesArgs(logEntries []utils.LogEntry, prevLogEntry utils.LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         rf.GetTermManager().GetTerm(),
		LeaderId:     rf.GetSelfPeerIndex(),
		PrevLogIndex: prevLogEntry.LogIndex,
		PrevLogTerm:  prevLogEntry.LogTerm,
		LogEntries:   logEntries,
		LeaderCommit: rf.GetCommitIndex(),
	}
}
