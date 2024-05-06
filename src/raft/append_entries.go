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
	Term         int32            // leader's Term
	LeaderId     int              // peer index of leader
	PrevLogIndex int32            // index of log entry immediately preceding new ones
	PrevLogTerm  int32            // Term of prevLogIndex entry
	LogEntries   []utils.LogEntry // log entries to store
	LeaderCommit int32            // leader's commit index
}

type AppendEntriesReply struct {
	Term   int32 // Term of the receiver of request, in order to update requester if he is behind
	XTerm  int32
	XIdx   int32
	XLen   int32
	Status Result // shows
}

func (rf *Raft) areValidAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) Result {
	lastLogEntry := rf.stable.GetLastLogEntry()
	rf.LogDebug("Checking for valid entry. Self lastLogEntry :", lastLogEntry)
	if rf.stable.GetTermManager().getTerm() > args.Term {
		return STALE_STATE
	}

	if lastLogEntry.LogIndex < args.PrevLogIndex || rf.stable.GetLogEntry(args.PrevLogIndex).LogTerm != args.PrevLogTerm {
		reply.XTerm = lastLogEntry.LogTerm
		reply.XIdx = rf.stable.GetFirstLogIdxInTerm(reply.XTerm)
		reply.XLen = rf.stable.GetLogLength()
		return LOG_INCONSISTENCY
	}

	return SUCCESS
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.LogInfo("Received AppendEntries request from", args.LeaderId)
	reply.Term = rf.stable.GetTermManager().getTerm()
	reply.Status = rf.areValidAppendEntries(args, reply)
	if reply.Status != SUCCESS {
		rf.LogWarn("Denied append entries to", args.LeaderId, ".Reason:", reply.Status)
		return
	} else {
		rf.LogDebug("Received Valid append entries from", args.LeaderId, "Args:", *args)
	}

	if rf.stable.AppendMultipleEntries(rf.getCommitIndex(), args.LogEntries) {
		rf.transitToNewRaftStateWithTerm(FOLLOWER, args.Term)
		rf.setLeaderPeerIndex(args.LeaderId)
		rf.setCommitIndexIfValid(min(args.LeaderCommit, rf.stable.GetLastLogIndex()))
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.LogDebug("Append Entries - server:", server, "args:", *args)
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	if !ok {
		rf.LogError("AppendEntries Rpc to", server, "failed")
	}
	return ok
}

func (rf *Raft) sendEntry(server int, entries []utils.LogEntry, reply *AppendEntriesReply) bool {
	prevLogEntry := rf.stable.GetLogEntry(rf.nextIndex[server].Load() - 1)
	args := rf.getAppendEntriesArgs(entries, prevLogEntry)
	ok := rf.sendAppendEntries(server, args, reply)
	return ok
}

func (rf *Raft) getAppendEntriesArgs(logEntries []utils.LogEntry, prevLogEntry utils.LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         rf.stable.GetTermManager().getTerm(),
		LeaderId:     rf.getSelfPeerIndex(),
		PrevLogIndex: prevLogEntry.LogIndex,
		PrevLogTerm:  prevLogEntry.LogTerm,
		LogEntries:   logEntries,
		LeaderCommit: rf.getCommitIndex(),
	}
}
