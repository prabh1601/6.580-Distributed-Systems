package raft

type AppendEntriesArgs struct {
	Term         int32      // leader's term
	LeaderId     int        // peer index of leader
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int32      // term of prevLogIndex entry
	LogEntries   []LogEntry // log entries to store
	LeaderCommit int32      // leader's commit index
}

type AppendEntriesReply struct {
	Term    int32
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// todo : implement
func (rf *Raft) areValidAppendEntries(args *AppendEntriesArgs) bool {
	if rf.GetTermManager().GetTerm() > args.Term {
		return false
	}

	if rf.GetLogLength() > args.PrevLogIndex && rf.GetLogEntry(args.PrevLogIndex).LogTerm != args.PrevLogTerm {
		return false
	}

	return true
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.LogInfo("Received append entries request from", args.LeaderId)
	reply.Term = rf.GetTermManager().GetTerm()
	if !rf.areValidAppendEntries(args) {
		reply.Success = false
		rf.LogWarn("Denied append entries to", args.LeaderId)
		return
	}

	reply.Success = true
	rf.updateHeartBeat()
	rf.transitToNewRaftStateWithTerm(FOLLOWER, args.Term)
	// todo : implement
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.LogInfo("Sending append entries to", server)
	executionResult := doExecutionWithTimeout(func() bool {
		ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
		if !ok {
			rf.LogError("AppendEntries Rpc to", server, "failed abruptly")
		}
		return ok
	}, func() { rf.LogError("RequestVote Rpc to", server, "timed out") })

	return executionResult == SUCESS
}

func (rf *Raft) sendHeartBeat(server int, reply *AppendEntriesReply) bool {
	args := rf.getAppendEntriesArgs(nil)
	ok := rf.sendAppendEntries(server, args, reply)
	return ok
}

func (rf *Raft) getAppendEntriesArgs(logEntries []LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:     rf.GetTermManager().GetTerm(),
		LeaderId: rf.GetSelfPeerIndex(),
		//PrevLogIndex:,
		//PrevLogTerm:,
		LogEntries:   logEntries,
		LeaderCommit: rf.GetCommitIndex(),
	}
}
