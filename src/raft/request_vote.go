package raft

import "6.5840/utils"

// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return. Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int32
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

func (rf *Raft) isCandidateLogUptoDate(args *RequestVoteArgs) bool {
	lastLogEntry := rf.stable.GetLastLogEntry()
	if args.LastLogTerm == lastLogEntry.LogTerm {
		return args.LastLogIndex >= lastLogEntry.LogIndex
	}

	return args.LastLogTerm > lastLogEntry.LogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.LogInfo("Received vote request from", args.CandidateId)
	reply.VoteGranted = false
	reply.Term = rf.stable.GetTermManager().GetTerm()
	termUptoDate := reply.Term <= args.Term
	logUptoDate := rf.isCandidateLogUptoDate(args)
	if termUptoDate && logUptoDate {
		reply.VoteGranted = rf.grantVoteIfPossible(args.CandidateId, args.Term)
	}

	if reply.VoteGranted {
		rf.transitToNewRaftStateWithTerm(FOLLOWER, args.Term)
		rf.updateHeartBeat()
		rf.LogInfo("Successfully granted vote to", args.CandidateId)
	} else {
		reason := "Already voted in current term"
		if !logUptoDate {
			reason = "Stale Log"
		}

		if !termUptoDate {
			reason = "Stale Term"
		}

		rf.LogWarn("Denied vote request to", args.CandidateId, ". Reason :", reason)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.LogInfo("Sending vote request to", server)
	executionResult := utils.ExecuteRpcWithTimeout(func() bool {
		ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
		if !ok {
			rf.LogError("RequestVote Rpc to", server, "failed abruptly")
		}
		return ok
	}, func() { rf.LogError("RequestVote Rpc to", server, "timed out") })

	return executionResult == utils.SUCCESS
}

func (rf *Raft) getRequestVoteArgs() *RequestVoteArgs {
	lastLogEntry := rf.stable.GetLastLogEntry()
	return &RequestVoteArgs{
		Term:         rf.stable.GetTermManager().GetTerm(),
		CandidateId:  rf.GetSelfPeerIndex(),
		LastLogIndex: lastLogEntry.LogIndex,
		LastLogTerm:  lastLogEntry.LogTerm,
	}
}
