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
	RpcId        int64
	Term         int32
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
}

func (rv RequestVoteArgs) GetRpcId() int64 {
	return rv.RpcId
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	RpcId       int64
	Term        int32
	VoteGranted bool
}

func (rv RequestVoteReply) GetRpcId() int64 {
	return rv.RpcId
}

func (rv RequestVoteReply) GetReply() any {
	return rv
}

func (rf *Raft) isCandidateLogUptoDate(args *RequestVoteArgs) bool {
	lastLogEntry := rf.stable.GetLastLogEntry()
	rf.LogDebug("requestVote Args", *args, "Last Log", lastLogEntry)
	if args.LastLogTerm == lastLogEntry.LogTerm {
		return args.LastLogIndex >= lastLogEntry.LogIndex
	}

	return args.LastLogTerm > lastLogEntry.LogTerm
}

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.LogInfo("Received vote request from", args.CandidateId)
	reply.VoteGranted = false
	reply.Term = rf.getTerm()
	candidateTermNotBehind := reply.Term <= args.Term
	candidateLogNotBehind := rf.isCandidateLogUptoDate(args)
	if candidateTermNotBehind && candidateLogNotBehind {
		reply.VoteGranted = rf.grantVoteIfPossible(args.CandidateId, args.Term)
	}

	if reply.VoteGranted || rf.getTerm() < args.Term {
		rf.transitToNewRaftStateWithTerm(FOLLOWER, args.Term)
		if reply.VoteGranted {
			rf.LogInfo("Successfully granted vote to", args.CandidateId)
		} else {
			rf.LogInfo("Didnt grant vote but Changed to better term from request by:", args.CandidateId)
		}
	}

	if !reply.VoteGranted {
		reason := "Already voted in current term"
		if !candidateLogNotBehind {
			reason = "Stale Log"
		}

		if !candidateTermNotBehind {
			reason = "Stale Term"
		}

		rf.LogWarn("Denied vote request to", args.CandidateId, ".Reason :", reason)
	}
}

func (rf *Raft) sendRequestVote(server int) (bool, RequestVoteReply) {
	rpcId := utils.Nrand()
	lastLogEntry := rf.stable.GetLastLogEntry()
	rf.LogInfo("Sending vote request to", server, "rpcId :", rpcId)
	ok, reply := utils.ExecuteRPC[RequestVoteReply](func() (bool, RequestVoteReply) {
		args := rf.getRequestVoteArgs(rpcId, lastLogEntry)
		reply := &RequestVoteReply{RpcId: args.RpcId}
		rf.LogDebug("Sending Request Vote - server:", server, "args:", *args)
		ok := false
		if rf.is(CANDIDATE) {
			ok = rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
		}
		return ok, *reply
	})
	if !ok {
		rf.LogError("RequestVote Rpc to", server, "failed")
	}
	return ok, reply
}

func (rf *Raft) getRequestVoteArgs(rpcId int64, lastLogEntry LogEntry) *RequestVoteArgs {
	return &RequestVoteArgs{
		RpcId:        rpcId,
		Term:         rf.getTerm(),
		CandidateId:  rf.getSelfPeerIndex(),
		LastLogIndex: lastLogEntry.LogIndex,
		LastLogTerm:  lastLogEntry.LogTerm,
	}
}
