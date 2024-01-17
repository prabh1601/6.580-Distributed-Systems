package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetStatus() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server
//

import (
	"6.5840/utils"
	"go.uber.org/zap"
	"math"
	"sync"
	"sync/atomic"
	"time"

	//"6.5840/labgob"
	"6.5840/labrpc"
)

type RaftState int32

const (
	LEADER RaftState = iota
	CANDIDATE
	FOLLOWER
)

func (e RaftState) String() string {
	switch e {
	case LEADER:
		return "Leader"
	case CANDIDATE:
		return "Candidate"
	case FOLLOWER:
		return "Follower"
	default:
		return "Invalid State"
	}
}

type TermManager struct {
	State           RaftState // current State
	term            int32     // current term
	electionTimeout int64
}

type VoteManager struct {
	votedFor            int   // who got last vote from this server
	latestTermWhenVoted int32 // when was last vote given from this server
}

func (tm *TermManager) GetTerm() int32 {
	return tm.term
}

func (tm *TermManager) GetCurrentState() RaftState {
	return tm.State
}

func (tm *TermManager) GetElectionTimeout() int64 {
	return tm.electionTimeout
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	applyCond           *sync.Cond                  // condition for apply goroutine to sleep if not many conditions are available for being committed
	applyCh             chan ApplyMsg               // channel for delegating the committed message to State machine
	termManager         atomic.Pointer[TermManager] // Store info related to current term
	VoteManager         atomic.Pointer[VoteManager] // vote info related to vote
	peers               []*labrpc.ClientEnd         // RPC end points of all peers
	persister           *Persister                  // Object to hold this peer's persisted State
	me                  int                         // this peer's index into peers[]
	dead                int32                       // set by Kill()
	commitIndex         int32                       // index of highest log entry known to be committed
	lastApplied         int32                       // index of highest log entry known to be applied
	utils.ConcurrentLog                             // log
	logger              *zap.SugaredLogger          // logger to help log stuff
	lastHeartBeatEpoch  int64                       // epoch of last received hearbeat
	nextIndex           []atomic.Int32              // for each server, index of the next log entry to send to that server
	matchIndex          []atomic.Int32              // for each server, index of highest log entry known to be replicated on server

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server must maintain.
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyCommandToSM(msg ApplyMsg) {
	rf.applyCh <- msg
	rf.LogInfo("Sent", *(&msg), "for application to State Machine")
}

func (rf *Raft) GetCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}

// Commit index can only increment
func (rf *Raft) SetCommitIndexIfValid(index int32) {
	canTry := true
	for canTry {
		currentIdx := rf.GetCommitIndex()
		if index <= currentIdx {
			canTry = false
			continue
		}

		if atomic.CompareAndSwapInt32(&rf.commitIndex, currentIdx, index) {
			rf.LogWarn("Committed log entries upto index", index)
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) GetLastAppliedIndex() int32 {
	return atomic.LoadInt32(&rf.lastApplied)
}

func (rf *Raft) SetLastAppliedIndex(index int32) {
	atomic.StoreInt32(&rf.lastApplied, index)
}

func (rf *Raft) hasElectionTimeoutElapsed() bool {
	return utils.GetCurrentTimeInMs()-rf.GetLastHeartBeatEpoch() >= rf.GetTermManager().GetElectionTimeout()
}

func (rf *Raft) GetLastHeartBeatEpoch() int64 {
	return atomic.LoadInt64(&rf.lastHeartBeatEpoch)
}

func (rf *Raft) updateHeartBeat() {
	atomic.StoreInt64(&rf.lastHeartBeatEpoch, utils.GetCurrentTimeInMs())
}

func (rf *Raft) GetMajorityCount() int {
	peerCount := len(rf.peers)
	return (peerCount + 1) / 2
}

func (rf *Raft) GetTermManager() *TermManager {
	return rf.termManager.Load()
}

func (rf *Raft) SetTermManager(oldManager, newManager *TermManager) bool {
	return rf.termManager.CompareAndSwap(oldManager, newManager)
}

// grant vote if not already voted in current term
func (rf *Raft) grantVoteIfPossible(requestingPeer int, term int32) bool {
	oldVoteManager := rf.GetVoteManager()

	if oldVoteManager.latestTermWhenVoted < term {
		newVoteManager := &VoteManager{votedFor: requestingPeer, latestTermWhenVoted: term}
		return rf.SetVoteManager(oldVoteManager, newVoteManager)
	}

	return false
}

func (rf *Raft) GetVoteManager() *VoteManager {
	return rf.VoteManager.Load()
}

func (rf *Raft) SetVoteManager(oldManager, newManager *VoteManager) bool {
	return rf.VoteManager.CompareAndSwap(oldManager, newManager)
}

func (rf *Raft) GetSelfPeerIndex() int {
	return rf.me
}

func (rf *Raft) getNewCandidateState(term int32) *TermManager {
	return &TermManager{
		State:           CANDIDATE,
		term:            term,
		electionTimeout: utils.GetRandomElectionTimeoutPeriod(),
	}
}

func (rf *Raft) getNewLeaderState(term int32) *TermManager {
	return &TermManager{
		State:           LEADER,
		term:            term,
		electionTimeout: utils.GetRandomElectionTimeoutPeriod(),
	}
}

func (rf *Raft) getNewFollowerState(term int32) *TermManager {
	return &TermManager{
		State:           FOLLOWER,
		term:            term,
		electionTimeout: utils.GetRandomElectionTimeoutPeriod(),
	}
}

func (rf *Raft) transitToNewRaftState(newState RaftState) bool {
	return rf.transitToNewRaftStateWithTerm(newState, math.MaxInt32)
}

func (rf *Raft) transitToNewRaftStateWithTerm(newState RaftState, newTerm int32) bool {

	oldTermManager := rf.GetTermManager()

	if newTerm < oldTermManager.GetTerm() {
		rf.LogError("Trying to transition to old term", newTerm, "as compared to current term", oldTermManager.GetTerm())
		return false
	}

	var newTermManager *TermManager
	switch newState {
	case LEADER:
		newTermManager = rf.getNewLeaderState(oldTermManager.GetTerm())
	case CANDIDATE:
		newTermManager = rf.getNewCandidateState(oldTermManager.GetTerm() + 1)
	case FOLLOWER:
		newTermManager = rf.getNewFollowerState(newTerm)
	}

	successfulTransition := rf.SetTermManager(oldTermManager, newTermManager)
	if successfulTransition {
		rf.LogInfo("Transitioned from", *oldTermManager, "to", *newTermManager)
	}

	return successfulTransition
}

func (rf *Raft) initializeOtherPeerMetaData() {
	lastLogIdx := rf.GetLastLogIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i].Store(lastLogIdx + 1)
		rf.matchIndex[i].Store(0)
	}
}

func (rf *Raft) updateLatestCommitIndex() {
	minPossibleIdx := rf.GetCommitIndex()
	maxPossibleIdx := rf.GetLogLength()

	for minPossibleIdx+1 < maxPossibleIdx {
		mid := (minPossibleIdx + maxPossibleIdx) / 2
		count := 0

		for peerIdx := 0; peerIdx < len(rf.peers); peerIdx++ {
			latestMatchingIdx := rf.matchIndex[peerIdx].Load()
			if latestMatchingIdx >= mid && rf.GetLogEntry(latestMatchingIdx).LogTerm == rf.GetTermManager().GetTerm() {
				count++
			}
		}

		if count >= rf.GetMajorityCount() {
			minPossibleIdx = mid
		} else {
			maxPossibleIdx = mid
		}
	}

	rf.SetCommitIndexIfValid(minPossibleIdx)
}

// this will serve as medium for heartbeat as well as replicating new entries over to the follower
func (rf *Raft) replicateNewEntries(peerIdx int) {
	// Your code here (2B).
	nextLogIdx := rf.nextIndex[peerIdx].Load()
	currentLogLength := rf.GetLogLength()
	if nextLogIdx != currentLogLength {
		rf.LogInfo("Sending log entries from", nextLogIdx, "to", currentLogLength-1, "to peer", peerIdx)
	} else {
		rf.LogInfo("Sending heartbeat update to", peerIdx)
	}

	if peerIdx != rf.GetSelfPeerIndex() {
		reply := &AppendEntriesReply{}
		ok := rf.sendEntry(peerIdx, rf.GetLogEntries(nextLogIdx, currentLogLength), reply)
		if ok {
			switch reply.Status {
			case SUCCESS:
				rf.nextIndex[peerIdx].Store(currentLogLength)
				rf.matchIndex[peerIdx].Store(currentLogLength - 1)
				if nextLogIdx != currentLogLength {
					rf.LogInfo("Replicated log entries from", nextLogIdx, "to", currentLogLength-1, "on server", peerIdx)
				} else {
					rf.LogInfo("Updated Heartbeat for", peerIdx)
				}
			case LOG_INCONSISTENCY:
				rf.nextIndex[peerIdx].Add(-1)
				rf.LogInfo("Found Inconsistent Logs at", peerIdx, "trying again from", rf.nextIndex[peerIdx].Load())
				rf.replicateNewEntries(peerIdx)
				return
			case STALE_STATE:
				rf.transitToNewRaftStateWithTerm(FOLLOWER, reply.Term)
				return
			}
		}
	}

	rf.updateLatestCommitIndex()
}

func (rf *Raft) maintainLeadership() {
	rf.LogInfo("Starting as Leader")
	rf.initializeOtherPeerMetaData()

	for rf.GetTermManager().GetCurrentState() == LEADER {
		for peer := 0; peer < len(rf.peers); peer++ {
			if rf.GetSelfPeerIndex() != peer {
				go rf.replicateNewEntries(peer)
			}
		}

		time.Sleep(utils.GetRandomDurationInMs(50, 100))
	}

	rf.LogWarn("Stepped down from Leader State")
}

func (rf *Raft) beginElection() {
	if rf.GetTermManager().GetCurrentState() != CANDIDATE || !rf.grantVoteIfPossible(rf.GetSelfPeerIndex(), rf.GetTermManager().GetTerm()) {
		rf.LogError("Trying to initiate elections from an invalid State candidate peer. Current State :", rf.GetTermManager().GetCurrentState())
		return
	}

	rf.LogInfo("Starting election")
	var reqVoteLatch sync.WaitGroup
	reqVoteLatch.Add(len(rf.peers) - 1)
	voteChan := make(chan RequestVoteReply, len(rf.peers)-1)

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.GetSelfPeerIndex() {
			go func(requestPeer int, requestVoteLatch *sync.WaitGroup) {
				// this is a blocking call, need to add timeout
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(requestPeer, rf.getRequestVoteArgs(), &reply)
				if ok {
					rf.LogInfo("Received RequestVote Rpc reply from", requestPeer, reply)
					voteChan <- reply
				}
				reqVoteLatch.Done()
			}(peer, &reqVoteLatch)
		}
	}

	// update heartbeat to reset election timer
	rf.updateHeartBeat()

	timeoutChan := make(chan bool, 1)
	oncePusher := sync.Once{}
	defer close(timeoutChan)

	// this goroutine will wait for all rpc to return (or timeout)
	go func(requestVoteLatch *sync.WaitGroup) {
		reqVoteLatch.Wait()
		rf.LogInfo("All RequestVote Rpc ended")
		oncePusher.Do(func() {
			timeoutChan <- false
		})
		close(voteChan)
	}(&reqVoteLatch)

	// this goroutine will die once State changes
	go func() {
		timeoutHappened := rf.waitForElectionTimeout()
		oncePusher.Do(func() {
			timeoutChan <- timeoutHappened
		})
	}()

	// block all responses have been accumulated or election times-out
	electionTimedOut := <-timeoutChan
	if electionTimedOut {
		// election timeout will automatically transition to candidate
		return
	}

	votesReceived := 1
	for voteReply := range voteChan {
		if voteReply.VoteGranted {
			votesReceived++
		} else if voteReply.Term > rf.GetTermManager().GetTerm() {
			rf.transitToNewRaftStateWithTerm(FOLLOWER, voteReply.Term)
		}
	}

	rf.LogInfo("Election Voting Stats - Required :", rf.GetMajorityCount(), "Got :", votesReceived)

	// if we are still candidate and got required majority, transit to leader
	if rf.GetTermManager().GetCurrentState() == CANDIDATE && votesReceived >= rf.GetMajorityCount() {
		rf.LogInfo("Received majority, Transitioning to", LEADER)
		rf.transitToNewRaftState(LEADER)
		return
	}

	rf.LogInfo("Lost Election, Returning to", FOLLOWER)
	rf.transitToNewRaftStateWithTerm(FOLLOWER, rf.GetTermManager().GetTerm())
}

func (rf *Raft) waitForElectionTimeout() bool {
	termManager := rf.GetTermManager()
	originalState := termManager.GetCurrentState()
	for termManager.GetCurrentState() == originalState {
		if rf.hasElectionTimeoutElapsed() {
			rf.LogWarn("Election timeout elapsed")
			rf.transitToNewRaftState(CANDIDATE)
			return true
		}
		time.Sleep(utils.GetRandomDurationInMs(5, 10))
		termManager = rf.GetTermManager()
	}
	return false
}

// GetStatus returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetStatus() (int, bool) {
	currenttermManager := *rf.GetTermManager()
	term := int(currenttermManager.GetTerm())
	isleader := currenttermManager.GetCurrentState() == LEADER
	return term, isleader
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.GetTermManager().GetCurrentState() {
		case LEADER:
			rf.maintainLeadership()
		case CANDIDATE:
			rf.beginElection()
		case FOLLOWER:
			rf.waitForElectionTimeout()
		}
	}
}

// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted State.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applyCommitted() {
	for rf.killed() != true {
		lastAppliedIndex := rf.GetLastAppliedIndex()
		lastCommitIndex := rf.GetCommitIndex()

		if lastCommitIndex == lastAppliedIndex {
			rf.applyCond.L.Lock()
			rf.applyCond.Wait()
			rf.applyCond.L.Unlock()
			continue
		}

		rf.LogInfo("New Committed Entries found. Applying log entries from index", lastAppliedIndex+1, "to", lastCommitIndex)

		logEntries := rf.GetLogEntries(lastAppliedIndex+1, lastCommitIndex+1)
		for _, logEntry := range logEntries {
			applyMsg := ApplyMsg{Command: logEntry.LogCommand, CommandIndex: int(logEntry.LogIndex), CommandValid: true}
			rf.applyCommandToSM(applyMsg)
		}

		rf.LogInfo("Applied entries till index", lastCommitIndex)
		rf.SetLastAppliedIndex(lastCommitIndex)
	}
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	termManager := *rf.GetTermManager()
	index := -1
	term := termManager.GetTerm()
	isLeader := termManager.GetCurrentState() == LEADER

	if isLeader {
		appendedEntry := rf.AppendEntry(command, term)
		index = int(appendedEntry.LogIndex)
		me := rf.GetSelfPeerIndex()
		rf.matchIndex[me].Store(int32(index))
		rf.nextIndex[me].Store(int32(index))
		rf.LogWarn("Appended command", command, "at index", appendedEntry.LogIndex)
	}

	return index, int(term), isLeader
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		applyCond:          sync.NewCond(&sync.Mutex{}),
		applyCh:            applyCh,
		termManager:        atomic.Pointer[TermManager]{},
		VoteManager:        atomic.Pointer[VoteManager]{},
		peers:              peers,
		persister:          persister,
		me:                 me,
		dead:               0,
		ConcurrentLog:      utils.ConcurrentLog{LogArray: make([]utils.LogEntry, 1e5), LogIdx: 0, LogLock: sync.RWMutex{}},
		logger:             GetLogger(),
		lastHeartBeatEpoch: utils.GetCurrentTimeInMs(),
		nextIndex:          make([]atomic.Int32, len(peers)),
		matchIndex:         make([]atomic.Int32, len(peers)),
		commitIndex:        0,
		lastApplied:        0,
	}
	rf.termManager.Store(&TermManager{State: FOLLOWER, term: 0, electionTimeout: utils.GetRandomElectionTimeoutPeriod()})
	rf.VoteManager.Store(&VoteManager{votedFor: -1, latestTermWhenVoted: 0})

	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to send committed commands to state machine
	go rf.applyCommitted()

	return rf
}
