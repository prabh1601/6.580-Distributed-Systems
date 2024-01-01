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
//   in the same server.
//

import (
	"fmt"
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
	state           RaftState // current state
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
	return tm.state
}

func (tm *TermManager) GetElectiontimeout() int64 {
	return tm.electionTimeout
}

type LogCommand struct {
}

type LogEntry struct {
	LogIndex   int
	LogTerm    int32
	LogCommand LogCommand
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu                 sync.Mutex                  // Lock to protect shared access to this peer's state
	termManager        atomic.Pointer[TermManager] // Store info related to current term
	VoteManager        atomic.Pointer[VoteManager] // vote info related to vote
	peers              []*labrpc.ClientEnd         // RPC end points of all peers
	persister          *Persister                  // Object to hold this peer's persisted state
	me                 int                         // this peer's index into peers[]
	dead               int32                       // set by Kill()
	commitIndex        int32                       // index of highest log entry known to be committed
	log                []LogEntry                  // log of operations
	logger             *zap.SugaredLogger          // logger to help log stuff
	lastHeartBeatEpoch int64                       // epoch of last received hearbeat

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) GetCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}

func (rf *Raft) hasElectionTimeoutElapsed() bool {
	return getCurrentTimeInMs()-rf.GetLastHeartBeatEpoch() >= rf.GetTermManager().GetElectiontimeout()
}

func (rf *Raft) GetLastHeartBeatEpoch() int64 {
	return atomic.LoadInt64(&rf.lastHeartBeatEpoch)
}

func (rf *Raft) updateHeartBeat() {
	atomic.StoreInt64(&rf.lastHeartBeatEpoch, getCurrentTimeInMs())
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

	if oldVoteManager.latestTermWhenVoted < term || (oldVoteManager.latestTermWhenVoted == term && oldVoteManager.votedFor == -1) {
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

// todo : load log atomically
func (rf *Raft) GetLog() []LogEntry {
	return rf.log
}

func (rf *Raft) GetLogLength() int {
	return len(rf.log)
}

func (rf *Raft) GetLastLongIndex() int {
	return rf.GetLogLength() - 1
}

func (rf *Raft) GetLastLogEntry() LogEntry {
	return rf.GetLogEntry(rf.GetLastLongIndex())
}

func (rf *Raft) GetLogEntry(logIndex int) LogEntry {
	if logIndex == -1 {
		return LogEntry{LogIndex: -1, LogTerm: int32(-1), LogCommand: LogCommand{}}
	}
	return rf.log[logIndex]
}

func (rf *Raft) GetSelfPeerIndex() int {
	return rf.me
}

// todo : is this thread safe -> should be now
func (rf *Raft) getNewCandidateState(oldTermManager *TermManager) (*TermManager, bool) {
	return &TermManager{
		state:           CANDIDATE,
		term:            oldTermManager.GetTerm() + 1,
		electionTimeout: getRandomElectionTimeoutPeriod(),
	}, true
}

func (rf *Raft) getNewLeaderState(oldTermManager *TermManager) (*TermManager, bool) {
	if oldTermManager.GetCurrentState() != CANDIDATE {
		return &TermManager{}, false
	}

	return &TermManager{
		state:           LEADER,
		term:            oldTermManager.GetTerm(),
		electionTimeout: getRandomElectionTimeoutPeriod(),
	}, true
}

func (rf *Raft) getNewFollowerState(oldTermManager *TermManager, term int32) (*TermManager, bool) {
	return &TermManager{
		state:           FOLLOWER,
		term:            term,
		electionTimeout: getRandomElectionTimeoutPeriod(),
	}, true
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
	var isValidTransition bool
	switch newState {
	case LEADER:
		newTermManager, isValidTransition = rf.getNewLeaderState(oldTermManager)
	case CANDIDATE:
		newTermManager, isValidTransition = rf.getNewCandidateState(oldTermManager)
	case FOLLOWER:
		newTermManager, isValidTransition = rf.getNewFollowerState(oldTermManager, newTerm)
	}

	// use short-circuiting of logical statement, if not valid transition then new state wont be set
	successfulTransition := isValidTransition && rf.SetTermManager(oldTermManager, newTermManager)
	if successfulTransition {
		rf.LogInfo("Transitioned from", fmt.Sprintf("%+v", *oldTermManager), "to", fmt.Sprintf("%+v", *newTermManager))
	}

	return successfulTransition
}

func (rf *Raft) maintainLeadership() {
	rf.LogInfo("Starting as Leader")

	for rf.GetTermManager().GetCurrentState() == LEADER {
		for peer := 0; peer < len(rf.peers); peer++ {
			if rf.GetSelfPeerIndex() != peer {
				go func(peerIdx int) {
					reply := &AppendEntriesReply{}
					ok := rf.sendHeartBeat(peerIdx, reply)
					if ok && !reply.Success {
						rf.transitToNewRaftStateWithTerm(FOLLOWER, reply.Term)
					}
				}(peer)
			}
		}

		time.Sleep(getRandomDurationInMs(100, 120))
	}

	rf.LogWarn("Stepped down from Leader")
}

func (rf *Raft) beginElection() {
	if rf.GetTermManager().GetCurrentState() != CANDIDATE || !rf.grantVoteIfPossible(rf.GetSelfPeerIndex(), rf.GetTermManager().GetTerm()) {
		rf.LogError("Trying to initiate elections from an invalid state candidate peer. Current state :", rf.GetTermManager().GetCurrentState())
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
					rf.LogInfo("Received RequestVote Rpc reply from", requestPeer, fmt.Sprintf("%+v", reply))
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

	// this goroutine will die once state changes
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
		time.Sleep(getRandomDurationInMs(5, 10))
		termManager = rf.GetTermManager()
	}
	return false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetStatus() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currenttermManager := *rf.GetTermManager()
	term := int(currenttermManager.GetTerm())
	isleader := currenttermManager.GetCurrentState() == LEADER
	return term, isleader
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

// as each Raft peer becomes aware that successive log entries are
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

// save Raft's persistent state to stable storage,
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

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:                 sync.Mutex{},
		termManager:        atomic.Pointer[TermManager]{},
		VoteManager:        atomic.Pointer[VoteManager]{},
		peers:              peers,
		persister:          persister,
		me:                 me,
		dead:               0,
		log:                make([]LogEntry, 0), // todo : revisit the buffer size
		logger:             GetLogger(),
		lastHeartBeatEpoch: getCurrentTimeInMs(),
	}
	rf.termManager.Store(&TermManager{state: FOLLOWER, term: 0, electionTimeout: getRandomElectionTimeoutPeriod()})
	rf.VoteManager.Store(&VoteManager{votedFor: -1, latestTermWhenVoted: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
