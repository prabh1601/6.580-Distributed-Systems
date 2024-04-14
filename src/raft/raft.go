package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetStatus() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server
//

import (
	"6.5840/labgob"
	"6.5840/utils"
	"bytes"
	cmap "github.com/orcaman/concurrent-map/v2"
	"math"
	"strconv"
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
	Term            int32     // current Term
	ElectionTimeout int64
}

type VoteManager struct {
	VotedFor            int   // who got last vote from this server
	LatestTermWhenVoted int32 // when was last vote given from this server
}

type SnapshotManager struct {
	Data  []byte
	Index int32
	Term  int32
}

func (tm *TermManager) getTerm() int32 {
	return tm.Term
}

func (tm *TermManager) getCurrentState() RaftState {
	return tm.State
}

func (tm *TermManager) getElectionTimeout() int64 {
	return tm.ElectionTimeout
}

func (sm *SnapshotManager) getData() []byte {
	return sm.Data
}

type StableStorage struct {
	Snapshot            atomic.Pointer[SnapshotManager] // Store info related to current snapshot
	TermManager         atomic.Pointer[TermManager]     // Store info related to current Term
	VoteManager         atomic.Pointer[VoteManager]     // vote info related to vote
	utils.ConcurrentLog                                 // log
}

func (ss *StableStorage) StoreNewSnapshot(oldSnapshot, newSnapshot *SnapshotManager) bool {
	return ss.Snapshot.CompareAndSwap(oldSnapshot, newSnapshot)
}

func (ss *StableStorage) GetSnapshotManager() *SnapshotManager {
	return ss.Snapshot.Load()
}

func (ss *StableStorage) GetVoteManager() *VoteManager {
	return ss.VoteManager.Load()
}

func (ss *StableStorage) SetVoteManager(oldManager, newManager *VoteManager) bool {
	return ss.VoteManager.CompareAndSwap(oldManager, newManager)
}

func (ss *StableStorage) GetTermManager() *TermManager {
	return ss.TermManager.Load()
}

func (ss *StableStorage) SetTermManager(oldManager, newManager *TermManager) bool {
	return ss.TermManager.CompareAndSwap(oldManager, newManager)
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	stable             *StableStorage      // Stable storage which will be used to
	persister          *Persister          // Object to hold this peer's persisted State
	applyCond          *sync.Cond          // condition for apply goroutine to sleep if not many conditions are available for being committed
	applyCh            chan ApplyMsg       // channel for delegating the committed message to State machine
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	me                 int                 // this peer's index into peers[]
	leaderId           int32               // id of current leader
	dead               int32               // set by Kill()
	commitIndex        int32               // index of highest log entry known to be committed
	lastApplied        int32               // index of highest log entry known to be applied
	lastHeartBeatEpoch int64               // epoch of last received hearbeat
	nextIndex          []atomic.Int32      // for each server, index of the next log entry to send to that server
	matchIndex         []atomic.Int32      // for each server, index of highest log entry known to be replicated on server
	utils.Logger                           // logger to help log stuff
}

func (rf *Raft) GetLoggerPrefix() string {
	termManager := rf.stable.GetTermManager()
	return "[RAFT] [Peer : " + strconv.Itoa(rf.getSelfPeerIndex()) + "] [Term : " + strconv.Itoa(int(termManager.getTerm())) + "] [State : " + termManager.getCurrentState().String() + "] "
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
	rf.LogDebug("Applies to RSM :", *(&msg))
}

func (rf *Raft) GetCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}

// Commit index can only increment
func (rf *Raft) setCommitIndexIfValid(index int32) {
	canTry := true
	for canTry {
		currentIdx := rf.GetCommitIndex()
		if index <= currentIdx {
			canTry = false
			continue
		}

		if atomic.CompareAndSwapInt32(&rf.commitIndex, currentIdx, index) {
			rf.LogInfo("Committed log entries upto index", index)
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) GetLastAppliedIndex() int32 {
	return atomic.LoadInt32(&rf.lastApplied)
}

func (rf *Raft) setLastAppliedIndex(index int32) {
	atomic.StoreInt32(&rf.lastApplied, index)
}

func (rf *Raft) hasElectionTimeoutElapsed() bool {
	return utils.GetCurrentTimeInMs()-rf.getLastHeartBeatEpoch() >= rf.stable.GetTermManager().getElectionTimeout()
}

func (rf *Raft) getLastHeartBeatEpoch() int64 {
	return atomic.LoadInt64(&rf.lastHeartBeatEpoch)
}

func (rf *Raft) updateHeartBeat() {
	atomic.StoreInt64(&rf.lastHeartBeatEpoch, utils.GetCurrentTimeInMs())
}

func (rf *Raft) getMajorityCount() int {
	peerCount := len(rf.peers)
	return (peerCount + 1) / 2
}

// grant vote if not already voted in current Term
func (rf *Raft) grantVoteIfPossible(requestingPeer int, term int32) bool {

	voteGranted := false
	canTry := true
	for canTry {
		oldVoteManager := rf.stable.GetVoteManager()
		if oldVoteManager.LatestTermWhenVoted >= term {
			canTry = false
			continue
		}

		newVoteManager := &VoteManager{VotedFor: requestingPeer, LatestTermWhenVoted: term}
		voteGranted = rf.stable.SetVoteManager(oldVoteManager, newVoteManager)
	}

	if voteGranted {
		rf.persist()
	}

	return voteGranted
}

func (rf *Raft) GetLeaderPeerIndex() int { return int(atomic.LoadInt32(&rf.leaderId)) }

func (rf *Raft) setLeaderPeerIndex(leaderId int) { atomic.StoreInt32(&rf.leaderId, int32(leaderId)) }

func (rf *Raft) getSelfPeerIndex() int {
	return rf.me
}

func (rf *Raft) getNewCandidateState(term int32) *TermManager {
	return &TermManager{
		State:           CANDIDATE,
		Term:            term,
		ElectionTimeout: utils.GetRandomElectionTimeoutPeriod(),
	}
}

func (rf *Raft) getNewLeaderState(term int32) *TermManager {
	return &TermManager{
		State:           LEADER,
		Term:            term,
		ElectionTimeout: utils.GetRandomElectionTimeoutPeriod(),
	}
}

func (rf *Raft) getNewFollowerState(term int32) *TermManager {
	return &TermManager{
		State:           FOLLOWER,
		Term:            term,
		ElectionTimeout: utils.GetRandomElectionTimeoutPeriod(),
	}
}

func (rf *Raft) transitToNewRaftState(newState RaftState) bool {
	return rf.transitToNewRaftStateWithTerm(newState, math.MaxInt32)
}

func (rf *Raft) transitToNewRaftStateWithTerm(newState RaftState, newTerm int32) bool {

	oldTermManager := rf.stable.GetTermManager()
	if newTerm < oldTermManager.getTerm() {
		rf.LogError("Trying to transition to old Term", newTerm, "as compared to current Term", oldTermManager.getTerm())
		return false
	}

	var newTermManager *TermManager
	switch newState {
	case LEADER:
		newTermManager = rf.getNewLeaderState(oldTermManager.getTerm())
	case CANDIDATE:
		newTermManager = rf.getNewCandidateState(oldTermManager.getTerm() + 1)
	case FOLLOWER:
		newTermManager = rf.getNewFollowerState(newTerm)
	}

	successfulTransition := rf.stable.SetTermManager(oldTermManager, newTermManager)
	if successfulTransition {
		rf.persist()
		rf.LogInfo("Transitioned from", *oldTermManager, "to", *newTermManager)
	}

	return successfulTransition
}

func (rf *Raft) initializeMetaData() {
	rf.setLeaderPeerIndex(rf.getSelfPeerIndex())

	lastLogIdx := rf.stable.GetLastLogIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i].Store(lastLogIdx + 1)
		rf.matchIndex[i].Store(0)
	}
}

func (rf *Raft) updateLatestCommitIndex() {
	minPossibleIdx := rf.GetCommitIndex()
	maxPossibleIdx := rf.stable.GetLogLength()

	for minPossibleIdx+1 < maxPossibleIdx {
		mid := (minPossibleIdx + maxPossibleIdx) / 2
		count := 0

		for peerIdx := 0; peerIdx < len(rf.peers); peerIdx++ {
			latestMatchingIdx := rf.matchIndex[peerIdx].Load()
			if latestMatchingIdx >= mid && rf.stable.GetLogEntry(latestMatchingIdx).LogTerm == rf.stable.GetTermManager().getTerm() {
				count++
			}
		}

		if count >= rf.getMajorityCount() {
			minPossibleIdx = mid
		} else {
			maxPossibleIdx = mid
		}
	}

	rf.setCommitIndexIfValid(minPossibleIdx)
}

func (rf *Raft) getNextPeerAppendIndex(reply *AppendEntriesReply) int32 {
	return max(1, min(reply.XLen, reply.XIdx-1))
}

// this will serve as medium for heartbeat as well as replicating new entries over to the follower
func (rf *Raft) replicateNewEntries(peerIdx int) {
	// Your code here (2B).
	if peerIdx == rf.getSelfPeerIndex() {
		return
	}

	nextLogIdx := rf.nextIndex[peerIdx].Load()
	currentLogLength := rf.stable.GetLogLength()
	firstOffset := rf.stable.GetFirstIndex()

	// We need to install a snapshot at follower since it is very much behind
	if nextLogIdx <= firstOffset {
		rf.LogInfo("Sending snapshot to server", peerIdx, "as it is much far behind to follow up")
		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(peerIdx, rf.getInstallSnapshotArgs(), reply)
		if ok {
			// snapshot transferred successfully
			rf.nextIndex[peerIdx].Store(firstOffset + 1)
			rf.replicateNewEntries(peerIdx)
		} else if reply.Term > rf.stable.GetTermManager().getTerm() {
			rf.transitToNewRaftStateWithTerm(FOLLOWER, reply.Term)
		}
		return
	}

	if nextLogIdx != currentLogLength {
		rf.LogInfo("Sending log entries from", nextLogIdx, "to", currentLogLength-1, "to peer", peerIdx)
	} else {
		rf.LogInfo("Sending heartbeat update to", peerIdx)
	}

	reply := &AppendEntriesReply{}
	ok := rf.sendEntry(peerIdx, rf.stable.GetLogEntries(nextLogIdx, currentLogLength), reply)
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
			rf.nextIndex[peerIdx].Store(rf.getNextPeerAppendIndex(reply))
			rf.LogInfo("Found Inconsistent Logs at", peerIdx, "trying again from", rf.nextIndex[peerIdx].Load())
			rf.replicateNewEntries(peerIdx)
			return
		case STALE_STATE:
			rf.transitToNewRaftStateWithTerm(FOLLOWER, reply.Term)
			return
		}
	}

	rf.updateLatestCommitIndex()
}

func (rf *Raft) propageEntriesToPeers() {
	for peer := 0; peer < len(rf.peers); peer++ {
		if rf.getSelfPeerIndex() != peer {
			go rf.replicateNewEntries(peer)
		}
	}
}

func (rf *Raft) maintainLeadership() {
	rf.LogWarn("Starting as Leader")
	rf.initializeMetaData()

	for rf.stable.GetTermManager().getCurrentState() == LEADER {
		rf.propageEntriesToPeers()
		time.Sleep(utils.GetRandomDurationInMs(utils.MIN_HEARTBEAT_SEND_WAIT_MS, utils.MAX_HEARTBEAT_SEND_WAIT_MS))
	}

	rf.LogWarn("Stepped down from Leader State")
}

func (rf *Raft) beginElection() {
	maxPeerTerm := rf.stable.GetTermManager().getTerm()
	votesReceived := 1
	defer func() {
		termManager := rf.stable.GetTermManager()
		if termManager.getTerm() < maxPeerTerm || votesReceived < rf.getMajorityCount() {
			rf.LogWarn("Lost Election. Transitioning to follower with term", maxPeerTerm)
			rf.transitToNewRaftStateWithTerm(FOLLOWER, maxPeerTerm)
		}
	}()

	termManager := rf.stable.GetTermManager()
	if termManager.getCurrentState() != CANDIDATE || !rf.grantVoteIfPossible(rf.getSelfPeerIndex(), termManager.getTerm()) {
		rf.LogError("Trying to initiate elections from an invalid State candidate peer. Current State :", *termManager, *rf.stable.GetVoteManager())
		return
	}

	rf.LogWarn("Starting election")
	voteChan := make(chan RequestVoteReply, len(rf.peers)-1)

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.getSelfPeerIndex() {
			go func(requestPeer int) {
				// this is a blocking call, need to add timeout
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(requestPeer, rf.getRequestVoteArgs(), &reply)
				if ok {
					rf.LogInfo("Received RequestVote Rpc reply from", requestPeer, reply)
					voteChan <- reply
				}
			}(peer)
		}
	}

	// update heartbeat to reset election timer
	rf.updateHeartBeat()

	timeoutChan := make(chan bool, 1)

	// this goroutine will die once State changes
	go func() {
		timeoutHappened := rf.waitForElectionTimeout(false)
		if timeoutHappened {
			timeoutChan <- timeoutHappened
		}
	}()

	// block till all responses have been accumulated or election times-out
	electionTimedOut := false
	for votes := 0; votes < len(rf.peers)-1; votes++ {
		select {
		case voteReply := <-voteChan:
			if voteReply.VoteGranted {
				votesReceived++
			} else {
				maxPeerTerm = max(maxPeerTerm, voteReply.Term)
			}
		case <-timeoutChan:
			electionTimedOut = true
		}

		if electionTimedOut || votesReceived >= rf.getMajorityCount() {
			break
		}
	}

	if electionTimedOut {
		rf.LogInfo("Election Vote Period Timed Out")
		return
	}

	rf.LogDebug("Election Voting Stats - Required :", rf.getMajorityCount(), "Got :", votesReceived)
	// if we are still candidate and got required majority, transit to leader
	if rf.stable.GetTermManager().getCurrentState() == CANDIDATE && votesReceived >= rf.getMajorityCount() {
		rf.LogInfo("Received majority, Transitioning to", LEADER)
		rf.transitToNewRaftState(LEADER)
		return
	}
}

func (rf *Raft) waitForElectionTimeout(transitToCandidate bool) bool {
	termManager := rf.stable.GetTermManager()
	originalState := termManager.getCurrentState()
	for termManager.getCurrentState() == originalState {
		if rf.hasElectionTimeoutElapsed() {
			rf.LogWarn("Election timeout elapsed")
			if transitToCandidate {
				rf.transitToNewRaftState(CANDIDATE)
			}
			return true
		}
		time.Sleep(utils.GetRandomDurationInMs(5, 10))
		termManager = rf.stable.GetTermManager()
	}
	return false
}

// GetStatus returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetStatus() (int, bool) {
	currenttermManager := *rf.stable.GetTermManager()
	term := int(currenttermManager.getTerm())
	isleader := currenttermManager.getCurrentState() == LEADER
	return term, isleader
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.stable.GetTermManager().getCurrentState() {
		case LEADER:
			rf.maintainLeadership()
		case CANDIDATE:
			rf.beginElection()
		case FOLLOWER:
			rf.waitForElectionTimeout(true)
		}
	}
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
	CommandIndex int
	CommandValid bool
	Command      interface{}

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

		snapshotState := rf.stable.GetSnapshotManager()
		if snapshotState.Index >= lastAppliedIndex+1 {
			rf.LogInfo("Sending Snapshot for application to cover with peers. Fetched snapshot state till", snapshotState.Index)
			snapshotApplyMsg := ApplyMsg{SnapshotIndex: int(snapshotState.Index), SnapshotTerm: int(snapshotState.Term), Snapshot: snapshotState.Data, SnapshotValid: true}
			rf.applyCommandToSM(snapshotApplyMsg)
			rf.setLastAppliedIndex(snapshotState.Index)
		} else {
			logEntry := rf.stable.GetLogEntry(lastAppliedIndex + 1)
			if logEntry.LogIndex == lastAppliedIndex+1 {
				rf.LogInfo("Applying committed entry at index", lastAppliedIndex+1)
				logsApplyMsg := ApplyMsg{Command: logEntry.LogCommand, CommandIndex: int(logEntry.LogIndex), CommandValid: true}
				rf.applyCommandToSM(logsApplyMsg)
				rf.setLastAppliedIndex(lastAppliedIndex + 1)
			}
		}
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	termManager := *rf.stable.GetTermManager()
	index := -1
	term := termManager.getTerm()
	isLeader := termManager.getCurrentState() == LEADER

	if isLeader {
		appendedEntry := rf.stable.AppendEntry(command, term)
		rf.LogWarn("Appended command", command, "at index", appendedEntry.LogIndex)
		//rf.propageEntriesToPeers()
		rf.persist()
		index = int(appendedEntry.LogIndex)
		me := rf.getSelfPeerIndex()
		rf.matchIndex[me].Store(int32(index))
		rf.nextIndex[me].Store(int32(index))
	}

	return index, int(term), isLeader
}

func createStableState(term int32, voteManager VoteManager, log utils.Log, snapshotManager SnapshotManager) *StableStorage {
	stableStorage := &StableStorage{
		TermManager: atomic.Pointer[TermManager]{},
		VoteManager: atomic.Pointer[VoteManager]{},
		Snapshot:    atomic.Pointer[SnapshotManager]{},
	}

	stableStorage.TermManager.Store(&TermManager{State: FOLLOWER, Term: term, ElectionTimeout: utils.GetRandomElectionTimeoutPeriod()})
	stableStorage.VoteManager.Store(&voteManager)
	stableStorage.Snapshot.Store(&snapshotManager)

	stableStorage.Log = log
	stableStorage.TermVsFirstIdx = cmap.New[int32]()
	for _, entry := range log.LogArray {
		stableStorage.SetFirstOccuranceInTerm(entry.LogTerm, entry.LogIndex)
	}

	return stableStorage
}

// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.stable.GetTermManager().getTerm())
	if err != nil {
		rf.LogPanic("Failed to persist the current term", err)
	}

	err = e.Encode(rf.stable.GetVoteManager())
	if err != nil {
		rf.LogPanic("Failed to persist the current vote State", err)
	}

	err = rf.stable.EncodeLog(e)
	if err != nil {
		rf.LogPanic("Failed to persist the current Log", err)
	}

	raftstate := w.Bytes()
	snapshot := rf.stable.GetSnapshotManager().getData()

	rf.persister.Save(raftstate, snapshot)
	rf.LogDebug("Persisted current state into stable storage")
}

// restore previously persisted State.
func (rf *Raft) getOrCreateStableStorage(raftState []byte, snapshot []byte) {

	var term int32 = 0
	var voteManager = VoteManager{VotedFor: -1, LatestTermWhenVoted: 0}
	var log = utils.Log{LogArray: make([]utils.LogEntry, 1)}

	if raftState != nil && len(raftState) >= 1 {
		r := bytes.NewBuffer(raftState)
		d := labgob.NewDecoder(r)

		if err := d.Decode(&term); err != nil {
			panic(err)
		}

		if err := d.Decode(&voteManager); err != nil {
			panic(err)
		}

		if err := d.Decode(&log); err != nil {
			panic(err)
		}
	}

	// todo : fix this, this assumes that snapshot process was completed along with discarding log
	lastSnapshotEntry := utils.LogEntry{}
	if log.LogArray != nil && len(log.LogArray) > 0 {
		lastSnapshotEntry = log.LogArray[0]
	}
	var snapshotManager = SnapshotManager{Data: snapshot, Index: lastSnapshotEntry.LogIndex, Term: lastSnapshotEntry.LogTerm}
	rf.stable = createStableState(term, voteManager, log, snapshotManager)
	rf.LogWarn("Starting from stable State in term", term, "with voteManager being", voteManager, "and log of size", rf.stable.GetLastLogIndex())
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
		persister:          persister,
		applyCh:            applyCh,
		peers:              peers,
		me:                 me,
		dead:               0,
		lastHeartBeatEpoch: utils.GetCurrentTimeInMs(),
		nextIndex:          make([]atomic.Int32, len(peers)),
		matchIndex:         make([]atomic.Int32, len(peers)),
		commitIndex:        0,
		lastApplied:        0,
	}
	// instantiate logger
	rf.Logger = utils.GetLogger("raft_logLevel", rf.GetLoggerPrefix)

	// initialize from State persisted before a crash
	rf.getOrCreateStableStorage(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to send committed commands to State machine
	go rf.applyCommitted()

	return rf
}
