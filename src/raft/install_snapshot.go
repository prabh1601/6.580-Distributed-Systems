package raft

import "6.5840/utils"

type InstallSnapshotArgs struct {
	Term              int32
	LeaderId          int
	LastIncludedIndex int32
	LastIncludedTerm  int32
	Data              []byte

	// Not implementing the snapshot in split fashion, sending all  in one rpc
	// offset            int32
	// done              bool
}

type InstallSnapshotReply struct {
	Term int32
}

func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.LogInfo("Received Snapshot Install from :", args.LeaderId)
	rf.LogDebug("args", *args, "reply", *reply)
	reply.Term = rf.stable.GetTermManager().GetTerm()

	rf.Snapshot(int(args.LastIncludedIndex+1), args.Data)
	if rf.stable.GetLastLogIndex() == args.LastIncludedIndex {
		rf.stable.AppendEntry(utils.LogEntry{LogTerm: args.LastIncludedTerm, LogIndex: args.LastIncludedIndex}, args.LastIncludedTerm)
	}
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	executionResult := utils.ExecuteRpcWithTimeout(func() bool {
		rf.LogDebug("Install Snapshot - server:", server, "args:", *args)
		ok := rf.peers[server].Call("Raft.HandleInstallSnapshot", args, reply)
		if !ok {
			rf.LogError("InstallSnapshot Rpc to", server, "failed")
		}
		return ok
	}, func() { rf.LogError("InstallSnapshot Rpc to", server, "timed out") })

	return executionResult == utils.SUCCESS
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.LogInfo("Processing Snapshot Request from index", index)
	for {
		oldSnapshot := rf.stable.GetCurrentSnapshot()
		startOffset := rf.stable.GetFirstOffsetedIndex()
		if startOffset > int32(index) {
			rf.LogWarn("Trying to Install State Snapshot containing prefix till index", index)
			return
		}

		if rf.stable.StoreNewSnapshot(oldSnapshot, &snapshot) {
			rf.stable.DiscardLogPrefix(int32(index))
			rf.persist()
			rf.LogInfo("Installed Snapshot and discarded log till index", index)
			break
		}
	}
}

func (rf *Raft) GetInstalLSnapshotArgs() *InstallSnapshotArgs {
	lastIncludedEntry := rf.stable.GetLogEntry(rf.stable.GetFirstOffsetedIndex())
	return &InstallSnapshotArgs{
		Term:              rf.stable.GetTermManager().GetTerm(),
		LeaderId:          rf.GetSelfPeerIndex(),
		LastIncludedIndex: lastIncludedEntry.LogIndex,
		LastIncludedTerm:  lastIncludedEntry.LogTerm,
		Data:              *rf.stable.GetCurrentSnapshot(),
	}
}
