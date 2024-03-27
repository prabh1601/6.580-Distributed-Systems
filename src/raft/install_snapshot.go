package raft

import (
	"6.5840/utils"
)

type InstallSnapshotArgs struct {
	Term              int32
	LeaderId          int
	LastIncludedIndex int32
	LastIncludedTerm  int32
	Data              []byte

	// Not implementing the snapshot in split fashion, sending all data in one rpc
	// offset            int32
	// done              bool
}

type InstallSnapshotReply struct {
	Term int32
}

func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.logInfo("Received Snapshot Install from :", args.LeaderId)
	rf.logDebug("args", *args, "reply", *reply)
	reply.Term = rf.stable.GetTermManager().getTerm()

	if reply.Term > args.Term {
		rf.logWarn("Rejecting Snapshot from", args.LeaderId, "Reason : stale term")
		return
	}

	if rf.processSnapshotInstall(SnapshotManager{Data: args.Data, Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}) {
		rf.transitToNewRaftStateWithTerm(FOLLOWER, args.Term)
	}
	if rf.stable.GetLogLength() == args.LastIncludedIndex {
		rf.stable.AppendEntry(nil, args.LastIncludedTerm)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	executionResult := utils.ExecuteRpcWithTimeout(func() bool {
		rf.logDebug("Install Snapshot - server:", server, "args:", *args)
		ok := rf.peers[server].Call("Raft.HandleInstallSnapshot", args, reply)
		if !ok {
			rf.logError("InstallSnapshot Rpc to", server, "failed")
		}
		return ok
	}, func() { rf.logError("InstallSnapshot Rpc to", server, "timed out") })

	return executionResult == utils.SUCCESS
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) bool {
	rf.logInfo("Received Client Snapshot Request till index", index)
	tillSnapshotEntry := rf.stable.GetLogEntry(int32(index))
	return rf.processSnapshotInstall(SnapshotManager{Data: snapshot, Index: tillSnapshotEntry.LogIndex, Term: tillSnapshotEntry.LogTerm})
}

func (rf *Raft) processSnapshotInstall(newSnapshotManager SnapshotManager) bool {
	rf.logInfo("Processing Snapshot till index", newSnapshotManager.Index)
	installed := false
	index := newSnapshotManager.Index

	for {
		startOffset := rf.stable.GetFirstIndex()
		if startOffset >= index {
			rf.logWarn("Trying to install stale snapshot containing prefix till index", index, "server is already snapshotted", startOffset)
			break
		}

		oldSnapshot := rf.stable.GetSnapshotManager()
		installed = rf.stable.StoreNewSnapshot(oldSnapshot, &newSnapshotManager)
		if installed {
			rf.stable.DiscardLogPrefix(index)
			rf.persist()
			rf.logInfo("Snapshotted entries till index", index)
			break
		}
	}

	return installed
}

func (rf *Raft) getInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.stable.GetTermManager().getTerm(),
		LeaderId:          rf.getSelfPeerIndex(),
		LastIncludedIndex: rf.stable.GetSnapshotManager().Index,
		LastIncludedTerm:  rf.stable.GetSnapshotManager().Term,
		Data:              rf.stable.GetSnapshotManager().getData(),
	}
}
