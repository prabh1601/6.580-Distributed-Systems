package raft

import "6.5840/utils"

type InstallSnapshotArgs struct {
	Term              int32
	leaderId          int32
	lastIncludedIndex int32
	lastIncludedTerm  int32
	data              []byte

	// Not implementing the snapshot in split fashion, sending all data in one rpc
	// offset            int32
	// done              bool
}

type InstallSnapshotReply struct {
	Term int32
}

func (rf *Raft) HandleInstallSnapshot() {

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
	println(len(snapshot))
	rf.LogWarn("Processing Snapshot Request from index", index)
	rf.stable.DiscardLogPrefix(int32(index))
	rf.stable.StoreNewSnapshot(snapshot)
	rf.persist()
}
