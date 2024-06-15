package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"6.5840/rsm"
)

type Clerk struct {
	rsm.BaseClerk[int, NewConfigData]
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.BaseClerk = rsm.MakeBaseClerk[int, NewConfigData]("ShardCtrler", servers)
	return ck
}

func (ck *Clerk) getQueryArgs(num int) *QueryArgs {
	return &QueryArgs{
		BaseArgs: ck.GetArgBase(rsm.QUERY),
		Num:      num,
	}
}

func (ck *Clerk) Query(num int) Config {
	args := ck.getQueryArgs(num)
	reply := &QueryReply{}
	ck.SendRequest(args, reply, "Query")
	return reply.Config
}

func (ck *Clerk) getJoinArgs(servers map[int][]string) *JoinArgs {
	return &JoinArgs{
		BaseArgs: ck.GetArgBase(rsm.JOIN),
		Servers:  servers,
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := ck.getJoinArgs(servers)
	reply := &JoinReply{}
	ck.SendRequest(args, reply, "Join")
}

func (ck *Clerk) getLeaveArgs(gids []int) *LeaveArgs {
	return &LeaveArgs{
		BaseArgs: ck.GetArgBase(rsm.LEAVE),
		GIDs:     gids,
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := ck.getLeaveArgs(gids)
	reply := &LeaveReply{}
	ck.SendRequest(args, reply, "Leave")
}

func (ck *Clerk) getMoveArgs(shard, gid int) *MoveArgs {
	return &MoveArgs{
		BaseArgs: ck.GetArgBase(rsm.JOIN),
		Shard:    shard,
		GID:      gid,
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := ck.getMoveArgs(shard, gid)
	reply := &MoveReply{}
	ck.SendRequest(args, reply, "Move")
}
