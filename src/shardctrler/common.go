package shardctrler

import (
	"6.5840/rsm"
	"fmt"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type NewConfigData struct {
	NewJoiningGroups       map[int][]string
	LeavingGroups          []int
	NewShardToGroupMapping map[int]int
}

type JoinArgs struct {
	rsm.BaseArgs
	Servers map[int][]string // new GID -> servers mappings
}

func (args JoinArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

func (args JoinArgs) ConvertToRaftCommand() rsm.RaftCommand[int, NewConfigData] {
	return rsm.RaftCommand[int, NewConfigData]{
		OpType:   args.Op,
		ClientId: args.ClientId,
		OpId:     args.OpId,
		Value:    NewConfigData{NewJoiningGroups: args.Servers},
	}
}

type JoinReply struct {
	rsm.BaseReply
}

func (reply JoinReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}

type LeaveArgs struct {
	rsm.BaseArgs
	GIDs []int
}

func (args LeaveArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

func (args LeaveArgs) ConvertToRaftCommand() rsm.RaftCommand[int, NewConfigData] {
	return rsm.RaftCommand[int, NewConfigData]{
		OpType:   args.Op,
		ClientId: args.ClientId,
		OpId:     args.OpId,
		Value:    NewConfigData{LeavingGroups: args.GIDs},
	}
}

type LeaveReply struct {
	rsm.BaseReply
}

func (reply LeaveReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}

type MoveArgs struct {
	rsm.BaseArgs
	Shard int
	GID   int
}

func (args MoveArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

func (args MoveArgs) ConvertToRaftCommand() rsm.RaftCommand[int, NewConfigData] {
	newShardVsGroupMapping := make(map[int]int)
	newShardVsGroupMapping[args.Shard] = args.GID

	return rsm.RaftCommand[int, NewConfigData]{
		OpType:   args.Op,
		ClientId: args.ClientId,
		OpId:     args.OpId,
		Value:    NewConfigData{NewShardToGroupMapping: newShardVsGroupMapping},
	}
}

type MoveReply struct {
	rsm.BaseReply
}

func (reply MoveReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}

type QueryArgs struct {
	rsm.BaseArgs
	Num int // desired config number
}

func (args QueryArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

func (args QueryArgs) ConvertToRaftCommand() rsm.RaftCommand[int, NewConfigData] {
	return rsm.RaftCommand[int, NewConfigData]{
		OpType:   args.Op,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
}

type QueryReply struct {
	rsm.BaseReply
	Config Config
}

func (reply QueryReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}
