package shardctrler

import (
	"6.5840/labgob"
	"6.5840/raft"
	"6.5840/rsm"
	"6.5840/utils"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)
import "6.5840/labrpc"

type ShardCtrler struct {
	me           int
	dead         int32
	configNumber atomic.Int64 // current ongoing config number,
	rf           *raft.Raft
	*rsm.ReplicatedStateMachine[int, Config, NewConfigData]
	utils.Logger
}

func (sc *ShardCtrler) getNewConfig(num int, shardMapping [NShards]int, newGroups map[int][]string) Config {
	return Config{
		Num:    num,
		Shards: shardMapping,
		Groups: newGroups,
	}
}

func (sc *ShardCtrler) getEmptyConfig() Config {
	return sc.getNewConfig(0, *new([NShards]int), make(map[int][]string))
}

func (sc *ShardCtrler) getConfig(configNum int) Config {
	if configNum == -1 {
		configNum = int(sc.configNumber.Load())
	}
	return sc.GetStore().GetValue(configNum)
}

// PostSnapshotProcess this is not atomic operation along with install of snapshot -> eventual consistency
// todo : check if we can afford this
func (sc *ShardCtrler) PostSnapshotProcess() {
	kvStore := sc.GetStore().GetKvStore()
	maxConfigNum := 0
	kvStore.ForEach(func(i int, c Config) bool {
		maxConfigNum = max(maxConfigNum, i)
		return true
	})
	sc.configNumber.Store(int64(maxConfigNum))
}

func (sc *ShardCtrler) ProcessCommandInternal(command rsm.RaftCommand[int, NewConfigData]) {
	switch command.OpType {
	case rsm.QUERY:
	// do-nothing
	default:
		newConfigData := command.Value

		// create empty config
		newConfig := sc.getEmptyConfig()

		// get curr Config
		curConfigNum := int(sc.configNumber.Load())
		curConfig := sc.GetStore().GetValue(curConfigNum)

		// copy existing groups
		for gid, group := range curConfig.Groups {
			newConfig.Groups[gid] = group
		}

		//copy existing shard mapping
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = curConfig.Shards[i]
		}

		// remove required groups
		for _, gid := range newConfigData.LeavingGroups {
			delete(newConfig.Groups, gid)
		}

		// add new groups
		for gid, group := range newConfigData.NewJoiningGroups {
			if existing, ok := newConfig.Groups[gid]; !ok {
				newConfig.Groups[gid] = group
			} else {
				sc.LogError("Dropping Config Change. Trying to add a new group over existing gid", gid, "Existing :", existing, "New :", group)
				return
			}
		}

		curGroupCount := len(curConfig.Groups)
		newGroupCount := len(newConfig.Groups)

		// rebalance shards if required
		if curGroupCount != newGroupCount {
			if newGroupCount == 0 {
				for i := 0; i < NShards; i++ {
					newConfig.Shards[i] = 0
				}
			} else {
				newGids := make([]int, 0)
				for gid, _ := range newConfig.Groups {
					newGids = append(newGids, gid)
				}

				sort.Slice(newGids, func(i, j int) bool { return newGids[i] < newGids[j] })

				for i := 0; i < NShards; i++ {
					newConfig.Shards[i] = newGids[(i % len(newConfig.Groups))]
				}
			}
			sc.LogDebug("Rebalanced shards. New State :", newConfig.Shards)
		}

		// move asked shards
		for shard, gid := range newConfigData.NewShardToGroupMapping {
			newConfig.Shards[shard] = gid
		}

		newConfig.Num = curConfigNum + 1
		sc.LogInfo("Switching to new configuration:", newConfig)

		sc.GetStore().SetValue(int(sc.configNumber.Add(1)), newConfig)
	}
}

func (sc *ShardCtrler) HandleJoin(args *JoinArgs, reply *JoinReply) {
	_, err := sc.StartQuorum(args.ConvertToRaftCommand())
	reply.Err = err
}

func (sc *ShardCtrler) HandleLeave(args *LeaveArgs, reply *LeaveReply) {
	_, err := sc.StartQuorum(args.ConvertToRaftCommand())
	reply.Err = err
}

func (sc *ShardCtrler) HandleMove(args *MoveArgs, reply *MoveReply) {
	_, err := sc.StartQuorum(args.ConvertToRaftCommand())
	reply.Err = err
}

func (sc *ShardCtrler) HandleQuery(args *QueryArgs, reply *QueryReply) {
	ok, err := sc.StartQuorum(args.ConvertToRaftCommand())
	reply.Err = err
	if ok {
		reply.Config = sc.getConfig(args.Num)
	}
}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.ReplicatedStateMachine.Kill()
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// 'me' is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	serverName := "shardctrler"
	labgob.Register(NewConfigData{})
	sc := new(ShardCtrler)
	sc.me = me

	sc.rf = raft.Make(serverName, servers, me, persister, make(chan raft.ApplyMsg))
	sc.Logger = utils.GetLogger(serverName, func() string {
		return "[" + strings.ToUpper(serverName) + "] [Peer : " + strconv.Itoa(me) + "]"
	})

	sc.ReplicatedStateMachine = rsm.StartReplicatedStateMachine[int, Config, NewConfigData](serverName, me, -1, sc.rf, sc)
	sc.GetStore().SetValue(0, sc.getEmptyConfig())
	return sc
}
