package rsm

type Err string

const (
	Ok          Err = "Ok"
	WrongLeader     = "WrongLeader"
	WrongGroup      = "WrongGroup"
)

type OpType int32

const (
	GET OpType = iota
	PUT
	APPEND
	JOIN
	LEAVE
	MOVE
	QUERY
	ACTIVATE_SHARD
	DEACTIVATE_SHARD
)

func (e OpType) String() string {
	switch e {
	case GET:
		return "Get"
	case PUT:
		return "Put"
	case APPEND:
		return "Append"
	case JOIN:
		return "Join"
	case LEAVE:
		return "Leave"
	case MOVE:
		return "Move"
	case QUERY:
		return "Query"
	case ACTIVATE_SHARD:
		return "Activate Shard"
	case DEACTIVATE_SHARD:
		return "Deactivate Shard"
	default:
		return "Invalid Operation"
	}
}

type OpState int32

const (
	STARTED OpState = iota
	ABORTED
	COMPLETED
)

func (e OpState) String() string {
	switch e {
	case STARTED:
		return "Started"
	case ABORTED:
		return "Aborted"
	case COMPLETED:
		return "Completed"
	default:
		return "Invalid State"
	}
}

type ShardStatus int32

const (
	NOT_SERVING ShardStatus = iota
	SERVING
	TO_RECIEVE
	TO_MOVE
)

func (s ShardStatus) String() string {
	switch s {
	case TO_RECIEVE:
		return "To Recieve"
	case TO_MOVE:
		return "To Move"
	case SERVING:
		return "Serving"
	case NOT_SERVING:
		return "Not Serving"
	default:
		return "Invalid State"
	}
}

var ShardStatusString = func() map[string]ShardStatus {
	m := make(map[string]ShardStatus)
	for i := NOT_SERVING; i <= TO_MOVE; i++ {
		m[i.String()] = i
	}
	return m
}()

// ------------- interfaces --------------------

type CommandProcessor[key Key, value any] interface {
	ProcessCommandInternal(command RaftCommand[key, value])
	PostSnapshotProcess()
}

type BaseArgs struct {
	Shard    int
	OpId     int64
	ClientId int64
	Op       OpType
}

func (args BaseArgs) GetShardNum() int {
	return args.Shard
}

func (args BaseArgs) GetOpId() int64 {
	return args.OpId
}

type BaseReply struct {
	Err Err
}

func (reply BaseReply) GetErr() Err {
	return reply.Err
}

type ServerArgs[key Key, value any] interface {
	ConvertToRaftCommand() RaftCommand[key, value]
	ToString() string
	GetOpId() int64
	GetShardNum() int
}

type ServerReply interface {
	GetErr() Err
	ToString() string
}

type RaftCommand[key Key, value any] struct {
	OpType   OpType
	ClientId int64
	OpId     int64
	Key      key
	Value    value
}
