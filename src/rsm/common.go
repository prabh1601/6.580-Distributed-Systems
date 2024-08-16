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

type ShardState int32

const (
	NOT_SERVING ShardState = iota
	SERVING
	TO_RECIEVE
	TO_MOVE
)

func (s ShardState) String() string {
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

// ------------- interfaces --------------------

type CommandProcessor[key Key] interface {
	ProcessCommandInternal(command RaftCommand[key])
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

type ServerArgs[key Key] interface {
	ConvertToRaftCommand() RaftCommand[key]
	ToString() string
	GetOpId() int64
	GetShardNum() int
}

type ServerReply interface {
	GetErr() Err
	ToString() string
}

type RaftCommand[key Key] struct {
	OpType   OpType
	ClientId int64
	OpId     int64
	Key      key
	Value    interface{}
}
