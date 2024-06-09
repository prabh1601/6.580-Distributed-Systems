package rsm

type Err string

const (
	OK          Err = "OK"
	WrongLeader     = "WRONG_LEADER"
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
	default:
		return "Invalid Operation"
	}
}

type ArgBase struct {
	OpId     int64
	ClientId int64
	Op       OpType
}

type ReplyBase struct {
	LeaderId int
	Err      Err
}

type ServerArgs[key Key, value any] interface {
	ConvertToRaftCommand() RaftCommand[key, value]
	ToString() string
	GetOpId() int64
}

type ServerReply interface {
	GetLeaderId() int
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

type CommandProcessor[key Key, value any] interface {
	ProcessCommandInternal(command RaftCommand[key, value])
}
