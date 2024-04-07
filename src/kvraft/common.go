package kvraft

const (
	OK           = "OK"
	NO_KEY       = "NO_KEY"
	WRONG_LEADER = "WRONG_LEADER"
)

type OpType int32

const (
	GET OpType = iota
	PUT
	APPEND
)

func (e OpType) String() string {
	switch e {
	case GET:
		return "Get"
	case PUT:
		return "Put"
	case APPEND:
		return "Append"
	default:
		return "Invalid Operation"
	}
}

type Err string

// Put or Append
type PutAppendArgs struct {
	OpId     int64
	ClientId int64
	Key      string
	Value    string
	Op       OpType
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	LeaderId int
	Err      Err
}

type GetArgs struct {
	OpId     int64
	ClientId int64
	Key      string
}

type GetReply struct {
	LeaderId int
	Err      Err
	Value    string
}

type RaftCommand struct {
	OpType   OpType
	ClientId int64
	OpId     int64
	Key      string
	Value    string
}

type OpState int32

const (
	STARTED OpState = iota
	COMPLETED
)

type ClientRequest struct {
	clientId int64
	opId     int64
	index    int
}
