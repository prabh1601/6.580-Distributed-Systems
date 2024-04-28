package kvraft

import "fmt"

type Err string

const (
	OK           Err = "OK"
	NO_KEY           = "NO_KEY"
	WRONG_LEADER     = "WRONG_LEADER"
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

type ServerArgs interface {
	ToString() string
	GetOpId() int64
}

type ServerReply interface {
	GetLeaderId() int
	GetErr() Err
	ToString() string
}

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

func (args PutAppendArgs) GetOpId() int64 {
	return args.OpId
}

func (args PutAppendArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

type PutAppendReply struct {
	LeaderId int
	Err      Err
}

func (reply PutAppendReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
}

func (reply PutAppendReply) GetLeaderId() int {
	return reply.LeaderId
}

func (reply PutAppendReply) GetErr() Err {
	return reply.Err
}

type GetArgs struct {
	OpId     int64
	ClientId int64
	Key      string
}

func (args GetArgs) GetOpId() int64 {
	return args.OpId
}

func (args GetArgs) ToString() string {
	return fmt.Sprintf("%+v", args)
}

type GetReply struct {
	LeaderId int
	Err      Err
	Value    string
}

func (reply GetReply) GetLeaderId() int {
	return reply.LeaderId
}

func (reply GetReply) GetErr() Err {
	return reply.Err
}

func (reply GetReply) ToString() string {
	return fmt.Sprintf("%+v", reply)
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
	ABORTED
	COMPLETED
)

type ClientRequest struct {
	clientId int64
	opId     int64
	index    int
}
