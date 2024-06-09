package rsm

import "sync/atomic"

type BaseClerk struct {
	ClientId    int64
	OpsExecuted int64
}

func (ck *BaseClerk) GetArgBase(opType OpType) ArgBase {
	return ArgBase{
		Op:       opType,
		ClientId: ck.ClientId,
		OpId:     ck.GetNextOperationId(),
	}
}

func (ck *BaseClerk) GetNextOperationId() int64 {
	return atomic.AddInt64(&ck.OpsExecuted, 1)
}
