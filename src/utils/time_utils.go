package utils

import (
	"math/rand"
	"time"
)

func GetRandomElectionTimeoutPeriod() int64 {
	return getRandomPeriod(MIN_ELECTION_TIMEOUT_MS, MAX_ELECTION_TIMEOUT_MS)
}

func GetRandomDurationInMs(a, b int) time.Duration {
	return time.Duration(getRandomPeriod(a, b)) * time.Millisecond
}

type ExecutionResult int

const (
	SUCCESS ExecutionResult = iota
	FAILED
	TIMED_OUT
)

func ExecuteRpcWithTimeout(operation func() bool, timeoutHandler func()) ExecutionResult {
	PrintIfEnabled("log_rpc", "Executing rpc")
	executionCompleted := make(chan bool)
	go func() {
		executionCompleted <- operation()
	}()

	select {
	case result := <-executionCompleted:
		if result {
			return SUCCESS
		} else {
			return FAILED
		}
	case <-time.After(RPC_TIMEOUT_MS * time.Millisecond):
		timeoutHandler()
		return TIMED_OUT
	}
}

func getRandomPeriod(a, b int) int64 {
	if a > b {
		b, a = a, b
	}

	timeout := int64(a)
	if a != b {
		extra := rand.Int63() % int64(b-a)
		timeout += extra
	}
	return timeout
}

func GetCurrentTimeInMs() int64 {
	return time.Now().UnixMilli()
}
