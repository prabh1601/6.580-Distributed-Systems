package raft

import (
	"log"
	"math/rand"
	"time"
)

const MIN_ELECTION_TIMEOUT = 200
const MAX_ELECTION_TIMEOUT = 350

func getRandomElectionTimeoutPeriod() int64 {
	return getRandomPeriod(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
}

func getRandomDurationInMs(a, b int) time.Duration {
	return time.Duration(getRandomPeriod(100, 120)) * time.Millisecond
}

type ExecutionResult int

const (
	SUCESS ExecutionResult = iota
	FAILED
	TIMED_OUT
)

func doExecutionWithTimeout(operation func() bool, timeoutHandler func()) ExecutionResult {

	executionCompleted := make(chan bool)
	go func() {
		executionCompleted <- operation()
	}()

	select {
	case result := <-executionCompleted:
		if result {
			return SUCESS
		} else {
			return FAILED
		}
	case <-time.After(getRandomDurationInMs(5, 7)):
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

func getCurrentTimeInMs() int64 {
	return time.Now().UnixMilli()
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
