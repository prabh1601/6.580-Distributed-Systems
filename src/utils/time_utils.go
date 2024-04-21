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

func getRandomPeriod(a, b int) int64 {
	if a > b {
		b, a = a, b
	}

	period := int64(a)
	if a != b {
		extra := rand.Int63n(int64(b - a))
		period += extra
	}
	return period
}

func GetCurrentTimeInMs() int64 {
	return time.Now().UnixMilli()
}
