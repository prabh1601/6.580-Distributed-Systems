package utils

import "sync"

type LogEntry struct {
	LogIndex   int32
	LogTerm    int32
	LogCommand interface{}
}

type ConcurrentLog struct {
	LogLock  sync.RWMutex
	LogIdx   int32
	LogArray []LogEntry
}

// todo : insert some abstraction
func (cl *ConcurrentLog) GetLogLength() int32 {
	cl.LogLock.RLock()
	defer cl.LogLock.RUnlock()

	return cl.LogIdx + 1
}

func (cl *ConcurrentLog) GetLastLogIndex() int32 {
	cl.LogLock.RLock()
	defer cl.LogLock.RUnlock()

	return cl.LogIdx
}

func (cl *ConcurrentLog) GetLastLogEntry() LogEntry {
	cl.LogLock.RLock()
	defer cl.LogLock.RUnlock()

	if cl.LogIdx == 0 {
		return LogEntry{LogIndex: 0, LogTerm: 0}
	}
	return cl.LogArray[cl.LogIdx]
}

func (cl *ConcurrentLog) GetLogEntry(logIndex int32) LogEntry {
	cl.LogLock.RLock()
	defer cl.LogLock.RUnlock()

	if logIndex == 0 {
		return LogEntry{LogIndex: 0, LogTerm: 0}
	}
	return cl.LogArray[logIndex]
}

// half-open range
func (cl *ConcurrentLog) GetLogEntries(startIdx int32, endIdx int32) []LogEntry {
	cl.LogLock.RLock()
	defer cl.LogLock.RUnlock()

	endIdx = min(int32(len(cl.LogArray)), endIdx)

	if startIdx >= endIdx {
		return nil
	} else {
		return cl.LogArray[startIdx:endIdx]
	}
}

func (cl *ConcurrentLog) AppendMultipleEntries(commitIndex int32, entries []LogEntry) {
	if entries == nil {
		return
	}

	cl.LogLock.Lock()
	defer cl.LogLock.Unlock()

	for _, entry := range entries {
		writeIdx := entry.LogIndex
		if writeIdx <= commitIndex {
			panic("Trying to overwrite committed entries")
		}

		cl.LogArray[writeIdx] = entry
		cl.LogIdx = writeIdx
	}
}

func (cl *ConcurrentLog) AppendEntry(command interface{}, term int32) LogEntry {
	cl.LogLock.Lock()
	defer cl.LogLock.Unlock()

	cl.LogIdx += 1
	entry := LogEntry{LogTerm: term, LogIndex: cl.LogIdx, LogCommand: command}
	cl.LogArray[cl.LogIdx] = entry
	return entry
}
