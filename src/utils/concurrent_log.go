package utils

import (
	"6.5840/labgob"
	cmap "github.com/orcaman/concurrent-map/v2"
	"strconv"
	"sync"
)

// todo : insert some abstraction

type LogEntry struct {
	LogIndex   int32
	LogTerm    int32
	LogCommand interface{}
}

type Log struct {
	LogIdx   int32
	LogArray []LogEntry
}

type ConcurrentLog struct {
	TermVsFirstIdx cmap.ConcurrentMap[string, int32] // Concurrent map for storing
	logLock        sync.RWMutex
	Log
}

func (cl *ConcurrentLog) GetFirstLogIdxInTerm(term int32) int32 {
	value, ok := cl.TermVsFirstIdx.Get(strconv.Itoa(int(term)))
	if !ok {
		value = 0
	}

	return value
}

func (cl *ConcurrentLog) EncodeLog(encoder *labgob.LabEncoder) error {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return encoder.Encode(cl.Log)
}

func (cl *ConcurrentLog) GetLogLength() int32 {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.LogIdx + 1
}

func (cl *ConcurrentLog) GetLastLogIndex() int32 {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.LogIdx
}

func (cl *ConcurrentLog) GetLastLogEntry() LogEntry {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.LogArray[cl.LogIdx]
}

func (cl *ConcurrentLog) GetLogEntry(logIndex int32) LogEntry {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.LogArray[logIndex]
}

// half-open range
func (cl *ConcurrentLog) GetLogEntries(startIdx int32, endIdx int32) []LogEntry {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	endIdx = min(int32(len(cl.LogArray)), endIdx)

	if startIdx >= endIdx {
		return nil
	} else {
		// todo : race condition here, not sure
		return cl.LogArray[startIdx:endIdx]
	}
}

func (cl *ConcurrentLog) AppendMultipleEntries(commitIndex int32, entries []LogEntry) {
	if entries == nil {
		return
	}

	cl.logLock.Lock()
	defer cl.logLock.Unlock()

	for _, entry := range entries {
		writeIdx := entry.LogIndex
		if writeIdx <= commitIndex {
			if entry.LogTerm != cl.LogArray[writeIdx].LogTerm {
				panic("Trying to overwrite committed entries. Write idx : " + strconv.Itoa(int(writeIdx)) + " commitIndex : " + strconv.Itoa(int(commitIndex)))
			} else {
				// Log completeness property
				continue
			}
		}

		cl.LogArray[writeIdx] = entry
		cl.LogIdx = writeIdx
		cl.SetFirstIdxInTermIfAbsent(entry.LogTerm, entry.LogIndex)
	}
}

func (cl *ConcurrentLog) AppendEntry(command interface{}, term int32) LogEntry {
	cl.logLock.Lock()
	defer cl.logLock.Unlock()

	cl.LogIdx += 1
	entry := LogEntry{LogTerm: term, LogIndex: cl.LogIdx, LogCommand: command}
	cl.LogArray[cl.LogIdx] = entry
	cl.SetFirstIdxInTermIfAbsent(entry.LogTerm, entry.LogIndex)
	return entry
}

func (cl *ConcurrentLog) SetFirstIdxInTermIfAbsent(term int32, idx int32) {
	cl.TermVsFirstIdx.SetIfAbsent(strconv.Itoa(int(term)), idx)
}
