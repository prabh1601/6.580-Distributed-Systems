package utils

import (
	"6.5840/labgob"
	"fmt"
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
	StartOffset int32
	LogArray    []LogEntry
}

type ConcurrentLog struct {
	TermVsFirstIdx cmap.ConcurrentMap[string, int32] // Concurrent map for storing
	logLock        sync.RWMutex
	Log
}

func (cl *ConcurrentLog) SetFirstOccuranceInTerm(term int32, idx int32) {
	cl.TermVsFirstIdx.SetIfAbsent(strconv.Itoa(int(term)), idx)
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

	return cl.logLength()
}

func (cl *ConcurrentLog) GetFirstOffsetedIndex() int32 {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.StartOffset
}

func (cl *ConcurrentLog) GetLastLogIndex() int32 {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.lastLogIndex()
}

func (cl *ConcurrentLog) GetLastLogEntry() LogEntry {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.LogArray[cl.getOffsetAdjustedIdx(cl.lastLogIndex())]
}

func (cl *ConcurrentLog) GetLogEntry(logIndex int32) LogEntry {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	return cl.LogArray[cl.getOffsetAdjustedIdx(logIndex)]
}

// half-open range
func (cl *ConcurrentLog) GetLogEntries(startIdx int32, endIdx int32) []LogEntry {
	cl.logLock.RLock()
	defer cl.logLock.RUnlock()

	startIdx = cl.getOffsetAdjustedIdx(startIdx)
	endIdx = cl.getOffsetAdjustedIdx(endIdx)

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
		offsetedWriteIdx := cl.getOffsetAdjustedIdx(writeIdx)
		if writeIdx <= commitIndex {
			if entry.LogTerm != cl.LogArray[offsetedWriteIdx].LogTerm {
				fmt.Println(entry, cl.LogArray[offsetedWriteIdx])
				panic("Trying to overwrite committed entries. Write idx : " + strconv.Itoa(int(writeIdx)) + " commitIndex : " + strconv.Itoa(int(commitIndex)))
			} else {
				// Log completeness property
				continue
			}
		}

		if writeIdx > cl.lastLogIndex() {
			PrintIfEnabled("debug_log", fmt.Sprint(writeIdx, len(cl.LogArray), int32(len(cl.LogArray)), entry, cl.LogArray))
			cl.LogArray = append(cl.LogArray, entry)
		} else {
			PrintIfEnabled("debug_log", fmt.Sprint(writeIdx, commitIndex, cl.LogArray[offsetedWriteIdx], entry))
			cl.LogArray[offsetedWriteIdx] = entry
		}

		// todo : fix bug here in case of overwrite -> This is not a bug ? it still works fine as we only overwrite with higher term values
		cl.SetFirstOccuranceInTerm(entry.LogTerm, entry.LogIndex)
	}
}

func (cl *ConcurrentLog) AppendEntry(command interface{}, term int32) LogEntry {
	cl.logLock.Lock()
	defer cl.logLock.Unlock()

	entry := LogEntry{LogTerm: term, LogCommand: command, LogIndex: cl.logLength()}
	cl.LogArray = append(cl.LogArray, entry)
	cl.SetFirstOccuranceInTerm(entry.LogTerm, entry.LogIndex)
	return entry
}

func (cl *ConcurrentLog) DiscardLogPrefix(startIdx int32) {
	newLogArray := make([]LogEntry, 0)
	newTermVsFirstOccurance := cmap.New[int32]()

	cl.logLock.Lock()
	defer cl.logLock.Unlock()

	for i := startIdx - 1; i < cl.logLength(); i++ {
		entry := cl.LogArray[cl.getOffsetAdjustedIdx(i)]
		newLogArray = append(newLogArray, entry)
		newTermVsFirstOccurance.SetIfAbsent(strconv.Itoa(int(entry.LogTerm)), i)
	}

	cl.Log = Log{
		LogArray:    newLogArray,
		StartOffset: startIdx - 1,
	}
	cl.TermVsFirstIdx = newTermVsFirstOccurance
}

// private methods
// use these methods in-case the original method already holds a lock

func (cl *ConcurrentLog) getOffsetAdjustedIdx(idx int32) int32 {
	return idx - cl.StartOffset
}

func (cl *ConcurrentLog) logLength() int32 {
	return cl.StartOffset + int32(len(cl.LogArray))
}

func (cl *ConcurrentLog) lastLogIndex() int32 {
	return cl.logLength() - 1
}
