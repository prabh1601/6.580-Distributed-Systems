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
	var err error
	cl.performRead("encodeLog", func() {
		if cl.logLength() == 0 {
			err = nil
		} else {
			err = encoder.Encode(cl.Log)
		}
	})
	return err
}

func (cl *ConcurrentLog) GetLogLength() int32 {
	var length int32
	cl.performRead("getLogLength", func() {
		length = cl.logLength()
	})
	return length
}

func (cl *ConcurrentLog) GetFirstIndex() int32 {
	var offset int32
	cl.performRead("getFirstIndex", func() {
		offset = cl.StartOffset
	})
	return offset
}

func (cl *ConcurrentLog) GetLastLogIndex() int32 {
	var lastLogIndex int32
	cl.performRead("getLastLogIndex", func() {
		lastLogIndex = cl.lastLogIndex()
	})
	return lastLogIndex
}

func (cl *ConcurrentLog) GetLastLogEntry() LogEntry {
	var entry LogEntry
	cl.performRead("getLastLogEntry", func() {
		entry = cl.LogArray[cl.getOffsetAdjustedIdx(cl.lastLogIndex())]
	})
	return entry
}

func (cl *ConcurrentLog) GetLogEntry(logIndex int32) LogEntry {
	var entry LogEntry
	cl.performRead("getLogEntry", func() {
		offsetIndex := cl.getOffsetAdjustedIdx(logIndex)
		if offsetIndex < 0 {
			PrintIfEnabled("debug_log", "Trying to capture entry at "+strconv.Itoa(int(logIndex))+" which is either discarded during snapshot or is less than 0")
			entry = LogEntry{}
		} else {
			entry = cl.LogArray[offsetIndex]
		}

	})
	return entry
}

// half-open range
func (cl *ConcurrentLog) GetLogEntries(startIdx int32, endIdx int32) []LogEntry {
	var entries []LogEntry
	cl.performRead("getLogEntries", func() {
		startIdx = cl.getOffsetAdjustedIdx(startIdx)
		endIdx = cl.getOffsetAdjustedIdx(endIdx)
		if startIdx <= 0 || startIdx >= endIdx {
			entries = nil
		} else {
			// todo : race condition here if this is passed to some other, as slice is a pointer to underlying array
			entries = cl.LogArray[startIdx:endIdx]
		}
	})
	return entries
}

func (cl *ConcurrentLog) AppendMultipleEntries(commitIndex int32, entries []LogEntry) {
	if entries == nil {
		return
	}
	cl.performWrite("appendMultipleEntries", func() {
		for _, entry := range entries {
			writeIdx := entry.LogIndex
			if writeIdx < cl.StartOffset {
				// this means this value is already snapshotted and discarded
				continue
			}

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

			cl.SetFirstOccuranceInTerm(entry.LogTerm, entry.LogIndex)
		}
	})
}

func (cl *ConcurrentLog) AppendEntry(command interface{}, term int32) LogEntry {
	var entry LogEntry
	cl.performWrite("appendEntry", func() {
		entry = LogEntry{LogTerm: term, LogCommand: command, LogIndex: cl.logLength()}
		cl.LogArray = append(cl.LogArray, entry)
		cl.SetFirstOccuranceInTerm(entry.LogTerm, entry.LogIndex)
	})
	return entry
}

func (cl *ConcurrentLog) DiscardLogPrefix(startIdx int32) {
	newLogArray := make([]LogEntry, 0)
	newTermVsFirstOccurance := cmap.New[int32]()
	cl.performWrite("discardLogPrefix", func() {
		for i := startIdx; i < cl.logLength(); i++ {
			entry := cl.LogArray[cl.getOffsetAdjustedIdx(i)]
			newLogArray = append(newLogArray, entry)
			newTermVsFirstOccurance.SetIfAbsent(strconv.Itoa(int(entry.LogTerm)), i)
		}

		cl.Log = Log{
			LogArray:    newLogArray,
			StartOffset: startIdx,
		}
		cl.TermVsFirstIdx = newTermVsFirstOccurance
	})
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

func (cl *ConcurrentLog) performWrite(label string, operation func()) {
	PrintIfEnabled("debug_log", "Trying to acquire write "+label)
	cl.logLock.Lock()
	PrintIfEnabled("debug_log", "Acquired write "+label)
	operation()
	cl.logLock.Unlock()
	PrintIfEnabled("debug_log", "Finished write "+label)
}

func (cl *ConcurrentLog) performRead(label string, operation func()) {
	PrintIfEnabled("debug_log", "Trying to acquire read "+label)
	cl.logLock.RLock()
	PrintIfEnabled("debug_log", "Acquired read "+label)
	operation()
	cl.logLock.RUnlock()
	PrintIfEnabled("debug_log", "Finished read "+label)
}
