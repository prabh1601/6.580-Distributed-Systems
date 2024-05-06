package utils

import (
	"6.5840/labgob"
	"strconv"
	"sync"
)

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
	TermVsFirstIdx map[int32]int32 // Concurrent map for storing
	logLock        sync.RWMutex
	Logger
	Log
}

func MakeLog(log Log, termVsFirstIdx map[int32]int32, peerIdx int) ConcurrentLog {
	cLog := ConcurrentLog{logLock: sync.RWMutex{}, TermVsFirstIdx: termVsFirstIdx, Log: log}
	cLog.Logger = GetLogger("raftLog_logLevel", func() string {
		return "[RAFT-LOG] [Peer : " + strconv.Itoa(peerIdx) + "] "
	})
	return cLog
}

func (cl *ConcurrentLog) SetFirstOccurrenceInTerm(term int32, idx int32) {
	cl.performWrite("setFirstOccurrenceInTerm", func() {
		cl.setFirstOccurrenceInTerm(term, idx)
	})
}

func (cl *ConcurrentLog) GetFirstLogIdxInTerm(term int32) int32 {
	var value int32
	cl.performRead("getFirstLogIdxInTerm", func() {
		value = cl.TermVsFirstIdx[term]
	})
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
		if offsetIndex < 0 || logIndex > cl.lastLogIndex() {
			if offsetIndex < 0 {
				cl.LogInfo("Trying to capture entry at", logIndex, "which is either discarded during snapshot or is less than 0")
			} else {
				cl.LogInfo("Trying to capture entry at", logIndex, " which is probably removed due to overwrite of entries by new leader")
			}
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

func (cl *ConcurrentLog) AppendMultipleEntries(commitIndex int32, entries []LogEntry) bool {
	if entries == nil || len(entries) == 0 {
		// it was heartbeat rpc
		return true
	}

	appendSuccess := false
	cl.performWrite("appendMultipleEntries", func() {
		entriesOverwritten := false
		for _, entry := range entries {
			writeIdx := entry.LogIndex
			offsetWriteIdx := cl.getOffsetAdjustedIdx(entry.LogIndex)
			if writeIdx < cl.StartOffset || (writeIdx <= cl.lastLogIndex() && entry.LogTerm == cl.LogArray[offsetWriteIdx].LogTerm) {
				// value is already snapshotted and discarded || log completeness property
				continue
			}

			if writeIdx <= commitIndex {
				cl.LogError("Commit Index :", commitIndex, "Trying to overwrite committed entries. Write idx:", writeIdx, "currently containing entry :", cl.LogArray[offsetWriteIdx], " and overwriting with :", entry)
				return
			}

			if writeIdx > cl.lastLogIndex() {
				cl.LogInfo("Append at", writeIdx, "with entry:", entry, "Current Array:", cl.LogArray)
				cl.LogArray = append(cl.LogArray, entry)
			} else {
				if cl.LogArray[offsetWriteIdx].LogTerm >= entry.LogTerm {
					cl.LogPanic("Wrong overwrite at:", writeIdx, "with entry:", entry, "over existing :", cl.LogArray[offsetWriteIdx])
				}
				entriesOverwritten = true
				cl.LogInfo("Overwrite at", writeIdx, "with entry:", entry, "commit Index:", commitIndex, "Current Entry:", cl.LogArray[offsetWriteIdx])
				cl.LogArray[offsetWriteIdx] = entry
			}

			appendSuccess = true
			cl.setFirstOccurrenceInTerm(entry.LogTerm, entry.LogIndex)
		}

		lastAppendIdx := entries[len(entries)-1].LogIndex
		if entriesOverwritten && cl.lastLogIndex() != lastAppendIdx {
			// remove additional wrong entries if any
			cl.LogArray = cl.LogArray[:cl.getOffsetAdjustedIdx(lastAppendIdx)]
			if len(cl.LogArray) == 0 {
				cl.LogPanic("Created an empty LogArray while discarding overwritten entries")
			}
		}
	})

	return appendSuccess
}

func (cl *ConcurrentLog) AppendEntry(command interface{}, term int32) LogEntry {
	var entry LogEntry
	cl.performWrite("appendEntry", func() {
		entry = cl.appendEntry(command, term)
	})
	return entry
}

func (cl *ConcurrentLog) DiscardLogPrefix(startIdx, startTerm int32) bool {
	newLogArray := make([]LogEntry, 0)
	newTermVsFirstOccurrence := make(map[int32]int32)
	discarded := false

	cl.performWrite("discardLogPrefix", func() {
		cl.LogInfo("startOffset:", cl.StartOffset, "startIdx:", startIdx)
		if startIdx <= cl.StartOffset {
			return
		}

		for i := startIdx; i < cl.logLength(); i++ {
			entry := cl.LogArray[cl.getOffsetAdjustedIdx(i)]
			newLogArray = append(newLogArray, entry)
			if newTermVsFirstOccurrence[entry.LogTerm] == 0 {
				newTermVsFirstOccurrence[entry.LogTerm] = i
			}
		}

		cl.TermVsFirstIdx = newTermVsFirstOccurrence
		cl.Log = Log{
			LogArray:    newLogArray,
			StartOffset: startIdx,
		}

		if len(newLogArray) == 0 {
			cl.appendEntry(nil, startTerm)
		}

		discarded = true
	})

	return discarded
}

// private methods
// use these methods in-case the original method already holds a lock
func (cl *ConcurrentLog) appendEntry(command interface{}, term int32) LogEntry {
	entry := LogEntry{LogTerm: term, LogCommand: command, LogIndex: cl.logLength()}
	cl.LogArray = append(cl.LogArray, entry)
	cl.setFirstOccurrenceInTerm(entry.LogTerm, entry.LogIndex)
	return entry
}

func (cl *ConcurrentLog) setFirstOccurrenceInTerm(term int32, idx int32) {
	if cl.TermVsFirstIdx[term] == 0 {
		cl.TermVsFirstIdx[term] = idx
	}
}

func (cl *ConcurrentLog) getFirstIndex() int32 {
	return cl.StartOffset
}

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
	opId := int(Nrand())
	cl.LogDebug("Op:", opId, "Trying to acquire write", label, ".Time :", GetCurrentTimeInMs())
	cl.logLock.Lock()
	cl.LogDebug("Op:", opId, "Acquired Write", label, ".Time :", GetCurrentTimeInMs())
	operation()
	cl.logLock.Unlock()
	cl.LogDebug("Op:", opId, "Finished Write", label, ".Time :", GetCurrentTimeInMs())
}

func (cl *ConcurrentLog) performRead(label string, operation func()) {
	opId := int(Nrand())
	cl.LogDebug("Op:", opId, "Trying to acquire read", label, ".Time :", GetCurrentTimeInMs())
	cl.logLock.RLock()
	cl.LogDebug("Op:", opId, "Acquired Read", label, ".Time :", GetCurrentTimeInMs())
	operation()
	cl.logLock.RUnlock()
	cl.LogDebug("Op:", opId, "Finished Read", label, ".Time :", GetCurrentTimeInMs())
}
