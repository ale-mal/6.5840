package raft

type LogEntry struct {
	Index int
	Term  int
	Data  interface{}
}

type Log struct {
	entries  []LogEntry
	commited int
	applied  int
}

func makeLog() Log {
	log := Log{
		entries:  []LogEntry{{Term: 0}},
		commited: 0,
		applied:  0,
	}
	return log
}

func (log *Log) lastIndex() int {
	return len(log.entries) - 1
}

func (log *Log) term(index int) int {
	if index < 0 || index >= len(log.entries) {
		return -1
	}
	return log.entries[index].Term
}

func (log *Log) clone(entries []LogEntry) []LogEntry {
	clone := make([]LogEntry, len(entries))
	copy(clone, entries)
	return clone
}

func (log *Log) slice(start, end int) []LogEntry {
	if start < 0 {
		start = 0
	}
	if end > len(log.entries) {
		end = len(log.entries)
	}
	if start >= end {
		return nil
	}
	return log.clone(log.entries[start:end])
}

func (log *Log) append(entries []LogEntry) {
	log.entries = append(log.entries, entries...)
}

func (log *Log) commitedTo(index int) {
	if index > log.commited {
		log.commited = index
	}
}

func (log *Log) appliedTo(index int) {
	if index > log.applied {
		log.applied = index
	}
}

func (log *Log) newCommittedEntries() []LogEntry {
	start := log.applied + 1
	end := log.commited + 1
	if start >= end {
		return nil
	}
	return log.clone(log.entries[start:end])
}
