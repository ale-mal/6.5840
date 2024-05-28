package raft

type LogEntry struct {
	Index int
	Term  int
	Data  interface{}
}

type Snapshot struct {
	Data  []byte
	Index int
	Term  int
}

type Log struct {
	snapshot           Snapshot
	hasPendingSnapshot bool

	entries  []LogEntry
	commited int
	applied  int
}

func makeLog() Log {
	log := Log{
		snapshot:           Snapshot{Data: nil, Index: 0, Term: 0},
		hasPendingSnapshot: false,
		entries:            []LogEntry{{Data: nil, Index: 0, Term: 0}},
		commited:           0,
		applied:            0,
	}
	return log
}

func (log *Log) toArrayIndex(index int) int {
	return index - log.firstIndex()
}

func (log *Log) firstIndex() int {
	return log.entries[0].Index
}

func (log *Log) lastIndex() int {
	return log.entries[len(log.entries)-1].Index
}

func (log *Log) term(index int) int {
	if index < log.firstIndex() || index > log.lastIndex() {
		return -1
	}
	return log.entries[log.toArrayIndex(index)].Term
}

func (log *Log) clone(entries []LogEntry) []LogEntry {
	clone := make([]LogEntry, len(entries))
	copy(clone, entries)
	return clone
}

func (log *Log) slice(start, end int) []LogEntry {
	if start >= end {
		return nil
	}
	start = log.toArrayIndex(start)
	end = log.toArrayIndex(end)
	return log.clone(log.entries[start:end])
}

func (log *Log) truncateSuffix(index int) {
	if index <= log.firstIndex() || index > log.lastIndex() {
		return
	}
	index = log.toArrayIndex(index)
	if index < len(log.entries) {
		log.entries = log.entries[:index]
	}
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
	start := log.toArrayIndex(log.applied + 1)
	end := log.toArrayIndex(log.commited + 1)
	if start >= end {
		return nil
	}
	return log.clone(log.entries[start:end])
}

func (log *Log) compactedTo(snapshot Snapshot) {
	suffix := make([]LogEntry, 0)
	if suffixStart := snapshot.Index + 1; suffixStart <= log.lastIndex() {
		suffixStart = log.toArrayIndex(suffixStart)
		suffix = log.entries[suffixStart:]
	}

	log.entries = append(make([]LogEntry, 1), suffix...)
	log.entries[0] = LogEntry{Index: snapshot.Index, Term: snapshot.Term}
	log.snapshot = snapshot

	log.commitedTo(log.snapshot.Index)
	log.appliedTo(log.snapshot.Index)
}

func (log *Log) clonedSnapshot() Snapshot {
	cloned := Snapshot{Data: make([]byte, len(log.snapshot.Data)), Index: log.snapshot.Index, Term: log.snapshot.Term}
	copy(cloned.Data, log.snapshot.Data)
	return cloned
}
