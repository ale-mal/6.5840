package raft

import "time"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type Err int

const (
	Rejected Err = iota
	Matched
	IndexNotMatched
	TermNotMatched
)

type AppendEntriesReply struct {
	Term    int
	Success bool
	Err     Err
}

type MessageType string

const (
	Vote        MessageType = "RequestVote"
	VoteReply   MessageType = "RequestVoteReply"
	Append      MessageType = "AppendEntries"
	AppendReply MessageType = "AppendEntriesReply"
)

type Message struct {
	Type         MessageType
	From         int
	Term         int
	ArgsTerm     int
	PrevLogIndex int
}

// return (termIsStale, termChanged)
func (rf *Raft) checkTerm(m Message) (bool, bool) {
	if m.Term < rf.persistentState.currentTerm {
		return false, false
	}
	// step down if received a more up-to-date message or received a message from the current leader.
	if m.Term > rf.persistentState.currentTerm || (m.Type == Append) {
		termChanged := rf.becomeFollower(m.Term)
		return true, termChanged
	}
	return true, false
}

// return true if the raft peer is eligible to handle the message.
func (rf *Raft) checkState(m Message) bool {
	eligible := false

	switch m.Type {
	case Vote:
		fallthrough
	case Append:
		eligible = rf.state == Follower
	case VoteReply:
		eligible = rf.state == Candidate && rf.persistentState.currentTerm == m.ArgsTerm
	case AppendReply:
		eligible = rf.state == Leader && rf.persistentState.currentTerm == m.ArgsTerm
	}

	if rf.state == Follower && (m.Type == Append) {
		rf.resetElectionTimer()
	}

	return eligible
}

func (rf *Raft) checkMessage(m Message) (bool, bool) {
	if m.Type == VoteReply || m.Type == AppendReply {
		rf.peerTrackers[m.From].lastAck = time.Now()
	}

	ok, termChanged := rf.checkTerm(m)
	if !ok || !rf.checkState(m) {
		return false, termChanged
	}
	return true, termChanged
}
