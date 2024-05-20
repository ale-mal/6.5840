package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 150

func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElection) >= rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	rf.lastElection = time.Now()
	rf.electionTimeout = time.Duration(baseElectionTimeout+rand.Int63()%baseElectionTimeout) * time.Millisecond
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	DPrintf(dVote, "S%d RequestVote: sending request to %v with term %v", rf.me, server, args.Term)
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		rf.handleRequestVoteReply(server, args, reply)
	}
}

func (rf *Raft) handleRequestVoteReply(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerTrackers[server].lastAck = time.Now()

	m := Message{From: server, Type: VoteReply, Term: reply.Term, ArgsTerm: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	if reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 {
			DPrintf(dLeader, "S%d RequestVote: got majority of votes, switching state from %v to %v", rf.me, rf.state, Leader)
			rf.becomeLeader()
		} else {
			DPrintf(dVote, "S%d RequestVote: got vote, but not majority yet, votes received %v", rf.me, rf.votesReceived)
		}
	} else {
		DPrintf(dVote, "S%d RequestVote: vote not granted", rf.me)
	}
}

func (rf *Raft) broadcastRequestVote() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// itself
			continue
		}
		args := &RequestVoteArgs{}
		args.Term = rf.persistentState.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.persistentState.log.lastIndex()
		args.LastLogTerm = rf.persistentState.log.term(args.LastLogIndex)
		go rf.sendRequestVote(i, args)
	}
}

func (rf *Raft) eligibleToGrantVote(candidateLastLogIndex, candidateLastLogTerm int) bool {
	lastLogIndex := rf.persistentState.log.lastIndex()
	lastLogTerm := rf.persistentState.log.term(lastLogIndex)
	if candidateLastLogTerm > lastLogTerm {
		return true
	} else if candidateLastLogTerm == lastLogTerm {
		return candidateLastLogIndex >= lastLogIndex
	}
	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.persistentState.currentTerm
	reply.VoteGranted = false

	m := Message{Type: Vote, From: args.CandidateId, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.persistentState.currentTerm
		defer rf.persist()
	}
	if !ok {
		return
	}

	if (rf.persistentState.votedFor == -1 || rf.persistentState.votedFor == args.CandidateId) && rf.eligibleToGrantVote(args.LastLogIndex, args.LastLogTerm) {
		rf.persistentState.votedFor = args.CandidateId
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
}
