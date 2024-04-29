package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 150

// if the peer has not acked in this duration, it's considered inactive.
const activeWindowWidth = 2 * baseElectionTimeout * time.Millisecond

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
		if len(rf.persistentState.log) > 0 {
			args.LastLogIndex = len(rf.persistentState.log)
			args.LastLogTerm = rf.persistentState.log[args.LastLogIndex-1].Term
		} else {
			args.LastLogIndex = 0
			args.LastLogTerm = 0
		}
		go rf.sendRequestVote(i, args)
	}
}

func (rf *Raft) eligibleToGrantVote(candidateLastLogIndex, candidateLastLogTerm int) bool {
	if candidateLastLogIndex > 0 {
		if len(rf.persistentState.log) > 0 {
			if candidateLastLogIndex < len(rf.persistentState.log) {
				return false
			}
			if candidateLastLogIndex == len(rf.persistentState.log) && candidateLastLogTerm < rf.persistentState.log[candidateLastLogIndex-1].Term {
				return false
			}
		}
	} else {
		if len(rf.persistentState.log) > 0 {
			return false
		}
	}
	return true
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
