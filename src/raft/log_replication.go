package raft

import "time"

func (rf *Raft) checkLogPrefixMatched(leaderPrevLogIndex, leaderPrevLogTerm int) Err {
	lastLogIndex := rf.persistentState.log.lastIndex()
	if leaderPrevLogIndex < 0 {
		if lastLogIndex >= 0 {
			// Reply false if log doesn’t contain an entry at prevLogIndex (§5.3)
			return IndexNotMatched
		}
	} else if leaderPrevLogIndex > lastLogIndex {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		return IndexNotMatched
	} else {
		logTerm := rf.persistentState.log.term(leaderPrevLogIndex)
		if logTerm != leaderPrevLogTerm {
			// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			return TermNotMatched
		}
	}
	return Matched
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.persistentState.currentTerm
	reply.Success = false

	m := Message{Type: Append, From: args.LeaderId, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.persistentState.currentTerm
		defer rf.persist()
	}
	if !ok {
		DPrintf(dLeader, "S%d AppendEntries: not ok", rf.me)
		return
	}

	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		DPrintf(dLeader, "S%d AppendEntries: not matched", rf.me)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+1+len(args.Entries); i++ {
		if i > rf.persistentState.log.lastIndex() {
			rf.persistentState.log.append(args.Entries[i-args.PrevLogIndex-1:])
			break
		}
		if rf.persistentState.log.term(i) != args.Entries[i-args.PrevLogIndex-1].Term {
			rf.persistentState.log.entries = rf.persistentState.log.entries[:i]
			rf.persistentState.log.append(args.Entries[i-args.PrevLogIndex-1:])
			break
		}
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	lastNewLogIndex := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit < lastNewLogIndex {
		lastNewLogIndex = args.LeaderCommit
	}
	DPrintf(dLeader, "S%d AppendEntries: maybeCommittedTo min(%v, %v)", rf.me, args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	rf.maybeCommittedTo(lastNewLogIndex)

	rf.lastHeartbeatTime = time.Now()
	rf.resetElectionTimer()

	reply.Success = true
}

func (rf *Raft) maybeCommittedTo(index int) {
	if index > rf.persistentState.log.commited {
		rf.persistentState.log.commitedTo(index)
		rf.claimToBeApplied.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	DPrintf(dLeader, "S%d AppendEntries: sending to %v, args %v", rf.me, server, args)
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.handleAppendEntriesReply(server, args, reply)
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerTrackers[server].lastAck = time.Now()

	m := Message{Type: AppendReply, From: server, Term: reply.Term, ArgsTerm: args.Term, PrevLogIndex: args.PrevLogIndex}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	if reply.Success {
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		rf.peerTrackers[server].nextIndex = args.PrevLogIndex + len(args.Entries) + 1
		rf.peerTrackers[server].matchIndex = args.PrevLogIndex + len(args.Entries)
		DPrintf(dLeader, "S%d AppendEntries: got success from %v, nextIndex %v, matchIndex %v", rf.me, server, rf.peerTrackers[server].nextIndex, rf.peerTrackers[server].matchIndex)

		if rf.maybeCommitMatched(rf.peerTrackers[server].matchIndex) {
			rf.broadcastAppendEntries(true)
		}
	} else {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		rf.peerTrackers[server].nextIndex--
		DPrintf(dLeader, "S%d AppendEntries: got fail from %v, nextIndex %v, matchIndex %v", rf.me, server, rf.peerTrackers[server].nextIndex, rf.peerTrackers[server].matchIndex)
	}
}

func (rf *Raft) quorumMatched(index int) bool {
	matched := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// itself
			continue
		}
		if rf.peerTrackers[i].matchIndex >= index {
			matched++
		}
	}
	return 2*matched > len(rf.peers)
}

func (rf *Raft) maybeCommitMatched(index int) bool {
	for i := index; i > rf.persistentState.log.commited; i-- {
		term := rf.persistentState.log.term(i)
		if term == rf.persistentState.currentTerm && rf.quorumMatched(i) {
			rf.persistentState.log.commitedTo(i)
			rf.claimToBeApplied.Signal()
			return true
		}
	}
	return false
}

func (rf *Raft) broadcastAppendEntries(forced bool) {
	if !forced {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// itself
			continue
		}
		nextIndex := rf.peerTrackers[i].nextIndex
		prevLogIndex := nextIndex - 1

		args := &AppendEntriesArgs{}
		args.Term = rf.persistentState.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.persistentState.log.term(prevLogIndex)
		args.Entries = rf.persistentState.log.slice(nextIndex, rf.persistentState.log.lastIndex()+1)
		args.LeaderCommit = rf.persistentState.log.commited
		go rf.sendAppendEntries(i, args)
	}
}
