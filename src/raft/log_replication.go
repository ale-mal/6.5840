package raft

import "time"

func (rf *Raft) checkLogPrefixMatched(leaderPrevLogIndex, leaderPrevLogTerm int) Err {
	lastLogIndex := rf.persistentState.log.lastIndex()
	if leaderPrevLogIndex < 0 || leaderPrevLogIndex > lastLogIndex {
		// Reply false if log doesn’t contain an entry at prevLogIndex (§5.3)
		return IndexNotMatched
	}
	prevLogTerm := rf.persistentState.log.term(leaderPrevLogIndex)
	if prevLogTerm != leaderPrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		return TermNotMatched
	}
	return Matched
}

func (rf *Raft) findFirstConflict(index int) (int, int) {
	conflictTerm := rf.persistentState.log.term(index)
	firstConflictIndex := index
	for i := index - 1; i >= 0; i-- {
		if term := rf.persistentState.log.term(i); term != conflictTerm {
			break
		}
		firstConflictIndex = i
	}
	return conflictTerm, firstConflictIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.persistentState.currentTerm
	reply.Err = Rejected

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
		if reply.Err == IndexNotMatched {
			reply.LastLogIndex = rf.persistentState.log.lastIndex()
		} else {
			reply.ConflictTerm, reply.FirstConflictIndex = rf.findFirstConflict(args.PrevLogIndex)
		}
		DPrintf(dLeader, "S%d AppendEntries: not matched", rf.me)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+1+len(args.Entries); i++ {
		if i > rf.persistentState.log.lastIndex() {
			rf.persistentState.log.append(args.Entries[i-args.PrevLogIndex-1:])
			if !termChanged {
				rf.persist()
			}
			break
		}
		if rf.persistentState.log.term(i) != args.Entries[i-args.PrevLogIndex-1].Term {
			rf.persistentState.log.entries = rf.persistentState.log.entries[:i]
			rf.persistentState.log.append(args.Entries[i-args.PrevLogIndex-1:])
			if !termChanged {
				rf.persist()
			}
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

	switch reply.Err {
	case Rejected:
		// Do nothing

	case Matched:
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		rf.peerTrackers[server].nextIndex = args.PrevLogIndex + len(args.Entries) + 1
		rf.peerTrackers[server].matchIndex = args.PrevLogIndex + len(args.Entries)
		DPrintf(dLeader, "S%d AppendEntries: got success from %v, nextIndex %v, matchIndex %v", rf.me, server, rf.peerTrackers[server].nextIndex, rf.peerTrackers[server].matchIndex)

		if rf.maybeCommitMatched(rf.peerTrackers[server].matchIndex) {
			rf.broadcastAppendEntries(true)
		}

	case IndexNotMatched:
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		rf.peerTrackers[server].nextIndex--
		if reply.LastLogIndex < rf.persistentState.log.lastIndex() {
			rf.peerTrackers[server].nextIndex = reply.LastLogIndex + 1
		} else {
			rf.peerTrackers[server].nextIndex = rf.persistentState.log.lastIndex() + 1
		}

		rf.broadcastAppendEntries(true)

		DPrintf(dLeader, "S%d AppendEntries: got IndexNotMatched from %v, nextIndex %v, matchIndex %v", rf.me, server, rf.peerTrackers[server].nextIndex, rf.peerTrackers[server].matchIndex)

	case TermNotMatched:
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		rf.peerTrackers[server].nextIndex--
		newNextIndex := reply.FirstConflictIndex
		for i := rf.persistentState.log.lastIndex(); i >= 0; i-- {
			if term := rf.persistentState.log.term(i); term == reply.ConflictTerm {
				newNextIndex = i
				break
			}
		}

		rf.peerTrackers[server].nextIndex = newNextIndex

		rf.broadcastAppendEntries(true)

		DPrintf(dLeader, "S%d AppendEntries: got TermNotMatched from %v, nextIndex %v, matchIndex %v", rf.me, server, rf.peerTrackers[server].nextIndex, rf.peerTrackers[server].matchIndex)
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
