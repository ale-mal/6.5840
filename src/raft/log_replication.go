package raft

import "time"

func (rf *Raft) checkLogPrefixMatched(leaderPrevLogIndex, leaderPrevLogTerm int) Err {
	if leaderPrevLogIndex < 0 {
		if len(rf.persistentState.log) > 0 {
			// Reply false if log doesn’t contain an entry at prevLogIndex (§5.3)
			return IndexNotMatched
		}
	} else if len(rf.persistentState.log) <= leaderPrevLogIndex {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		return IndexNotMatched
	} else if rf.persistentState.log[leaderPrevLogIndex].Term != leaderPrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		return TermNotMatched
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
		return
	}

	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	for i := 0; i < len(args.Entries); i++ {
		if len(rf.persistentState.log) == args.PrevLogIndex+i+1 {
			rf.persistentState.log = append(rf.persistentState.log, args.Entries[i])
		} else {
			if rf.persistentState.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				rf.persistentState.log = rf.persistentState.log[:args.PrevLogIndex+i+1]
				rf.persistentState.log = append(rf.persistentState.log, args.Entries[i])
			}
		}
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.volatileState.commitIndex {
		rf.volatileState.commitIndex = args.LeaderCommit
		indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
		if rf.volatileState.commitIndex > indexOfLastNewEntry {
			rf.volatileState.commitIndex = indexOfLastNewEntry
		}
	}

	rf.lastHeartbeatTime = time.Now()
	rf.resetElectionTimer()

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
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
		rf.volatileStateLeader.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.volatileStateLeader.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	} else {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		rf.volatileStateLeader.nextIndex[server]--
	}
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
		args := &AppendEntriesArgs{}
		args.Term = rf.persistentState.currentTerm
		args.LeaderId = rf.me
		if len(rf.persistentState.log) > 0 {
			args.PrevLogIndex = len(rf.persistentState.log) - 1
			args.PrevLogTerm = rf.persistentState.log[args.PrevLogIndex].Term
		} else {
			args.PrevLogIndex = -1
			args.PrevLogTerm = -1
		}
		args.Entries = rf.persistentState.log
		args.LeaderCommit = rf.volatileState.commitIndex
		go rf.sendAppendEntries(i, args)
	}
}
