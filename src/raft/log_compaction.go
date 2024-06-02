package raft

func (rf *Raft) lagBehindSnapshot(to int) bool {
	return rf.peerTrackers[to].nextIndex <= rf.persistentState.log.firstIndex()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	DPrintf(dSnap, "S%d InstallSnapshot: sending snapshot to %v with term %v", rf.me, server, args.Term)
	reply := &InstallSnapshotReply{}
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply); ok {
		rf.handleInstallSnapshotReply(server, args, reply)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.From = rf.me
	reply.Term = rf.persistentState.currentTerm
	reply.CaughtUp = false

	m := Message{Type: Snap, From: args.LeaderId, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.persistentState.currentTerm
		defer rf.persist()
	}
	if !ok {
		return
	}

	if args.Snapshot.Index <= rf.persistentState.log.commited {
		reply.CaughtUp = true
		return
	}

	rf.persistentState.log.compactedTo(args.Snapshot)
	reply.CaughtUp = true
	if !termChanged {
		defer rf.persist()
	}

	rf.persistentState.log.hasPendingSnapshot = true
	rf.claimToBeApplied.Signal()
}

func (rf *Raft) handleInstallSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	m := Message{From: server, Type: SnapReply, Term: reply.Term, ArgsTerm: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		DPrintf(dSnap, "S%d InstallSnapshot: term is stale from %v. Reply term: %v. Next index: %v", rf.me, server, reply.Term, rf.peerTrackers[server].nextIndex)
		return
	}

	if reply.CaughtUp {
		DPrintf(dSnap, "S%d InstallSnapshot: got caught up from %v", rf.me, server)
		rf.peerTrackers[server].nextIndex = args.Snapshot.Index + 1
		rf.peerTrackers[server].matchIndex = args.Snapshot.Index

		rf.broadcastAppendEntries(true)
	} else {
		DPrintf(dSnap, "S%d InstallSnapshot: not caught up from %v", rf.me, server)
	}
}
