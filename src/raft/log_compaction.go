package raft

func (rf *Raft) lagBehindSnapshot(to int) bool {
	return rf.peerTrackers[to].nextIndex <= rf.persistentState.log.firstIndex()
}
