package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const tickInterval = 50 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type PersistentStateOnAllServers struct {
	currentTerm int
	votedFor    int
	log         Log
}

type VolatileStateOnAllServers struct {
	lastApplied int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             RaftState
	persistentState   PersistentStateOnAllServers
	volatileState     VolatileStateOnAllServers
	lastHeartbeatTime time.Time
	votesReceived     int

	electionTimeout time.Duration
	lastElection    time.Time

	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time

	peerTrackers []PeerTracker

	applyCh          chan<- ApplyMsg
	claimToBeApplied sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.persistentState.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == Leader
	if !isLeader {
		return -1, -1, false
	}

	index := rf.persistentState.log.lastIndex() + 1
	entry := LogEntry{Term: rf.persistentState.currentTerm, Index: index, Data: command}
	rf.persistentState.log.append([]LogEntry{entry})
	rf.persist()

	rf.broadcastAppendEntries(true)

	return index, rf.persistentState.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.resetTrackedIndexes()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.persistentState.currentTerm++
	rf.persistentState.votedFor = rf.me
	rf.votesReceived = 1
	rf.resetElectionTimer()
}

func (rf *Raft) becomeFollower(term int) bool {
	rf.state = Follower
	if term > rf.persistentState.currentTerm {
		rf.persistentState.currentTerm = term
		rf.persistentState.votedFor = -1
		return true
	}
	return false
}

func (rf *Raft) pastHeartbeatTimeout() bool {
	return time.Since(rf.lastHeartbeat) >= rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		switch rf.state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.pastElectionTimeout() {
				rf.becomeCandidate()
				rf.broadcastRequestVote()
			}

		case Leader:
			if !rf.quorumActive() {
				rf.becomeFollower(rf.persistentState.currentTerm)
				break
			}

			forced := false
			if rf.pastHeartbeatTimeout() {
				forced = true
				rf.resetHeartbeatTimer()
			}
			rf.broadcastAppendEntries(forced)
		}

		rf.mu.Unlock()
		time.Sleep(tickInterval)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DInit()

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.persistentState.log = makeLog()
	rf.persistentState.currentTerm = 0
	rf.persistentState.votedFor = -1
	rf.volatileState.lastApplied = 0
	rf.applyCh = applyCh
	rf.claimToBeApplied = sync.Cond{L: &rf.mu}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// update tracked indexes with the restored log entries.
	rf.peerTrackers = make([]PeerTracker, len(rf.peers))
	rf.resetTrackedIndexes()

	rf.state = Follower
	rf.resetElectionTimer()
	rf.heartbeatTimeout = heartbeatTimeout

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.committer()

	return rf
}
