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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term int
}

type PersistentStateOnAllServers struct {
	currentTerm int
	votedFor    int
	log         []LogEntry
}

type VolatileStateOnAllServers struct {
	commitIndex int
	lastApplied int
}

type VolatileStateOnLeaders struct {
	nextIndex  []int
	matchIndex []int
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
	state               RaftState
	persistentState     PersistentStateOnAllServers
	volatileState       VolatileStateOnAllServers
	volatileStateLeader VolatileStateOnLeaders
	lastHeartbeatTime   time.Time
	votesReceived       int
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


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.persistentState.currentTerm {
		DPrintf(dVote, "S%d RequestVote: term %v < currentTerm %v", rf.me, args.Term, rf.persistentState.currentTerm)
		reply.Term = rf.persistentState.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.persistentState.currentTerm {
		DPrintf(dTerm, "S%d RequestVote: another server is requesting vote with greater term %v, current term goes from %v to %v. State is switching from %v to %v", rf.me, args.Term, rf.persistentState.currentTerm, args.Term, rf.state, Follower)
		rf.persistentState.currentTerm = args.Term
		rf.persistentState.votedFor = args.CandidateId
		reply.Term = rf.persistentState.currentTerm
		reply.VoteGranted = true
		rf.mu.Unlock()
		rf.switchToFollower()
		return
	}
	if rf.persistentState.votedFor != -1 && rf.persistentState.votedFor != args.CandidateId {
		DPrintf(dVote, "S%d RequestVote: already voted for %v, can't vote for %v", rf.me, rf.persistentState.votedFor, args.CandidateId)
		reply.Term = rf.persistentState.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	if args.LastLogIndex > 0 {
		if len(rf.persistentState.log) > 0 {
			if args.LastLogIndex < len(rf.persistentState.log) {
				DPrintf(dVote, "S%d RequestVote: lastLogIndex %v < len(log) %v", rf.me, args.LastLogIndex, len(rf.persistentState.log))
				reply.Term = rf.persistentState.currentTerm
				reply.VoteGranted = false
				rf.mu.Unlock()
				return
			}
			if args.LastLogIndex == len(rf.persistentState.log) && args.LastLogTerm < rf.persistentState.log[args.LastLogIndex-1].Term {
				DPrintf(dVote, "S%d RequestVote: lastLogTerm %v < log[lastLogIndex-1].Term %v", rf.me, args.LastLogTerm, rf.persistentState.log[args.LastLogIndex-1].Term)
				reply.Term = rf.persistentState.currentTerm
				reply.VoteGranted = false
				rf.mu.Unlock()
				return
			}
		}
	} else {
		if len(rf.persistentState.log) > 0 {
			DPrintf(dVote, "S%d RequestVote: lastLogIndex %v < 0, but log is not empty", rf.me, args.LastLogIndex)
			reply.Term = rf.persistentState.currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}
	}

	DPrintf(dVote, "S%d RequestVote: voting for %v", rf.me, args.CandidateId)
	rf.persistentState.votedFor = args.CandidateId
	reply.Term = rf.persistentState.currentTerm
	reply.VoteGranted = true
	rf.mu.Unlock()
	rf.electionTimeout(args.Term, Follower)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()

	if rf.state != Candidate {
		rf.mu.Unlock()
		return true
	}

	if args.Term != rf.persistentState.currentTerm {
		rf.mu.Unlock()
		return true
	}

	if reply.Term > rf.persistentState.currentTerm {
		DPrintf(dTerm, "S%d RequestVote: another server replies with greater term %v, current term goes from %v to %v. State is switching from %v to %v", rf.me, reply.Term, rf.persistentState.currentTerm, reply.Term, rf.state, Follower)
		rf.persistentState.currentTerm = reply.Term
		rf.persistentState.votedFor = -1
		rf.mu.Unlock()
		rf.switchToFollower()
		return true
	}

	if !reply.VoteGranted {
		DPrintf(dVote, "S%d RequestVote: vote not granted", rf.me)
		rf.mu.Unlock()
		return true
	}

	rf.votesReceived++
	if rf.votesReceived <= len(rf.peers)/2 {
		DPrintf(dVote, "S%d RequestVote: got vote, but not majority yet, votes received %v", rf.me, rf.votesReceived)
		rf.mu.Unlock()
		return true
	}

	DPrintf(dLeader, "S%d RequestVote: got majority of votes, switching state from %v to %v", rf.me, rf.state, Leader)
	rf.mu.Unlock()
	rf.switchToLeader()
	return true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.persistentState.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		DPrintf(dLog, "S%d AppendEntries: term %v < currentTerm %v", rf.me, args.Term, rf.persistentState.currentTerm)
		reply.Term = rf.persistentState.currentTerm
		reply.Success = false
		return
	}
	if args.PrevLogIndex < 0 {
		if len(rf.persistentState.log) > 0 {
			// Reply false if log doesn’t contain an entry at prevLogIndex (§5.3)
			DPrintf(dLog, "S%d AppendEntries: log doesn't contain an entry at prevLogIndex %v", rf.me, args.PrevLogIndex)
			reply.Term = rf.persistentState.currentTerm
			reply.Success = false
			return
		}
	} else if len(rf.persistentState.log) <= args.PrevLogIndex || rf.persistentState.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		DPrintf(dLog, "S%d AppendEntries: log doesn't contain an entry at prevLogIndex %v whose term matches prevLogTerm %v", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = rf.persistentState.currentTerm
		reply.Success = false
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
	if rf.persistentState.currentTerm == args.Term {
		DPrintf(dLog, "S%d AppendEntries: all ok, current term is %v", rf.me, rf.persistentState.currentTerm)
	} else {
		DPrintf(dTerm, "S%d AppendEntries: all ok, current term goes from %v to %v", rf.me, rf.persistentState.currentTerm, args.Term)
	}

	rf.persistentState.currentTerm = args.Term
	rf.persistentState.votedFor = -1
	rf.state = Follower
	rf.lastHeartbeatTime = time.Now()

	reply.Term = rf.persistentState.currentTerm
	reply.Success = true

	go rf.electionTimeout(rf.persistentState.currentTerm, Follower)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.persistentState.currentTerm {
			DPrintf(dTerm, "S%d AppendEntries: another server replies with greater term %v, current term goes from %v to %v. State is switching from %v to %v", rf.me, reply.Term, rf.persistentState.currentTerm, reply.Term, rf.state, Follower)
			rf.persistentState.currentTerm = reply.Term
			rf.persistentState.votedFor = -1
			rf.mu.Unlock()
			rf.switchToFollower()
			return true
		}
		if rf.state != Leader || args.Term != rf.persistentState.currentTerm {
			rf.mu.Unlock()
			return true
		}
		if reply.Success {
			// If successful: update nextIndex and matchIndex for follower (§5.3)
			rf.volatileStateLeader.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.volatileStateLeader.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			rf.volatileStateLeader.nextIndex[server]--
		}
		rf.mu.Unlock()
	}
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

func (rf *Raft) electionTimeout(term int, state RaftState) {
	// pause for a random amount of time between 150 and 300
	ms := 150 + (rand.Int63() % 150)
	time.Sleep(time.Duration(ms) * time.Millisecond)

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.persistentState.currentTerm != term {
		DPrintf(dTimer, "S%d Skip election, term changed from %v to %v", rf.me, term, rf.persistentState.currentTerm)
		return
	}
	if rf.state != state {
		DPrintf(dTimer, "S%d Skip election, state changed from %v to %v", rf.me, state, rf.state)
		return
	}
	if time.Since(rf.lastHeartbeatTime) <= 150*time.Millisecond {
		DPrintf(dTimer, "S%d Restart election timeout, heartbeat received within last 150ms", rf.me)
		return
	}

	DPrintf(dTimer, "S%d Start election, no leader heartbeat received", rf.me)
	go rf.switchToCandidate()
}

func (rf *Raft) heartbeatTimeout(term int) {
	time.Sleep(time.Duration(100) * time.Millisecond)

	rf.mu.Lock()
	if rf.persistentState.currentTerm != term {
		DPrintf(dTimer, "S%d Stopping heartbeat timer, term changed from %v to %v", rf.me, term, rf.persistentState.currentTerm)
		rf.mu.Unlock()
		return
	}
	if rf.state != Leader {
		DPrintf(dTimer, "S%d Stopping heartbeat timer, not a leader, state is %v", rf.me, rf.state)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.sendHeartbeat()
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dTimer, "S%d Sending heartbeat", rf.me)
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
		go func(server int) {
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
		}(i)
	}

	go rf.heartbeatTimeout(rf.persistentState.currentTerm)
}

func (rf *Raft) switchToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dLog, "S%d Follower: switching state from %v to %v", rf.me, rf.state, Follower)
	rf.state = Follower
	go rf.electionTimeout(rf.persistentState.currentTerm, Follower)
}

func (rf *Raft) switchToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dLog, "S%d Candidate: switching state from %v to %v, increment currentTerm to %v, start election", rf.me, rf.state, Candidate, rf.persistentState.currentTerm+1)
	rf.state = Candidate
	rf.persistentState.currentTerm++
	rf.persistentState.votedFor = rf.me
	rf.votesReceived = 1

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
		go func(server int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)
		}(i)
	}

	go rf.electionTimeout(rf.persistentState.currentTerm, Candidate)
}

func (rf *Raft) switchToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dLog, "S%d Leader: switching state from %v to %v", rf.me, rf.state, Leader)
	rf.state = Leader
	rf.volatileStateLeader.nextIndex = make([]int, len(rf.peers))
	rf.volatileStateLeader.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.volatileStateLeader.nextIndex[i] = len(rf.persistentState.log)
		rf.volatileStateLeader.matchIndex[i] = 0
	}
	go rf.sendHeartbeat()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.state = Follower
	rf.persistentState.currentTerm = 0
	rf.persistentState.votedFor = -1
	rf.volatileState.commitIndex = 0
	rf.volatileState.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.switchToFollower()
	go rf.ticker()


	return rf
}
