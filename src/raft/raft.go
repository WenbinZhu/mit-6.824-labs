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
	"sync"
	"labrpc"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistant state on all servers
	currentTerm int
	votedFor int
	logs []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex []int
	matchIndex []int

	// Others
	status int
	voteCount int
	applyCh chan ApplyMsg
	electWin chan bool
	granted chan bool
	heartbeat chan bool
}

type LogEntry struct {
	Term int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.status == Leader
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// bootstrap without any state
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term int
	Success bool
	nextTryIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Do not grant vote if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// Convert to follower state if term > currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.granted <- true
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) isUpToDate(cIndex int, cTerm int) bool {
	term, index := rf.getLastTerm(), rf.getLastIndex()

	if cTerm != term {
		return cTerm >= term
	}

	return cIndex >= index
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return ok
	}
	if rf.status != Candidate || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers) / 2 {
			rf.status = Leader
			rf.electWin <- true
		}
	}

	return ok
}

func (rf *Raft) sendAllRequestVotes() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.status == Candidate {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.nextTryIndex = rf.getLastIndex() + 1
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}

	rf.heartbeat <- true
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.nextTryIndex = rf.getLastIndex() + 1
		return
	}

	// Follower's log conflict with leader's
	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		term := rf.logs[args.PrevLogIndex].Term

		for reply.nextTryIndex = args.PrevLogIndex - 1;
			reply.nextTryIndex > 0 && rf.logs[reply.nextTryIndex].Term == term;
			reply.nextTryIndex-- {}

		reply.nextTryIndex++
	} else {
		var restLogs []LogEntry
		rf.logs, restLogs = rf.logs[:args.PrevLogIndex + 1], rf.logs[args.PrevLogIndex + 1 :]

		// If an existing entry conflicts with a new one (same index but
		// different terms), delete the existing entry and all that follow it
		if rf.hasConflictLogs(restLogs, args.Entries) || len(restLogs) < len(args.Entries) {
			rf.logs = append(rf.logs, args.Entries...)
		} else {
			rf.logs = append(rf.logs, restLogs...)
		}

		reply.Success = true
		reply.nextTryIndex = args.PrevLogIndex

		// Update follower's commitIndex if no conflict
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit <= rf.getLastIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.getLastIndex()
			}

			go rf.commitLogs()
		}
	}

	return
}

func (rf *Raft) hasConflictLogs(serverLogs []LogEntry, leaderLogs []LogEntry) bool {
	for i := range serverLogs {
		if i >= len(leaderLogs) {
			break
		}
		if serverLogs[i].Term != leaderLogs[i].Term {
			return true
		}
	}

	return false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.status != Leader || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = reply.nextTryIndex
	}

	// If there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
	for N := rf.getLastIndex(); N > rf.commitIndex; N-- {
		count := 1

		if rf.logs[N].Term == rf.currentTerm {
			for i := range rf.peers {
				if rf.matchIndex[i] >= N {
					count++
				}
			}
		}

		if count > len(rf.peers) / 2 {
			rf.commitIndex = N
			go rf.commitLogs()
			break
		}
	}

	return ok
}

func (rf *Raft) sendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.LeaderCommit = rf.commitIndex

			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}
			if rf.nextIndex[i] <= rf.getLastIndex() {
				args.Entries = rf.logs[rf.nextIndex[i] :]
			}

			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

 	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Index: i, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := 0
	term := 0
	isLeader := rf.status == Leader

	if isLeader {
		index = rf.getLastIndex() + 1
		term = rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{term, command})
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) runServer() {
	//rand.Seed(time.Now().Unix())

	for {
		switch rf.status {
		case Leader:
			//DPrintf("Server %d, Leader, term %d", rf.me, rf.currentTerm)
			rf.sendAllAppendEntries()
			time.Sleep(time.Millisecond * 120)

		case Follower:
			select {
			case <-rf.granted:
			case <-rf.heartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200) + 300)):
				rf.status = Candidate
			}

		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			rf.voteCount = 1
			rf.mu.Unlock()
			rf.sendAllRequestVotes()

			select {
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200) + 300)):
			case <-rf.heartbeat:
				rf.status = Follower
			case <-rf.electWin:
				rf.mu.Lock()
				rf.status = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				nextIdx := rf.getLastIndex() + 1

				for i := range rf.peers {
					rf.nextIndex[i] = nextIdx
				}

				rf.mu.Unlock()
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteCount = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower
	rf.applyCh = applyCh
	rf.electWin = make(chan bool)
	rf.granted = make(chan bool)
	rf.heartbeat = make(chan bool)
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	go rf.runServer()

	return rf
}
