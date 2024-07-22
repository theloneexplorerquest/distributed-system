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
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"strings"
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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type state int

// Declare constants using iota
const (
	Leader state = iota
	Follower
	Candidate
)

func (cr state) String() string {
	switch cr {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	currentTerm     int
	votedFor        int
	logs            []Entry
	commitIndex     int
	lastApplied     int
	state           state
	lastActive      time.Time
	voteCount       int
	electionTimeOut time.Duration
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
	winElectionCh   chan bool
	currentTime     time.Time
	winElectCh      chan bool
	stepDownCh      chan bool
	grantVoteCh     chan bool
	heartbeatCh     chan bool
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		// Handle the error
		panic(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		// Handle the error, e.g., log it or panic
		panic(err)
	}
	if err := e.Encode(rf.logs); err != nil {
		// Handle the error
		panic(err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var logs []Entry
	var currentTerm int
	// Decode each field with detailed error handling
	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("Failed to decode currentTerm: %v\n", err)
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("Failed to decode votedFor: %v\n", err)
		return
	}
	if err := d.Decode(&logs); err != nil {
		fmt.Printf("Failed to decode logs: %v\n", err)
		return
	}
	// Only update the state if all decodes were successful
	rf.votedFor = votedFor
	rf.logs = logs
	rf.currentTerm = currentTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	XIndex  int
	XTerm   int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1

	// Check if the term is outdated
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownAsFollower(args.Term)
	}

	rf.sendToChannel(rf.heartbeatCh, true)

	reply.Term = rf.currentTerm
	reply.Success = false
	// Check if PrevLogIndex is out of bounds
	if args.PrevLogIndex > len(rf.logs)-1 {
		reply.Success = false
		reply.XLen = len(rf.logs)
		return
	}

	// Check for log inconsistency
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		xIndex := args.PrevLogIndex
		for xIndex >= 0 && rf.logs[xIndex].Term == rf.logs[args.PrevLogIndex].Term {
			xIndex--
		}
		reply.XIndex = xIndex + 1
		return
	}

	// Truncate the log if there's a conflict with the incoming entries
	if len(args.Entries) > 0 {
		start := args.PrevLogIndex + 1
		index := 0
		for index < len(args.Entries) {
			if start+index < len(rf.logs) {
				if rf.logs[start+index].Term != args.Entries[index].Term {
					rf.logs = rf.logs[:start+index]
					break
				}
			} else {
				break
			}
			index++
		}
		// Append the new entries
		rf.logs = append(rf.logs, args.Entries[index:]...)
	}

	// Update commit index
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//go rf.commitLogEntries()
	}
	return

}

func (rf *Raft) updateLeaderCommitIndex() {
	for n := len(rf.logs) - 1; n >= rf.commitIndex; n-- {
		count := 1
		if rf.logs[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			//go rf.commitLogEntries()
			break
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownAsFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.LogOk(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.sendToChannel(rf.grantVoteCh, true)
	}
}

func (rf *Raft) LogOk(argLastLogTerm int, argLastLogIndex int) bool {
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	logOk := argLastLogTerm > lastLogTerm || (lastLogTerm == argLastLogTerm && argLastLogIndex >= lastLogIndex)
	return logOk
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
	//index := -1
	//term := -1
	//isLeader := true
	//log.Printf("enter raft start %s", command)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("leave raft start")
	if rf.state != Leader {
		//log.Printf("leave raft start %s", command)
		return -1, rf.currentTerm, false
	} else {
		currentTerm := rf.currentTerm
		entry := Entry{
			Term:    rf.currentTerm,
			Command: command,
		}

		rf.logs = append(rf.logs, entry)
		rf.persist()
		//log.Printf("s%d raft start %s", rf.me, command)
		rf.broadcastAppendEntries()
		return len(rf.logs) - 1, currentTerm, true
	}
}

func (rf *Raft) stepDownAsFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	// step down if not follower, this check is needed
	// to prevent race where state is already follower
	if state != Follower {
		rf.sendToChannel(rf.stepDownCh, true)
	}
}

func (rf *Raft) commitLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) runCommitLogEntries() {
	for !rf.Killed() {
		rf.commitLogEntries()
		time.Sleep(20 * time.Millisecond)
	}
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

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state != Candidate {
		return
	}

	rf.resetChannels()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.resetIndex()
	//log.Printf("s%d is elected as leader", rf.me)
	rf.broadcastAppendEntries()
}

func (rf *Raft) resetIndex() {
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs) // Replace initialValue with the desired default value
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			entries := rf.logs[rf.nextIndex[server]:]
			args.Entries = make([]Entry, len(entries))
			// make a deep copy of the entries to send
			copy(args.Entries, entries)
			go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownAsFollower(args.Term)
		return
	}

	// update matchIndex and nextIndex of the follower
	if reply.Success {
		// match index should not regress in case of stale rpc response
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.backwardIndex(reply.XTerm, reply.XIndex, reply.XLen, server)
	}

	rf.updateLeaderCommitIndex()
}

func (rf *Raft) initiateElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.voteCount = 1
}

func (rf *Raft) backwardIndex(xTerm int, xIndex int, xLen int, server int) {
	if xTerm != -1 && xIndex != -1 {
		currentIndex := rf.nextIndex[server] - 1
		hasTerm := false
		for currentIndex >= 1 && rf.logs[currentIndex].Term >= xTerm {
			if rf.logs[currentIndex].Term == xTerm {
				rf.nextIndex[server] = currentIndex
				hasTerm = true
				break
			}
			currentIndex -= 1
		}
		if hasTerm == false {
			rf.nextIndex[server] = xIndex
		}
	} else if xLen != -1 {
		rf.nextIndex[server] = xLen
	} else {
		rf.nextIndex[server]--
	}
	rf.matchIndex[server] = rf.nextIndex[server] - 1
}

func (rf *Raft) PrintLogs(logs []Entry) string {
	var sb strings.Builder
	sb.WriteString("[")
	for index, entry := range logs {
		if rf.lastApplied < index {
			sb.WriteString(fmt.Sprintf("%d:%v(%d, f)", index, entry.Command, entry.Term))
		} else {
			sb.WriteString(fmt.Sprintf("%d:%v(%d)", index, entry.Command, entry.Term))
		}
		if index < len(logs)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]")
	return sb.String()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.logs = append(rf.logs, Entry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start the background server loop
	go rf.runServer()
	go rf.runCommitLogEntries()
	return rf
}

func (rf *Raft) runServer() {
	for !rf.Killed() {
		//log.Printf("raft running")
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
				// state should already be follower
			case <-time.After(120 * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
				// state should already be follower
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Candidate)
			}
		}
	}
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

func (rf *Raft) convertToCandidate(fromState state) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	rf.broadcastRequestVote()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownAsFollower(args.Term)
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		// only send once when vote count just reaches majority
		if rf.voteCount == len(rf.peers)/2+1 {
			rf.sendToChannel(rf.winElectCh, true)
		}
	}
}

// broadcast RequestVote RPCs to all peers in parallel.
// lock must be held before calling this.
func (rf *Raft) broadcastRequestVote() {
	if rf.state != Candidate {
		return
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

// get the term of the last log entry.
// lock must be held before calling this.
func (rf *Raft) getLastTerm() int {
	return rf.logs[rf.getLastIndex()].Term
}

func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}

func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}
