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
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type CurrentRole int

// Declare constants using iota
const (
	Leader CurrentRole = iota
	Follower
	Candidate
)

func (cr CurrentRole) String() string {
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

const (
	minElectionTimeout     = 300 * time.Millisecond // Minimum election timeout
	maxElectionTimeout     = 800 * time.Millisecond // Maximum election timeout
	heartbeatCheckInterval = 100 * time.Millisecond // Consistent heartbeat interval
)

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
	currentRole     CurrentRole
	lastActive      time.Time
	voteReceived    int
	electionTimeOut time.Duration
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
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
	return rf.currentTerm, rf.currentRole == Leader
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
	//DPrintf(dClient, "S%d Called AppendEntries at Term %d", rf.me, rf.currentTerm)

	// Update last active time

	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1

	// Check if the term is outdated
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

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
	//entryModified := false
	if len(args.Entries) > 0 {
		start := args.PrevLogIndex + 1
		index := 0
		for index < len(args.Entries) {
			if start+index < len(rf.logs) {
				if rf.logs[start+index].Term != args.Entries[index].Term {
					rf.logs = rf.logs[:start+index]
					//entryModified = true
					break
				}
			} else {
				break
			}
			index++
		}
		// Append the new entries
		rf.logs = append(rf.logs, args.Entries[index:]...)
		//entryModified = true
	}

	// Persist log if modified

	// Update commit index
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	// Update stat
	if rf.currentTerm != args.Term {
		//entryModified = true
		rf.currentTerm = args.Term
	}
	rf.currentRole = Follower

	//if entryModified {
	//	rf.persist()
	//}
	rf.resetElectionTimeout()

	// Send success reply
	reply.Term = args.Term
	reply.Success = true

	commitIndex := rf.commitIndex
	go rf.commitLogEntries(commitIndex)
	return

}

func (rf *Raft) updateCommitIndex() {
	minAcks := (len(rf.peers) + 1) / 2
	ready := make([]int, 0)
	for i := 0; i < len(rf.logs); i++ {
		if rf.acks(i) >= minAcks {
			ready = append(ready, i)
		}
	}
	if len(ready) > 0 && ready[len(ready)-1] > rf.commitIndex && rf.logs[ready[len(ready)-1]].Term == rf.currentTerm {
		rf.commitIndex = ready[len(ready)-1]
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//entryModified := false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		rf.votedFor = -1 // Reset votedFor as this is a new term
		//entryModified = true
		//rf.persist()
	}
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	logOk := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.logs)-1)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logOk {
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		//entryModified = true
		rf.resetElectionTimeout()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	//if entryModified {
	//	rf.persist()
	//}
	//DPrintf(dVote, "S%d voted to %d at Term %d, request from %d, granted? %t timeout is %s", rf.me, rf.votedFor, rf.currentTerm, args.CandidateId, reply.VoteGranted, rf.electionTimeOut)

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
	//index := -1
	//term := -1
	//isLeader := true
	rf.mu.Lock()
	if rf.currentRole != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	} else {
		currentTerm := rf.currentTerm
		leaderCommit := rf.commitIndex
		entry := Entry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.logs = append(rf.logs, entry)
		//rf.persist()
		logIndex := len(rf.logs) - 1
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		rf.mu.Unlock()
		go rf.broadcastAppendEntries(currentTerm, leaderCommit)
		return logIndex, currentTerm, true
	}
}

func (rf *Raft) broadcastAppendEntries(currentTerm int, leaderCommit int) {
	var wg sync.WaitGroup
	for idx, peer := range rf.peers {
		rf.mu.Lock()
		if idx != rf.me && len(rf.logs) >= rf.nextIndex[idx] {
			rf.mu.Unlock()
			wg.Add(1)
			go rf.sendAppendEntries(peer, idx, currentTerm, leaderCommit, &wg)
		} else {
			rf.mu.Unlock()
		}
	}
	wg.Wait()
}

func (rf *Raft) sendAppendEntries(peer *labrpc.ClientEnd, idx, currentTerm, leaderCommit int, wg *sync.WaitGroup) {
	defer wg.Done()
	appendEntriesReply := AppendEntriesReply{}
	rf.mu.Lock()
	if len(rf.logs) < rf.nextIndex[idx] {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[idx] - 1
	entries := make([]Entry, 0)
	// index out of bound
	slice := rf.logs[rf.nextIndex[idx]:]
	entries = make([]Entry, len(slice))
	copy(entries, slice)
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.logs[prevLogIndex].Term
	}
	rf.mu.Unlock()
	appendEntriesArgs := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	for {
		response := peer.Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply)
		if response {
			rf.handleAppendEntriesReply(idx, &appendEntriesArgs, &appendEntriesReply)
			return
		} else {
			time.Sleep(heartbeatCheckInterval)
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(idx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	currentRole := rf.currentRole
	rf.mu.Unlock()
	if reply.Term == currentTerm && currentRole == Leader {
		rf.mu.Lock()
		if reply.Success && args.PrevLogIndex+len(args.Entries)+1 >= rf.nextIndex[idx] {
			rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[idx] = rf.nextIndex[idx] - 1
			rf.updateCommitIndex()
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			go rf.commitLogEntries(commitIndex)
			//return
		} else if reply.Term > currentTerm {
			rf.currentTerm = args.Term
			rf.currentRole = Follower
			rf.votedFor = -1
			//rf.persist()
			rf.mu.Unlock()
		} else if rf.nextIndex[idx] > 1 {
			if reply.XTerm != -1 && reply.XIndex != -1 {
				currentIndex := rf.nextIndex[idx] - 1
				hasTerm := false
				for currentIndex >= 1 && rf.logs[currentIndex].Term >= reply.XTerm {
					if rf.logs[currentIndex].Term == reply.Term {
						rf.nextIndex[idx] = currentIndex
						if rf.nextIndex[idx] < 1 {
							panic(fmt.Sprintf("currentIndex out of range: %d", reply.XIndex))
						}
						hasTerm = true
						break
					}
					currentIndex -= 1
				}
				if hasTerm == false {
					rf.nextIndex[idx] = reply.XIndex
					if rf.nextIndex[idx] < 1 {
						panic(fmt.Sprintf("XIndex out of range: %d", reply.XIndex))
					}
				}
			} else if reply.XLen != -1 {
				rf.nextIndex[idx] = reply.XLen
				if rf.nextIndex[idx] < 1 {
					panic(fmt.Sprintf("XLen out of range: %d", reply.XIndex))
				}
			} else {
				rf.nextIndex[idx]--
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}

	} else if args.Term > currentTerm {
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		rf.votedFor = -1
		//rf.persist()
	}
}

func (rf *Raft) commitLogEntries(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
			CommandTerm:  rf.logs[rf.lastApplied].Term,
		}
	}
}

func (rf *Raft) acks(idx int) int {
	totalAcked := 0
	for i, _ := range rf.peers {
		if rf.matchIndex[i] >= idx {
			totalAcked++
		}
	}
	return totalAcked
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

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		// Sleep for the specified heartbeat check interval
		// Check if the Raft instance is killed
		time.Sleep(heartbeatCheckInterval)

		// Sleep for the specified heartbeat check interval

		rf.mu.Lock()
		if rf.currentRole != Leader && time.Since(rf.lastActive) >= rf.electionTimeOut {
			rf.mu.Unlock()
			// Start an election
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.lastActive = time.Now()
	rf.electionTimeOut = time.Duration(rand.Intn(int(maxElectionTimeout-minElectionTimeout))) + minElectionTimeout
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	// Start election
	//DPrintf(dCommit, "S%d start election %d, timeout is %s", rf.me, rf.currentTerm, rf.electionTimeOut)
	rf.currentTerm++
	rf.currentRole = Candidate
	rf.votedFor = rf.me
	//rf.persist()
	rf.voteReceived = 1
	currentTerm := rf.currentTerm
	candidateId := rf.me
	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	lastLogIndex := len(rf.logs) - 1
	rf.resetElectionTimeout()
	electionTimeout := time.After(rf.electionTimeOut)
	rf.mu.Unlock()

	var wg sync.WaitGroup
	voteCh := make(chan bool, len(rf.peers))
	for idx, peer := range rf.peers {
		if idx != candidateId {
			wg.Add(1)
			go func(peer *labrpc.ClientEnd, idx int) {
				defer wg.Done()
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  candidateId,
					LastLogTerm:  lastLogTerm,
					LastLogIndex: lastLogIndex,
				}
				reply := RequestVoteReply{}
				//DPrintf(dTerm, "S%d Request to vote at Term %d, request to %d at %s", rf.me, rf.currentTerm, idx, args.Time)
				if peer.Call("Raft.RequestVote", &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentRole != Candidate {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.currentRole = Follower
						rf.votedFor = -1
						//rf.persist()
					} else if reply.VoteGranted && reply.Term == currentTerm {
						rf.voteReceived++
						if rf.voteReceived > len(rf.peers)/2 {
							rf.currentRole = Leader
							for i := range rf.nextIndex {
								rf.nextIndex[i] = len(rf.logs) // Replace initialValue with the desired default value
								rf.matchIndex[i] = 0
							}
							go rf.sendHeartbeats()
							voteCh <- true
						}
					}
				}
			}(peer, idx)
		}
	}

	go func() {
		wg.Wait()
		voteCh <- true
	}()
	for {
		select {
		case <-voteCh:
			rf.mu.Lock()
			if rf.currentRole == Candidate && rf.voteReceived > len(rf.peers)/2 {
				DPrintf(dLeader, "S%d Leader elected", rf.me)
				rf.currentRole = Leader
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.logs) // Replace initialValue with the desired default value
					rf.matchIndex[i] = 0
				}
				go rf.sendHeartbeats()
			}
			rf.mu.Unlock()
			return
		case <-electionTimeout:
			rf.mu.Lock()
			if rf.currentRole == Candidate {
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
			}
			return
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.currentRole != Leader {
			rf.mu.Unlock()
			time.Sleep(heartbeatCheckInterval)
			continue
		}
		currentTerm := rf.currentTerm
		leaderId := rf.me
		commitIndex := rf.commitIndex

		rf.mu.Unlock()

		var wg sync.WaitGroup
		for idx, peer := range rf.peers {
			if idx != rf.me {
				wg.Add(1)
				go rf.sendHeartbeatsToServer(peer, idx, currentTerm, leaderId, commitIndex, &wg)
			}
		}

		wg.Wait()
		time.Sleep(heartbeatCheckInterval)
	}
}

func (rf *Raft) sendHeartbeatsToServer(peer *labrpc.ClientEnd, idx int, currentTerm int, leaderId int, commitIndex int, wg *sync.WaitGroup) {
	defer wg.Done()
	rf.mu.Lock()
	if rf.currentRole != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[idx] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.logs[prevLogIndex].Term
	}
	entries := make([]Entry, 0)
	if len(rf.logs)-1 >= rf.nextIndex[idx] {
		slice := rf.logs[rf.nextIndex[idx]:]
		entries = make([]Entry, len(slice))
		copy(entries, slice)
	}
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIndex,
		Entries:      entries,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	done := make(chan bool)
	go func() {
		done <- peer.Call("Raft.AppendEntries", &args, &reply)
	}()
	select {
	case success := <-done:
		if success {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > currentTerm {
				rf.currentTerm = reply.Term
				rf.currentRole = Follower
				rf.votedFor = -1
				//rf.persist()
			} else if reply.Success && reply.Term == currentTerm {
				rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[idx] = rf.nextIndex[idx] - 1
			} else if rf.nextIndex[idx] > 1 {
				if reply.XTerm != -1 && reply.XIndex != -1 {
					currentIndex := rf.nextIndex[idx] - 1
					hasTerm := false
					for currentIndex >= 1 && rf.logs[currentIndex].Term >= reply.XTerm {
						if rf.logs[currentIndex].Term == reply.Term {
							rf.nextIndex[idx] = currentIndex
							if rf.nextIndex[idx] < 1 {
								panic(fmt.Sprintf("currentIndex out of range: %d", reply.XIndex))
							}
							hasTerm = true
							break
						}
						currentIndex -= 1
					}
					if hasTerm == false {
						rf.nextIndex[idx] = reply.XIndex
						if rf.nextIndex[idx] < 1 {
							panic(fmt.Sprintf("XIndex out of range: %d", reply.XIndex))
						}
					}
				} else if reply.XLen != -1 {
					rf.nextIndex[idx] = reply.XLen
					if rf.nextIndex[idx] < 1 {
						panic(fmt.Sprintf("XLen out of range: %d", reply.XIndex))
					}
				} else {
					rf.nextIndex[idx]--
				}
			}
		} else {
			rf.mu.Lock()
			defer rf.mu.Unlock()
		}
	case <-time.After(100 * time.Millisecond):
		rf.mu.Lock()
		defer rf.mu.Unlock()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentRole = Follower
	rf.voteReceived = 0
	rf.votedFor = -1
	rf.resetElectionTimeout()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.logs = make([]Entry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs) // Replace initialValue with the desired default value
		rf.matchIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.sendHeartbeats()
	return rf
}
