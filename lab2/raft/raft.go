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
	"lab2/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term int
}

type ServerState int

const (
	Leader ServerState = iota
	Follower 
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	state ServerState

	lastHeartbeatTime time.Time

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	//persistent state on all servers
	currentTerm int
	votedFor int      //-1 if voted for none, assuming this is the index within the peers array
	log []LogEntry
	
	//volatile state on all servers
	commitIndex int
	lastApplied int
	
	//volatile state on leader
	nextIndex []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	
	term = rf.currentTerm
	isleader = rf.state == Leader
	
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!

//the candidate wants to recieve the vote from this server
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (rf *Raft) getLastLogInfo() (int, int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// will cause a deadlock, since the calling function also holds the lock

	logLen := len(rf.log)
	if logLen == 0 {
		return -1, 0
	}
	return rf.log[logLen - 1].Term, logLen
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	
	//the only conditions are for the candidate to not lag behind the receiving server

	/*
	This is the reference in the Raft paper:
		"Raft determines which of two logs is more up-to-date
		by comparing the index and term of the last entries in the
		logs. If the logs have last entries with different terms, then
		the log with the later term is more up-to-date. If the logs
		end with the same term, then whichever log is longer is
		more up-to-date."
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	candidateLastLogTerm := args.LastLogTerm
	receiverLastLogTerm, logLen := rf.getLastLogInfo()

	if (args.Term < rf.currentTerm) {
		reply.VoteGranted = false
	}

	if ((candidateLastLogTerm < receiverLastLogTerm) || (candidateLastLogTerm == receiverLastLogTerm && args.LastLogIndex + 1 < logLen)) {
		reply.VoteGranted = false
	}

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	prevLogIndex int     //index of log entry immediately preceding new ones
	prevLogTerm int       //term of the prevLogIndex entry
	entries []LogEntry
	leaderCommit int    //informs the followers as to which entries have been successfully replicated to
	//a majority of followers 
}

type AppendEntriesReply struct {
	Term int        //currentTerm, for leader to update itself
	Success bool      
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//for 2A, its just using AppendEntries as a heartbeat
	//this might be a point of error as other conditions must be handled
	if (args.Term < rf.currentTerm) {
		reply.Success = false
	}else if (args.Term > rf.currentTerm) {
		rf.currentTerm = args.Term  //set the receiver node's current term to the leader's current term, 
	//this is the part which does the clock sync
	}
	//other checks here, not needed for now

	reply.Success = true
	rf.state = Follower      //after node has received the hearbeat, it becomes a follower

	rf.lastHeartbeatTime = time.Now()
	 
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.currentTerm = 0
	rf.votedFor = -1      
	rf.log = make([]LogEntry, 0)
	rf.state = Follower
	rf.lastHeartbeatTime = time.Now()
	
	rf.commitIndex = 0
	rf.lastApplied = 0
	
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for (!rf.killed()) {
			timeout := 500 + rand.Float64() * (800 - 500)
			time.Sleep(time.Duration(timeout))

			rf.mu.Lock()

			//if current node is not already a leader and time passed since last heartbeat is greater or
			//equal to the timeout defined for this iteration, then *election* happens, so change state of this node to
			//a candidate, increment currentTerm
			//ELECTION BELOW!!
			if rf.state != Leader && time.Since(rf.lastHeartbeatTime) >= time.Duration(timeout) {
				rf.currentTerm += 1
				rf.state = Candidate

				//now this node must vote for itself and request its peers to vote too
				//to be done now
				rf.votedFor = rf.me
				rf.lastHeartbeatTime = time.Now()

				lastLogTerm, logLen := rf.getLastLogInfo()

				reqVoteArgs := RequestVoteArgs {
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: logLen-1,
					LastLogTerm: lastLogTerm,
				}

				//go through all the peers and invoke RequestVote on them
				votes := 1     //the candidate voted for itself
				
				for peerIndex := range rf.peers {
					if peerIndex != rf.me {
						go func(peerIndex int) {
							reqVoteReply := RequestVoteReply {}
							rf.sendRequestVote(peerIndex, &reqVoteArgs, &reqVoteReply)
							
							rf.mu.Lock()
							//if the current node (which acts as the candidate) becomes a follower due to
							//it getting a RequestVote reply from a leader, then the condition for election
							//does not hold, and hence the below state change logic must not execute

							//if reply from a node has a higher term than this one, then this cannot be the leader
							if reqVoteReply.Term > rf.currentTerm {
								rf.state = Follower
								rf.votedFor = -1
								rf.currentTerm = reqVoteReply.Term     //let this candidate catch up
							}

							if rf.state != Candidate || rf.currentTerm != reqVoteReply.Term {
								rf.mu.Unlock()
								return
							}

							if reqVoteReply.VoteGranted {
								votes += 1
								if votes > len(rf.peers)/2 {
									rf.state = Leader

									//TODO: leader now starts the hearbeat goroutine
								}
							}
							rf.mu.Unlock()
						}(peerIndex)
					}
				}
			}

			rf.mu.Unlock()
		}
	}()

	return rf
}
