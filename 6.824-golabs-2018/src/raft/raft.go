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

import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//

type raftLogEntry struct {
	command interface{}
	term    int
}

const (
	STATE_LEADER    = 0
	STATE_FOLLOWER  = 1
	STATE_CANDIDATE = 2
)

const (
	ELECT_TIMER_STOP           = 0
	ELECT_TIMER_RECV_HEARTBEAT = 1
	ELECT_TIMER_START          = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []raftLogEntry
	state       int

	commitIndex int
	lastApplied int

	//for leader
	nextIndex  []int
	matchIndex []int

	chHeartBeat    chan int
	chElectTimer   chan int
	grantVotes     int
	failedRpcVotes int

	hbTimerCnt int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == STATE_LEADER)
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) switchToFollower(term int) {
	rf.currentTerm = term
	rf.state = STATE_FOLLOWER
	rf.grantVotes = 0
	rf.votedFor = -1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("%d reject %d vote request, because its term(%d) is larger than request term(%d)", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		goto end
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf("%d reject %d vote request, because it voted for %d in term %d", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm)
		goto end
	}

	if args.LastLogTerm == 0 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		goto end
	}

	/*
		if len(rf.log) == 0 {
			reply.VoteGranted = false
			goto end
		}
	*/

	rf.mu.Lock()
	if len(rf.log) > 0 {
		if rf.log[len(rf.log)-1].term > args.Term {
			reply.VoteGranted = false
		} else if rf.log[len(rf.log)-1].term < args.Term {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			if args.LastLogIndex >= len(rf.log)-1 {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		}
	} else {
		reply.VoteGranted = true
	}
	rf.mu.Unlock()

end:
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	currentState := rf.state
	if args.Term > currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.grantVotes = 0
		rf.failedRpcVotes = 0
		rf.mu.Unlock()
		//leader switch to follower, stop heartbeat timer
		if currentState == STATE_LEADER {
			rf.votedFor = -1
			DPrintf("%d switch to follwer because recv larger term(%d) vote request from %d", rf.me, args.Term, args.CandidateId)
			rf.chHeartBeat <- 0
			rf.chElectTimer <- ELECT_TIMER_START
		}
	} else {
		rf.mu.Unlock()
	}

}

//
//RequestAppendEntries RPC handler
//
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	DPrintf("node %d recv RequestAppendEntries from %d, term=%d", rf.me, args.LeaderId, args.Term)

	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		reply.Success = false
		DPrintf("node %d reject RequestAppendEntries from %d, because currentTerm(%d) > request term(%d)", rf.me, args.LeaderId, rf.currentTerm, args.Term)
	} else if rf.currentTerm < args.Term {
		reply.Success = true
		rf.currentTerm = args.Term
		state := rf.state
		rf.state = STATE_FOLLOWER
		rf.mu.Unlock()
		if state == STATE_LEADER {
			rf.chHeartBeat <- 0
			rf.chElectTimer <- ELECT_TIMER_START
			rf.votedFor = -1
			rf.failedRpcVotes = 0
			rf.grantVotes = 0
			DPrintf("%d switch to follwer because recv larger term(%d) AppendEntries request from %d", rf.me, args.Term, args.LeaderId)
		}

		if len(args.Entries) == 0 {
			rf.chElectTimer <- ELECT_TIMER_RECV_HEARTBEAT
			reply.Success = true
		}
	} else {
		if len(args.Entries) == 0 {
			rf.chElectTimer <- ELECT_TIMER_RECV_HEARTBEAT
			reply.Success = true
		} else {

		}
		rf.mu.Unlock()
	}
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
	if !ok {
		DPrintf("%d recv RequestVote reply from %d FAILED!", rf.me, server)
	}
	return ok
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	if !ok {
		DPrintf("%d recv AppendEntries reply from %d FAILED!", rf.me, server)
	}
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) Vote(id int, req *RequestVoteArgs, wg *sync.WaitGroup) {
	defer wg.Done()
	var reply RequestVoteReply
	DPrintf("node %d send vote to %d, state=%d", rf.me, id, rf.state)
	ok := rf.sendRequestVote(id, req, &reply)
	if !ok {
		DPrintf("node %d send vote rpc to node %d failed", rf.me, id)
		rf.mu.Lock()
		rf.failedRpcVotes++
		rf.mu.Unlock()
		return
	}

	DPrintf("node %d get vote resp from node %d:%v", rf.me, id, reply)

	if reply.VoteGranted {
		rf.mu.Lock()
		rf.grantVotes++
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.failedRpcVotes = 0
			rf.grantVotes = 0
			DPrintf("node %d switch from candidate to follower because recv larger term(%d) vote reply from %d", rf.me, reply.Term, id)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) BroadcastVoteRequest() {
	var req RequestVoteArgs

	DPrintf("node %d broad cast vote requests, state=%d", rf.me, rf.state)
	//switch to candidate
	rf.mu.Lock()
	rf.state = STATE_CANDIDATE
	rf.votedFor = rf.me
	rf.grantVotes = 1
	rf.failedRpcVotes = 0
	rf.currentTerm += 1

	req.CandidateId = rf.me
	req.Term = rf.currentTerm
	if len(rf.log) == 0 {
		req.LastLogTerm = 0
		req.LastLogIndex = 0
	} else {
		req.LastLogIndex = len(rf.log) - 1
		req.LastLogTerm = rf.log[len(rf.log)-1].term
	}
	rf.mu.Unlock()

	var wg sync.WaitGroup
	//send vote request to other nodes
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go rf.Vote(i, &req, &wg)
		}
	}

	//vote for all vote request reply
	wg.Wait()

	rf.mu.Lock()
	requireVotes := (len(rf.peers) - rf.failedRpcVotes) / 2
	if rf.grantVotes > requireVotes && rf.grantVotes > 1 {
		DPrintf("%d get majority votes, swith to leader, current term=%d", rf.me, rf.currentTerm)
		rf.state = STATE_LEADER
		rf.mu.Unlock()
		//stop election timer
		rf.chElectTimer <- ELECT_TIMER_STOP

		//send heartbeat upon election
		go rf.BroadcastAppendEntriesRequest()

		//start heartbeat timer
		//go rf.HeartbeatTimer()
		rf.chHeartBeat <- 1
	} else {
		rf.mu.Unlock()
	}

}

func (rf *Raft) AppendEntries(id int, req *RequestAppendEntriesArgs) {
	var reply RequestAppendEntriesReply
	DPrintf("node %d send append entries to %d", rf.me, id)
	ok := rf.sendRequestAppendEntries(id, req, &reply)
	if !ok {
		DPrintf("node %d recv append entries reply from %d FAILED! ", rf.me, id)
		return
	}

	DPrintf("node %d recv append entries reply from %d:%v ", rf.me, id, reply)
	if reply.Success {

	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			DPrintf("node %d swith to follower because append entry reply has larger term(%d)", rf.me, reply.Term)
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
			rf.failedRpcVotes = 0
			rf.grantVotes = 0
			rf.mu.Unlock()
			rf.chHeartBeat <- 0
			rf.chElectTimer <- ELECT_TIMER_START
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) BroadcastAppendEntriesRequest() {
	var req RequestAppendEntriesArgs

	DPrintf("node %d broad cast heartbeat, state=%d, hbtimer cnt=%d", rf.me, rf.state, rf.hbTimerCnt)

	rf.mu.Lock()
	req.LeaderCommit = rf.commitIndex
	req.Term = rf.currentTerm
	rf.mu.Unlock()
	req.LeaderId = rf.me
	//req.PrevLogTerm =
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.AppendEntries(i, &req)
		}
	}
}

func (rf *Raft) HeartbeatTimer() {
	var cmd int
	rf.hbTimerCnt++
	period := time.Millisecond * time.Duration(150)
	timer := time.NewTimer(period)
	timer.Stop()

	for {
		select {
		case <-timer.C:
			DPrintf("%d reset heartbeat timer", rf.me)
			timer.Reset(period)
			go rf.BroadcastAppendEntriesRequest()
		case cmd = <-rf.chHeartBeat:
			if cmd == 0 {
				DPrintf("%d stop heartbeat timer", rf.me)
				timer.Stop()
			} else {
				DPrintf("%d start heartbeat timer", rf.me)
				timer.Reset(period)
			}
		}
	}
	rf.hbTimerCnt--
}

func (rf *Raft) electTimer() {
	var cmd int

	electTimeout := 500 + rand.Intn(300)
	DPrintf("timeout:%d", electTimeout)
	period := time.Millisecond * time.Duration(electTimeout)
	timer := time.NewTimer(period)

	for {
		select {
		/* election timer expired, vote for self */
		case <-timer.C:
			DPrintf("node %d election timer expired", rf.me)
			timer.Reset(period)
			go rf.BroadcastVoteRequest()

		case cmd = <-rf.chElectTimer:
			//DPrintf("electTimer recv cmd:%d", cmd)
			if cmd == ELECT_TIMER_RECV_HEARTBEAT {
				// recv heartbeat from leader, reset timer
				DPrintf("node %d recv heartbeat, reset election timer", rf.me)
				timer.Reset(period)
			} else if cmd == ELECT_TIMER_STOP {
				//become leader, stop the election timer
				DPrintf("node %d stop election timer", rf.me)
				timer.Stop()
			} else {
				DPrintf("node %d restart election timer", rf.me)
				timer.Reset(period)
			}
		}
	}

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = STATE_FOLLOWER

	rf.hbTimerCnt = 0

	rf.chElectTimer = make(chan int)
	rf.chHeartBeat = make(chan int)

	go rf.electTimer()
	go rf.HeartbeatTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
