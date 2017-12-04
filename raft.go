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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

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

//state
const (
	LEADER int = iota
	CANDIATE
	FOLLOWER
)

type LogData struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int
	voteNumber    int
	chanHeartbeat chan bool
	chanLeader    chan bool
	chanCommit    chan bool

	currentTerm int
	votedFor    int
	logs        []LogData
	//volatile state on all servers
	commitIndex int
	lastApplied int
	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.state == LEADER
}
func (rf *Raft) getLastLogTerm() int {
	end := len(rf.logs) - 1
	if end < 0 {
		return 0
	}
	return rf.logs[end].LogTerm
}
func (rf *Raft) getLastLogIndex() int {
	end := len(rf.logs) - 1
	if end < 0 {
		return 0
	}
	return rf.logs[end].LogIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
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
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here.
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogData
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm < args.Term {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	flag := false
	localLogTerm := rf.getLastLogTerm()
	localLogIndex := rf.getLastLogIndex()
	if localLogTerm < args.LastLogTerm || ((localLogTerm == args.LastLogTerm) && (localLogIndex <= args.LastLogIndex)) {
		flag = true
	}
	if flag && rf.votedFor == -1 {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) BroadcastVoteRequest() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIATE {
			go rf.SingleVoteRequest(i, args)
		}
	}
}

func (rf *Raft) SingleVoteRequest(i int, args RequestVoteArgs) {
	var reply RequestVoteReply
	rf.sendRequestVote(i, args, &reply)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.chanHeartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	startIndex := rf.logs[0].LogIndex
	if args.PreLogIndex > startIndex && rf.logs[args.PreLogIndex-startIndex].LogTerm != args.PreLogTerm {
		return
	}
	if args.PreLogIndex >= startIndex {
		rf.logs = append(rf.logs[:args.PreLogIndex-startIndex+1], args.Entries...)
		reply.Success = true
	}
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := rf.getLastLogIndex()
		if lastIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		rf.chanCommit <- true
	}
	rf.persist()

}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//log.Println("reply %d,%d", reply.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if ok {
		if rf.state != LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			return ok
		}
		if reply.Success && len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
			rf.matchIndex[server]++
		} else {
			rf.nextIndex[server]--
		}
	}

	return ok
}

func (rf *Raft) BroadcastHeartbeat() {

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			go func(i int) {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PreLogIndex = rf.nextIndex[i] - 1
				args.PreLogTerm = rf.logs[args.PreLogIndex-rf.logs[0].LogIndex].LogTerm
				args.Entries = make([]LogData, len(rf.logs)-(args.PreLogIndex-rf.logs[0].LogIndex+1))
				copy(args.Entries, rf.logs[args.PreLogIndex-rf.logs[0].LogIndex+1:])
				args.LeaderCommit = rf.commitIndex
				var reply AppendEntriesReply
				rf.SendAppendEntries(i, args, &reply)
			}(i)
		}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		} else if args.Term == rf.currentTerm && rf.state == CANDIATE && reply.VoteGranted == true {
			rf.voteNumber++
			if rf.voteNumber > len(rf.peers)/2 {
				rf.state = LEADER
				rf.chanLeader <- true
			}
		}
	}
	rf.persist()
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.logs = append(rf.logs, LogData{LogTerm: term, LogIndex: index, Command: command})
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

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.voteNumber = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.chanHeartbeat:
				case <-time.After(time.Duration(rand.Int63()%200+300) * time.Millisecond):
					//log.Printf("candinate", rf.me)
					rf.mu.Lock()
					rf.state = CANDIATE
					rf.persist()
					rf.mu.Unlock()
				}
			case CANDIATE:
				rf.mu.Lock()
				rf.votedFor = rf.me
				rf.voteNumber = 1
				rf.currentTerm++
				rf.persist()
				rf.mu.Unlock()
				go rf.BroadcastVoteRequest()
				select {
				case <-rf.chanHeartbeat:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.persist()
					rf.mu.Unlock()
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.persist()
					rf.mu.Unlock()

				case <-time.After(time.Duration(rand.Int63()%150+150) * time.Millisecond):

				}
			case LEADER:
				rf.BroadcastHeartbeat()
				time.Sleep(time.Millisecond * 50)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				startIndex := rf.logs[0].LogIndex
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyCh <- ApplyMsg{Index: i, Command: rf.logs[i-startIndex].Command}
					rf.lastApplied = i
				}
				rf.mu.Unlock()

			}
		}
	}()
	return rf
}
