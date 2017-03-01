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
import (
	"labrpc"
//	"debug/elf"
)

import "bytes"
import (
	"encoding/gob"
	"time"
	"math/rand"
	"log"
)

const (
	HEARTBEAT_INTERVAL int = 60
	HEARTBEAT_TIMEOUT_BASE int = 150
	HEARTBEAT_TIMEOUT_RANGE int = 150
	ELECTION_TIMEOUT_BASE int = 150
	ELECTION_TIMEOUT_RANGE int = 150
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
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	isLeader    bool
	applyCh     chan ApplyMsg
	heartBeatCh chan *AppendEntriesArgs
	rander      *rand.Rand

	// persistent state on all servers
	currentTerm int
	votedFor    int // candidateId that received vote in current term(or null if none)
	log         []LogEntry

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex   []int // for each server, index of the next log entry to sent to that server
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server
}

type LogEntry struct {
	Term    int
	Command interface{}
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {


	// Your code here.
	return rf.currentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	writeBuffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writeBuffer)
	encoder.Encode(&rf.currentTerm)
	encoder.Encode(&rf.votedFor)
	encoder.Encode(&rf.log)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	readBuffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(readBuffer)
	decoder.Decode(&rf.currentTerm)
	decoder.Decode(&rf.votedFor)
	decoder.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int // currentTerm, for candidate to update itself
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	deny := false

	if args.Term < rf.currentTerm {
		// candidate's term is stale
		deny = true
	} else if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm &&
		args.LastLogIndex < lastLogIndex) {
		deny = true
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 {
		deny = true
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
	}

	reply.VoteGranted = !deny
	reply.Term = rf.currentTerm
	return
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
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int // so follower can redirect client
	PrevLogIndex int
	PrevlogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		log.Printf("server %v in term %v appendentries receive but ignore since arg's term is %v",
			rf.me, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term >= rf.currentTerm && rf.isLeader == false {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
	}
	rf.heartBeatCh <- &args
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true


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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your initialization code here.
	rf.isLeader = false
	rf.applyCh = applyCh
	rf.heartBeatCh = make(chan *AppendEntriesArgs)
	rf.rander = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	// insert a fake entry in the first log
	rf.log = append(rf.log, LogEntry{0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	// allocate it only in leader state
	rf.nextIndex = nil
	rf.matchIndex = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.HeartBeatTimer()

	return rf
}

//
// expect receive a heartbeat in a given interval
// otherwise it will issue a election
//
func (rf *Raft) HeartBeatTimer() {
	timeout := time.Duration(HEARTBEAT_TIMEOUT_BASE +
		rf.rander.Intn(HEARTBEAT_TIMEOUT_RANGE)) * time.Millisecond

	for {
		select {
		case msg := <- rf.heartBeatCh:
			log.Printf("server %v receive heartbeat", rf.me)
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.votedFor = msg.LeaderId
			rf.mu.Unlock()
		case <- time.After(timeout):
			go rf.Election()
			return
		}
	}
}

func (rf *Raft) Election() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	log.Printf("heartbeat timeout server %v issue a new election in term %v\n", rf.me, rf.currentTerm)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}

	// signal when winning election
	winSignal := make(chan bool, 1)
	// signal when receiving reply from server having a higher term
	staleSignal := make(chan *RequestVoteReply, 1)

	// vote for itself
	approveNum := 1
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// send requestVote RPC in parallel
		go func(i int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(i, args, reply) == false {
				log.Print("rpc call failed and retry")
			} else if reply.VoteGranted == true {
				log.Printf("server %v vote for candidate %v", i, rf.me)
				rf.mu.Lock()
				approveNum++
				if approveNum > len(rf.peers) / 2 {
					winSignal <- true
				}
				rf.mu.Unlock()
			} else if reply.Term > rf.currentTerm {
				staleSignal <- reply
			}
		}(index)
	}


	select {
	case msg := <- rf.heartBeatCh:
		//log.Printf("candidate %v receive heartbeat from leader %v", rf.me, msg.LeaderId)
		if msg.Term < rf.currentTerm {
			// receive stale heartbeat, just ignore
		} else {
			rf.currentTerm = msg.Term
			rf.votedFor = msg.LeaderId
			rf.isLeader = false
			go rf.HeartBeatTimer()
			log.Printf("candidate %v becomes follower\n", rf.me)
		}
		return
	case <- winSignal:
		rf.isLeader = true

		// reinit volatile state for leader
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		log.Printf("candidate %v becomes leader in term %v", rf.me, rf.currentTerm)
		go rf.BroadCastHeartBeat()
		return
	case reply := <- staleSignal:
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.isLeader = false
		go rf.HeartBeatTimer()
		return
	case <- time.After(time.Duration(ELECTION_TIMEOUT_BASE + rf.rander.Intn(ELECTION_TIMEOUT_RANGE)) * time.Millisecond):
		go rf.Election()
		log.Printf("candidate %v election timeout, start a new election\n", rf.me)
		return
	}
}

func (rf *Raft) BroadCastHeartBeat() {
	interval := time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond

	for {
		staleSignal := make(chan *AppendEntriesReply, len(rf.peers) - 1)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				prevLogIndex := len(rf.log) - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				args := AppendEntriesArgs {
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PrevLogIndex: prevLogIndex,
					PrevlogTerm: prevLogTerm,
					Entries: make([]LogEntry, 0),
					LeaderCommit: rf.commitIndex,
				}
				reply := new(AppendEntriesReply)
				log.Printf("leader %v send heartbeat to server %v in term %v", rf.me, i, rf.currentTerm)
				if rf.sendAppendEntries(i, args, reply) == false {
					log.Print("rpc send heartbeat failed")
				}

				if reply.Term > rf.currentTerm {
					log.Printf("leader %v know itself is stale, return to follow state", rf.me)
					staleSignal <- reply
				}
			}(i)

		}

		select {
		case reply := <- staleSignal:
			rf.isLeader = false
			rf.nextIndex = nil
			rf.matchIndex = nil
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			log.Printf("leader %v know itself is stale, return to follow state", rf.me)
			go rf.HeartBeatTimer()
			return
		case msg := <- rf.heartBeatCh:
			if msg.Term > rf.currentTerm {
				log.Printf("leader %v in term %v find a superior leader %v in term %v", rf.me, rf.currentTerm,
					msg.LeaderId, msg.Term)
				rf.isLeader = false
				rf.nextIndex = nil
				rf.matchIndex = nil
				rf.currentTerm = msg.Term
				rf.votedFor = msg.LeaderId
				go rf.HeartBeatTimer()
				return
			} else if msg.Term < rf.currentTerm {
				continue
			} else {
				log.Fatalf("leader %v in term %v broadcast, receive the same heartbeat term from server %v", rf.me, rf.currentTerm, msg.LeaderId)
			}
		case <- time.After(interval):
			//log.Print("fuck")
			continue
		}
	}
}


