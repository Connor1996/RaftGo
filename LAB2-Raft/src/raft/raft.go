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
	HEARTBEAT_INTERVAL int = 20 * 100000
	HEARTBEAT_TIMEOUT_BASE int = 150 * 1000000
	HEARTBEAT_TIMEOUT_RANGE int = 150 * 1000000
	ELECTION_TIMEOUT_BASE int = 150 * 1000000
	ELECTION_TIMEOUT_RANGE int = 150 * 1000000
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
	term    int
	command interface{}
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
	Term            int
	CandidateId     int
	LastLogIndex    int
	LastLogTerm     int
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
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		log.Printf("server %v don't vote for candidate %v for term %v", rf.me, args.CandidateId, rf.currentTerm)
		reply.VoteGranted = false
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		log.Printf("server %v don't vote since already voted for %v", rf.me, rf.votedFor)
		reply.VoteGranted = false
	} else if args.LastLogTerm < rf.currentTerm {
		log.Printf("server %v don't vote for lastlogterm", rf.me)
		reply.VoteGranted = false
	} else if args.LastLogIndex < rf.lastApplied {
		log.Printf("server %v don't vote for lastlogindex", rf.me)
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
	}

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
	Term            int
	LeaderId        int // so follower can redirect client
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []LogEntry
	LeaderCommit    int

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
		reply.Success = false
		reply.Term = rf.currentTerm
		return
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

	// Your initialization code here.
	rf.isLeader = false
	rf.applyCh = applyCh
	rf.heartBeatCh = make(chan *AppendEntriesArgs, 1)
	rf.rander = rand.New(rand.NewSource(time.Now().UnixNano()))

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
		rf.rander.Intn(HEARTBEAT_TIMEOUT_RANGE))

	ticker := time.NewTicker(timeout)
	for range ticker.C {
		select {
		case msg := <- rf.heartBeatCh:
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.mu.Unlock()
		default:
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

	log.Printf("new election begin in %v, term %v\n", rf.me, rf.currentTerm)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].term
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
				log.Fatal("wrong in rpc call")
			} else {
				if reply.VoteGranted == true {
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
			}
		}(index)
	}

	select {
	case msg := <- rf.heartBeatCh:
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
	case <- time.After(time.Duration(ELECTION_TIMEOUT_BASE + rf.rander.Intn(ELECTION_TIMEOUT_RANGE))):
		go rf.Election()
		log.Printf("follower %v timeout, become candidate\n", rf.me)
		return
	}
}

func (rf *Raft) BroadCastHeartBeat() {
	interval := time.Duration(HEARTBEAT_INTERVAL)

	ticker := time.NewTicker(interval)
	for range ticker.C {
		staleSignal := make(chan *AppendEntriesReply, 1)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				prevLogIndex := len(rf.log) - 1
				prevLogTerm := rf.log[prevLogIndex].term
				args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, []LogEntry{}, rf.commitIndex}
				reply := new(AppendEntriesReply)
				log.Printf("leader %v send heartbeat to server %v", rf.me, i)
				for !rf.sendAppendEntries(i, args, reply) {
					time.Sleep(time.Nanosecond * 5)
				}

				if reply.Term > rf.commitIndex {
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
			log.Println("%v receive a stale heartbeat")
			return
		case msg := <- rf.heartBeatCh:
			if msg.Term > rf.currentTerm {
				rf.isLeader = false
				rf.nextIndex = nil
				rf.matchIndex = nil
				rf.currentTerm = msg.Term
				rf.votedFor = msg.LeaderId
				return
			}
		default:
			continue
		}
	}
}


