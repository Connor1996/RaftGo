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
	locker      []sync.Mutex

	// persistent state on all servers
	currentTerm int
	votedFor    int // candidateId that received vote in current term(or -1 if none)
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
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
	rf.persister.SaveRaftState(writeBuffer.Bytes())
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here.
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if (args.Term > rf.currentTerm && rf.isLeader == true) {
		log.Printf("args term is %v from server %v, server %vreturn to follow state", args.Term, args.CandidateId, rf.me)
		rf.isLeader = false
	}

	if (args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId))) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		//log.Printf("server %v vote for candidate %v in term %v", rf.me, args.CandidateId, rf.currentTerm, rf.isLeader)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	} else {
		//log.Printf("server %v vote for false in term %v", rf.me, rf.currentTerm)
		reply.VoteGranted = false
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	rf.persist()
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
	Term        int
	Success     bool
	CommitIndex int
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		//ignore since args term is stale
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	go func() {
		rf.heartBeatCh <- &args
	}()

	if rf.isLeader == false {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		reply.Term = rf.currentTerm

		if len(rf.log) - 1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevlogTerm {
			// follower contained entry matching prevLogIndex and prevLogTerm
			reply.Success = true
			// append new entries to the point where the leader and follower logs match
			rf.log = append(rf.log[ : args.PrevLogIndex + 1], args.Entries...)
			rf.persist()
			//log.Printf("server %v append log %v-%v in term %v", rf.me, args.PrevLogIndex + 1, args.PrevLogIndex + len(args.Entries), rf.currentTerm)
		} else {
			//log.Printf("server %v : %v %v", rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1])
			reply.Success = false
			var index int

			if len(rf.log) - 1 >= args.PrevLogIndex {
				index = args.PrevLogIndex
				for rf.log[index].Term == rf.log[args.PrevLogIndex].Term {
					if index == 0 {
						break
					}
					index--
				}
			} else {
				index = len(rf.log) - 1
			}

			reply.CommitIndex = index + 1
			return
		}

		// update commitIndex
		if (rf.commitIndex <= args.LeaderCommit) {
			if len(rf.log) - 1 < args.LeaderCommit {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		} else {
			reply.Success = true
			reply.Term = rf.currentTerm
			reply.CommitIndex = rf.commitIndex
			rf.persist()
			//log.Printf("server %v commit index %v is larger than leadercommit %v", rf.me, rf.commitIndex, args.LeaderCommit)
			return
		}

		for rf.lastApplied < rf.commitIndex {
			// apply to state machine
			rf.lastApplied++
			log.Printf("server %v commit %v: %v", rf.me, rf.lastApplied, rf.log[rf.lastApplied])
			rf.applyCh <- ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, nil}
		}

		rf.persist()
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader == false {
		return -1, -1, false
	}

	for _, entry := range rf.log {
		//if i > rf.commitIndex {
		//	break
		//}
		//// the command is ever committed
		if entry.Command == command {
			log.Print("equal--------", command)
			return -1, rf.currentTerm, true
		}
	}

	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.Sync(i)
	}
	log.Printf("%v start in leader %v, index %v, term %v\n", command, rf.me, len(rf.log) - 1, rf.currentTerm)

	return len(rf.log) - 1, rf.currentTerm, true
}

//
// leader use AppendEntries RPC to replicate log to all servers
//
func (rf *Raft) Sync(server int) (ok bool,term int) {
	rf.locker[server].Lock()

	if rf.isLeader == false {
		log.Printf("server %v is not leader any more ", rf.me)
		rf.locker[server].Unlock()
		return
	}

	lastLogIndex := len(rf.log) - 1
	var entries []LogEntry

	if rf.matchIndex[server] + 1 == rf.nextIndex[server] {
		// consistent
		if lastLogIndex >= rf.nextIndex[server] {
			entries = rf.log[rf.nextIndex[server] : ]
		} else {
			// nothing to send, namely, sending heartbeat
			//log.Printf("leader %v send heartbeat to server %v", rf.me, server)
		}
	} else {
		// haven't achieve consistency
		// send empty entries to find out the stage where follower and leader is
	}

	args := AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevlogTerm: rf.log[rf.nextIndex[server] - 1].Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.locker[server].Unlock()


	reply := new(AppendEntriesReply)
	if rf.sendAppendEntries(server, args, reply) == false {
		//log.Printf("leader %v append log to server %v failed", rf.me, server)
		return false, reply.Term
	}

	rf.locker[server].Lock()
	defer rf.locker[server].Unlock()

	if reply.Term > rf.currentTerm {
		// do nothing because leader is stale
		//log.Printf("leader %v is stale since reply from server %v term %v", rf.me, server, reply.Term)

		return true, reply.Term
	}

	if reply.Success {
		// update nextIndex and matchIndex for follower
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		if reply.CommitIndex > rf.commitIndex && rf.matchIndex[server] > rf.commitIndex {
			for rf.commitIndex < rf.matchIndex[server] {
				rf.commitIndex++
				log.Printf("smaller than, ------leader %v commit %v: %v", rf.me, rf.commitIndex, rf.log[rf.commitIndex])
				rf.applyCh <- ApplyMsg{rf.commitIndex, rf.log[rf.commitIndex].Command, false, nil}
			}
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1

	} else if reply.Term == rf.currentTerm {
		// decrement nextIndex and retry
		rf.nextIndex[server] = reply.CommitIndex
		//log.Printf("leader %v decrement nextIndex[%v] to %v",rf.me, server, rf.nextIndex[server])
	} else {
		//log.Print("success is false and reply.term < rf.currentTerm")
	}


	rf.Commit()
	return true, reply.Term
}

func (rf *Raft) Commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader == false {
		return
	}

	index := -1
	// find the first entry that apply in current term
	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term == rf.currentTerm {
			index = i
		} else if rf.log[i].Term < rf.currentTerm {
			break
		}
	}
	// there is no entry applied in current term
	// unsafe to commit
	if (index == -1) {
		return
	}

	// find the upper bound where
	// index > commitIndex, a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm
	upperBound := index
	for upperBound < len(rf.log) {
		count := 1 // the number of servers that have given entry(include itself)
		isSafe := false
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			if rf.matchIndex[i] >= upperBound {
				count++
				if count > len(rf.peers) / 2 {
					isSafe = true
					break
				}
			}
		}
		if !isSafe {
			break
		}
		upperBound++
	}
	upperBound--

	// for situation described in figure 8
	// there is a current term apply, but not committed
	// so previous term log still shouldn't be committed
	if upperBound < index {
		return
	}

	// update commmit index to upperbound
	for rf.commitIndex < upperBound {
		rf.commitIndex++
		log.Printf("leader %v commit %v: %v", rf.me, rf.commitIndex, rf.log[rf.commitIndex])
		rf.applyCh <- ApplyMsg{rf.commitIndex, rf.log[rf.commitIndex].Command, false, nil}
	}

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
	rf.heartBeatCh = make(chan *AppendEntriesArgs)
	rf.rander = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.locker = make([]sync.Mutex, len(rf.peers))

	//log.Print("init here")
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
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.votedFor = msg.LeaderId
			rf.persist()
			rf.mu.Unlock()
		case <- time.After(timeout):
			//log.Printf("server %v heartbeat timer timeout", rf.me)
			go rf.Election()
			return
		}
	}
}

func (rf *Raft) Election() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	//log.Printf("heartbeat timeout server %v issue a new election in term %v\n", rf.me, rf.currentTerm)
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
				//log.Printf("candidate %v request vote rpc call to server %v failed", rf.me, i)
			} else if reply.VoteGranted == true {
				//rf.mu.Lock()
				approveNum++
				if approveNum > len(rf.peers) / 2 {
					winSignal <- true
				}
				//rf.mu.Unlock()
			} else if reply.Term > rf.currentTerm {
				staleSignal <- reply
			}
		}(index)
	}


	select {
	case msg := <- rf.heartBeatCh:
		if msg.Term < rf.currentTerm {
			// receive stale heartbeat, just ignore
		} else {
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.votedFor = msg.LeaderId
			rf.isLeader = false
			rf.persist()
			rf.mu.Unlock()

			go rf.HeartBeatTimer()
			//log.Printf("candidate %v becomes follower\n", rf.me)
		}
		return
	case <- winSignal:
		rf.mu.Lock()
		rf.isLeader = true
		log.Printf("candidate %v becomes leader in term %v", rf.me, rf.currentTerm)
		// reinit volatile state for leader
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.BroadCastHeartBeat()
		return
	case reply := <- staleSignal:
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		//log.Printf("candidate %v is stale", rf.me)
		go rf.HeartBeatTimer()
		return
	case <- time.After(time.Duration(ELECTION_TIMEOUT_BASE + rf.rander.Intn(ELECTION_TIMEOUT_RANGE)) * time.Millisecond):
		go rf.Election()
		//log.Printf("candidate %v election timeout", rf.me)
		return
	}
}

func (rf *Raft) BroadCastHeartBeat() {
	interval := time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond

	for {
		if rf.isLeader == false {
			//log.Printf("call broadcast, but server %v is not a leader", rf.me)
			return
		}

		staleSignal := make(chan *AppendEntriesReply, len(rf.peers) - 1)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				reply := new(AppendEntriesReply)
				ok, term := rf.Sync(i)
				if ok && term > rf.currentTerm {
					staleSignal <- reply
				}
			}(i)

		}

		select {
		case reply := <- staleSignal:
			rf.mu.Lock()
			rf.isLeader = false
			//rf.nextIndex = nil
			//rf.matchIndex = nil
			log.Printf("leader %v know itself term %v is stale, return to follow state", rf.me, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()

			go rf.HeartBeatTimer()
			return
		case msg := <- rf.heartBeatCh:
			if msg.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.isLeader = false
				log.Printf("leader %v in term %v find a superior leader %v in term %v, return to follow state", rf.me, rf.currentTerm,
					msg.LeaderId, msg.Term)
				//rf.nextIndex = nil
				//rf.matchIndex = nil
				rf.currentTerm = msg.Term
				rf.votedFor = msg.LeaderId
				rf.persist()
				rf.mu.Unlock()
				go rf.HeartBeatTimer()
				return
			} else if msg.Term < rf.currentTerm {
				// msg is stale, ignore
				log.Print("receive from stale msg, it is not filitered")
			} else {
				if rf.isLeader == false {
					//log.Printf("server %v is not leader any more", rf.me)
				} else {
					// just ignore
					//log.Printf("leader %v in term %v broadcast, receive the same heartbeat term from server %v", rf.me, rf.currentTerm, msg.LeaderId)
				}
			}
		case <- time.After(interval):
			//log.Print("fuck")
		}

	}
}


