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
	"os"
	"io/ioutil"
)

const (
	HEARTBEAT_INTERVAL int = 60
	HEARTBEAT_TIMEOUT_BASE int = 500
	HEARTBEAT_TIMEOUT_RANGE int = 500
	ELECTION_TIMEOUT_BASE int = 500
	ELECTION_TIMEOUT_RANGE int = 500
)

type Role int
const (
	FOLLOWER Role = 1 + iota
	CANDIDATE
	LEADER
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

type HeartBeatMsg struct {
	Term        int
	ServerId    int
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
	role        Role
	applyCh     chan ApplyMsg
	heartBeatCh chan HeartBeatMsg
	rander      *rand.Rand
	locker      []sync.Mutex

    // persistent state on all servers
	currentTerm         int
	votedFor            int     // candidateId that received vote in current term(or -1 if none)
	log                 []LogEntry
	lastIncludedIndex   int     // the index of the last entry in the log that the snapshot replace
	lastIncludedTerm    int     // the term of lastIncludedIndex

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex   []int // for each server, index of the next log entry to sent to that server
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server

	// signal when RPC request or response contains term T > currentTerm
	staleSignal chan bool

	logger      *log.Logger
}

type LogEntry struct {
	Term    int
	Command interface{}
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here.
	return rf.currentTerm, rf.role == LEADER
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
	encoder.Encode(&rf.lastIncludedIndex)
	encoder.Encode(&rf.lastIncludedTerm)
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
	decoder.Decode(&rf.lastIncludedIndex)
	decoder.Decode(&rf.lastIncludedTerm)
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

	//rf.logger.Printf("server %v receive request vote from candidate %v", rf.me, args.CandidateId)

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	// take offset into account
	lastLogIndex += rf.lastIncludedIndex + 1

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.logger.Printf("server %v in term %v deny for candidate %v for stale request of term %v",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// request contains term T > currentTerm
	// set currentTerm = T, convert to follower
	if (args.Term > rf.currentTerm) {
		rf.logger.Printf("server %v in term %v receive RequestVote RPC from server %v with higher term %v",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.role = FOLLOWER
		rf.staleSignal <- true
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// if voterFor is null or candidateId
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// and candidate's log is at least up-to-date as receiver's log
		// then grant vote
		if (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.heartBeatCh <- HeartBeatMsg{args.Term, args.CandidateId}
			rf.logger.Printf("server %v vote for candidate %v in term %v", rf.me, args.CandidateId, rf.currentTerm)
		} else {
			rf.logger.Printf("server %v deny for candidate %v for log's out of date", rf.me, args.CandidateId)
			rf.logger.Printf("args.LastLogIndex: %v, lastLogIndex: %v", args.LastLogIndex, lastLogIndex)
		}
	} else {
		rf.logger.Printf("server %v deny for candidate %v cause already voting", rf.me, args.CandidateId)
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
	ConflictIndex int // when success is false, return the first index for the term of the conflicting entry for optimization
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// take offset into account
	args.PrevLogIndex -= rf.lastIncludedIndex + 1

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		return
	}

	//if rf.role == LEADER && args.Term == rf.currentTerm {
	//	rf.logger.Fatal("leader %v: receive from other leader %v in same term %v", rf.me, args.LeaderId, rf.currentTerm)
	//}

	// request contains term T > currentTerm
	// set currentTerm = T, convert to follower
	if (args.Term > rf.currentTerm) {
		rf.logger.Printf("server %v in term %v receive AppendEntries RPC from server %v with higher term %v",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.role = FOLLOWER
		rf.persist()
		rf.staleSignal <- true
	}

	go func() {
		rf.heartBeatCh <- HeartBeatMsg{args.Term, args.LeaderId}
	}()

	reply.Term = rf.currentTerm
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm

	// support for consistency check for the first log entry following the snapshot
	if args.PrevLogIndex == -1 {
		if rf.lastIncludedTerm == args.PrevlogTerm {
			// follower contained entry matching prevLogIndex and prevLogTerm
			reply.Success = true
		} else {
			rf.logger.Fatal("inconsistent between snapsnot")
		}
	}else if args.PrevLogIndex < -1 {
		rf.logger.Printf("server %v in term %v: %v...%v....%v...%v... %v",
			rf.me, rf.currentTerm, len(rf.log), args.PrevLogIndex, args.PrevlogTerm, rf.lastIncludedTerm, rf.lastIncludedIndex)

	} else if len(rf.log) - 1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevlogTerm {
		// follower contained entry matching prevLogIndex and prevLogTerm
		reply.Success = true
	} else {
		reply.Success = false
	}


	if !reply.Success {
		// find the first index for the term of the conflicting entry
		if len(rf.log) - 1 >= args.PrevLogIndex {
			index := args.PrevLogIndex
			for rf.log[index].Term == rf.log[args.PrevLogIndex].Term {
				if index == 0 {
					index = -1
					break
				}
				index--
			}
			reply.ConflictIndex = index + 1
		} else {
			reply.ConflictIndex = len(rf.log)
		}
		// take offset into account
		reply.ConflictIndex += rf.lastIncludedIndex + 1
		rf.logger.Printf("PrevLogIndex:%v, PrevLogTerm:%v, lastIncludedTerm:%v", args.PrevLogIndex, args.PrevlogTerm, rf.lastIncludedTerm)
		rf.logger.Printf("server %v reply false, conflictIndex: %v", rf.me, reply.ConflictIndex)
	} else {
		// delete the existing entry conflicting with a new one and all that follow it
		for i := 1; i <= len(args.Entries); i++ {
			// append any new entries not already in log
			if args.PrevLogIndex + i > len(rf.log) - 1 {
				rf.log = append(rf.log, args.Entries[i - 1 : ]...)
				rf.persist()
				// take offset into account
				rf.logger.Printf("server %v append log %v-%v in term %v from leader %v, prevLogIndex: %v",
					rf.me, args.PrevLogIndex + i + rf.lastIncludedIndex + 1, len(rf.log) + rf.lastIncludedIndex, rf.currentTerm,
					args.LeaderId, args.PrevLogIndex + rf.lastIncludedIndex + 1)

				break
			}
			if rf.log[args.PrevLogIndex + i].Term != args.Entries[i - 1].Term {
				// append any new entries not already in log
				// take offset into account
				rf.logger.Printf("server %v delete log %v-%v",
					rf.me, args.PrevLogIndex + i + rf.lastIncludedIndex + 1, len(rf.log) + rf.lastIncludedIndex)
				rf.log = append(rf.log[ : args.PrevLogIndex + i], args.Entries[i - 1: ]...)
				rf.persist()
				rf.logger.Printf("server %v append log %v-%v in term %v from leader %v, prevLogIndex: %v",
					rf.me, args.PrevLogIndex + i + rf.lastIncludedIndex + 1, len(rf.log) + rf.lastIncludedIndex, rf.currentTerm, args.LeaderId, args.PrevLogIndex)

				break
			}
		}

		// update commitIndex
		if (args.LeaderCommit > rf.commitIndex) {
			// take offset into account
			if len(rf.log) + rf.lastIncludedIndex < args.LeaderCommit {
				rf.commitIndex = len(rf.log) + rf.lastIncludedIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}

		// increment lastApplied, apply log[lastApplied] to state machine
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			// take offset into account
			rf.applyCh <- ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied - rf.lastIncludedIndex - 1].Command, false, nil}
			rf.logger.Printf("server %v commit %v: %v", rf.me, rf.lastApplied, rf.log[rf.lastApplied - rf.lastIncludedIndex - 1])
		}
	}

	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term                int
	LeaderId            int // so follower can redirect client
	LastIncludedIndex   int
	LastIncludedTerm    int
	Data                []byte // little change from paper, not split into chunck
}

type InstallSnapshotReply struct {
	Term        int
	Success     bool
}

//
// InstallSnapshot RPC handler
//
func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}


	rf.logger.Printf("server %v receive InstallSnapshot RPC", rf.me)

	// request contains term T > currentTerm
	// set currentTerm = T, convert to follower
	if (args.Term > rf.currentTerm) {
		rf.logger.Printf("server %v in term %v receive InstallSnapshot RPC from server %v with higher term %v",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.role = FOLLOWER
		rf.persist()
		rf.staleSignal <- true
	}


	reply.Term = rf.currentTerm
	if rf.lastIncludedIndex > args.LastIncludedIndex {
		// stale snapshot
		reply.Success = false
		return
	}

	go func() {
		rf.heartBeatCh <- HeartBeatMsg{args.Term, args.LeaderId}
	}()

	rf.persister.SaveSnapshot(args.Data)
	rf.applyCh <- ApplyMsg{0, nil, true, args.Data}
	// if existing log entry has same index and term as snapshot's last included index
	// retain log entries following it and reply
	//rf.logger.Printf("...%v....%v", args.LastIncludedIndex, rf.lastIncludedIndex)
	if args.LastIncludedIndex - rf.lastIncludedIndex - 1 < len(rf.log)  &&
		args.LastIncludedIndex - rf.lastIncludedIndex - 1 >= 0 &&
		rf.log[args.LastIncludedIndex - rf.lastIncludedIndex - 1].Term == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex - rf.lastIncludedIndex : ]
	} else {
		// discard the entrie log
		rf.log = make([]LogEntry, 0)
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	reply.Success = true
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's rf.logger. if this
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

	if rf.role != LEADER {
		return -1, -1, false
	}

	// for the situation where lose leadership before commit the log
	// when client re-send the request to the server that append the old log of the request
	// we shouldn't simply the append new log cause old log will be commit and apply as well (namely, apply one request twice)
	// for the purpose of avoiding Figure 8, we also can not commit the old log when no new log append in current term
	// so we update the term of the old log, then it will be committed
	for index := rf.commitIndex + 1; index < len(rf.log); index++ {
		if rf.log[index].Command == command {
			rf.log[index].Term = rf.currentTerm
			return index, rf.currentTerm, true
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
	// take offset into account
	log.Printf("%v start in leader %v, index %v, term %v\n", command, rf.me, len(rf.log) + rf.lastIncludedIndex, rf.currentTerm)

	return len(rf.log) - 1, rf.currentTerm, true
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

	// init logger
	rf.logger = log.New(os.Stdout, "", log.LstdFlags)
	discard := false
	if (discard) {
		rf.logger.SetOutput(ioutil.Discard)
	}
	// Your initialization code here.
	rf.role = FOLLOWER
	rf.applyCh = applyCh
	rf.heartBeatCh = make(chan HeartBeatMsg, 1)
	rf.rander = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.locker = make([]sync.Mutex, len(rf.peers))

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = 0

	// insert a fake entry in the index 0
	rf.log = append(rf.log, LogEntry{0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	// allocate it only in leader state
	rf.nextIndex = nil
	rf.matchIndex = nil

	rf.staleSignal = make(chan bool, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.HeartBeatTimer()

	return rf
}







