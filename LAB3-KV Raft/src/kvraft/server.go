package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Value   string
}

type PendingOps struct {
	request Op
	channel chan bool
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	pendingOps  map[int]PendingOps
	data        map[string]string // linearizable data
}



func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{"GET", args.Key, ""})

	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	finishCh := make(chan bool)

	kv.pendingOps[index] = PendingOps{Op{"GET", args.Key, ""}, finishCh}
	select {
	case <- finishCh:
		value, ok := kv.data[args.Key]
		if ok {
			reply.Value = value
			reply.Err = ""
		} else {
			reply.Err = "the key doesn't exist"
		}
		return
	case <- time.After(time.Duration(time.Second)):
		reply.Err = "timeout"
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	index, term, isLeader := kv.rf.Start(Op{args.Op, args.Key, args.Value})

	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	finishCh := make(chan bool)

	kv.pendingOps[index] = PendingOps{Op{args.Op, args.Key, args.Value}, finishCh}

	select {
	case <- finishCh:
		reply.Err = ""
	case <- time.After(time.Duration(time.Second)):
		reply.Err = "timeout"
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int]PendingOps)

	go kv.ReceiveApply()

	return kv
}

func (kv *RaftKV) ReceiveApply() {
	var msg raft.ApplyMsg

	for {
		msg = <- kv.applyCh

		idx, _ := msg.Index, msg.Command.(Op)

		if op, ok := kv.pendingOps[idx]; !ok {
			//log.Printf("already finish pending operation index %v", idx)
			continue
		} else {
			//if command != op.request {
			//	log.Fatal("wrong command")
			//}
			if op.request.Type == "Put" {
				kv.data[op.request.Key] = op.request.Value
			} else if op.request.Type == "Append" {
				// if key does not exist, act like put
				kv.data[op.request.Key] += op.request.Value
			}
			delete(kv.pendingOps, idx)
			op.channel <- true
		}

	}
}