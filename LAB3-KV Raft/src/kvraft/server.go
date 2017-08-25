package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 1

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
	Type        string
	Key         string
	Value       string
	RequestId   int64
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

	persister *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	pendingChs  map[int64]chan bool
	marked      map[int64]bool
	data        map[string]string // linearizable data

}



func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// detect whether it is ever committed
	// in the case where rpc can not reply but log is committed before network failed

	kv.mu.Lock()
	if _, ok := kv.marked[args.RequestId]; ok{
		reply.Value = kv.data[args.Key]
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	operation := Op{"Get", args.Key, "", args.RequestId}
	finishCh := make(chan bool, 1)
	//DPrintf("kvserver %v: add pendingChs[%v]", kv.me, args.RequestId)
	kv.mu.Lock()
	kv.pendingChs[args.RequestId] = finishCh
	kv.mu.Unlock()
	_, term, isLeader := kv.rf.Start(operation)

	if !isLeader {
		kv.mu.Lock()
		delete(kv.pendingChs, args.RequestId)
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}



	for {
		select {
		case msg := <- finishCh:
			if msg {
				kv.mu.Lock()
				value, ok := kv.data[args.Key]
				kv.mu.Unlock()
				if ok {
					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
				}
			} else {
				reply.Err = "error"
			}
			DPrintf("kvserver %v return request: %v", kv.me, args)
			return
		case <- time.After(time.Duration(time.Second)):
			// handle the case in which a leader has called Start() for a client RPC,
			// but loses its leadership before the request is committed to the log
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader == false || currentTerm != term {
				reply.Err = ErrLoseLeader
				return
			}
			DPrintf("loop...")
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// detect whether it is ever committed
	// in the case where rpc can not reply but log is committed before network failed
	kv.mu.Lock()
	if _, ok :=kv.marked[args.RequestId]; ok {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	operation := Op{args.Op, args.Key, args.Value, args.RequestId}
	finishCh := make(chan bool, 1)
	kv.mu.Lock()
	kv.pendingChs[args.RequestId] = finishCh
	kv.mu.Unlock()
	DPrintf("kvserver %v: add pendingChs[%v]", kv.me, args.RequestId)
	_, term, isLeader := kv.rf.Start(operation)

	// detect whether it is leader or not
	if !isLeader {
		kv.mu.Lock()
		delete(kv.pendingChs, args.RequestId)
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}


	for {
		select {
		case msg := <-finishCh:
			if msg {
				reply.Err = OK
			} else {
				reply.Err = "error"
			}
			DPrintf("kvserver %v return request: %v", kv.me, args)
			return
		case <-time.After(time.Duration(time.Second)):
			// handle the case in which a leader has called Start() for a client RPC,
			// but loses its leadership before the request is committed to the log
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader == false || currentTerm != term {
				reply.Err = ErrLoseLeader
				return
			}
			DPrintf("loop...")
		}
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
// in order to allow Raft to garbage-collect its D if maxraftstate is -1,
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
	kv.persister = persister

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.pendingChs = make(map[int64]chan bool)
	kv.marked = make(map[int64]bool)

	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.ReceiveApply()

	return kv
}

func (kv *RaftKV) ReceiveApply() {
	var msg raft.ApplyMsg

	for {
		msg = <-kv.applyCh
		if msg.UseSnapshot {
			kv.readPersist(msg.Snapshot)
			continue
		}

		index, command := msg.Index, msg.Command.(Op)

		kv.mu.Lock()
		if _, ok := kv.marked[command.RequestId]; ok {
			//DPrint(kv.me, ": already finish operation", command)
		} else {
			if command.Type == "Put" {
				kv.data[command.Key] = command.Value
			} else if command.Type == "Append" {
				kv.data[command.Key] += command.Value
			}
			DPrintf("kvserver %v finish: %v", kv.me, command)

			kv.marked[command.RequestId] = true

			if ch, ok := kv.pendingChs[command.RequestId]; ok {
				ch <- true
				delete(kv.pendingChs, command.RequestId)
			}
		}
		kv.mu.Unlock()

		// check state size to make snapshot
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
			DPrintf("server %v making snapshot", kv.me)
			kv.persist()
			go kv.rf.DeleteOldEntries(index)
		}
		DPrintf("recevie loop...")
	}
}

//
// save previously persisted state
//
func (kv *RaftKV) persist() {
	writeBuffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writeBuffer)
	encoder.Encode(&kv.data)
	encoder.Encode(&kv.marked)
	kv.persister.SaveSnapshot(writeBuffer.Bytes())
}

//
// restore previously persisted state.
//
func (kv *RaftKV) readPersist(data []byte) {
	readBuffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(readBuffer)
	decoder.Decode(&kv.data)
	decoder.Decode(&kv.marked)
}