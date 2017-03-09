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
	if kv.marked[args.RequestId] == true {
		reply.Value = kv.data[args.Key]
		reply.Err = ""
		log.Print("already commit", args)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	operation := Op{"Get", args.Key, "", args.RequestId}
	index, term, isLeader := kv.rf.Start(operation)

	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}

	kv.mu.Lock()
	if index == -1 {
		reply.Value = kv.data[args.Key]
		reply.Err = ""
		kv.mu.Unlock()
		return
	}
	finishCh := make(chan bool, 1)
	kv.pendingChs[args.RequestId] = finishCh
	kv.mu.Unlock()

	for {
		select {
		case msg := <- finishCh:
			if msg {
				kv.mu.Lock()
				value, ok := kv.data[args.Key]
				kv.mu.Unlock()
				if ok {
					reply.Value = value
				} else {
					reply.Value = ""
				}
				reply.Err = ""
			} else {
				reply.Err = "lose leadership"
			}
			return
		case <- time.After(time.Duration(time.Second)):
			// handle the case in which a leader has called Start() for a client RPC,
			// but loses its leadership before the request is committed to the log
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader == false || currentTerm != term {
				log.Print("lose leadership")
				reply.Err = "lose leadership"
				return
			}
			log.Print(kv.me,": timeout put append ", args)
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// detect whether it is ever committed
	// in the case where rpc can not reply but log is committed before network failed
	kv.mu.Lock()
	if kv.marked[args.RequestId] == true {
		log.Print("already commit", args)
		reply.Err = ""
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	operation := Op{args.Op, args.Key, args.Value, args.RequestId}
	index, term, isLeader := kv.rf.Start(operation)

	// detect whether it is leader or not
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}

	kv.mu.Lock()
	if index == -1 {
		reply.Err = ""
		kv.mu.Unlock()
		return
	}
	finishCh := make(chan bool, 1)
	kv.pendingChs[args.RequestId] = finishCh
	kv.mu.Unlock()

	for {
		select {
		case msg := <-finishCh:
			//log.Print("finish ", args, kv.me)
			if msg {
				reply.Err = ""
			} else {
				reply.Err = "lose leadership"
			}
			return
		case <-time.After(time.Duration(time.Second)):
			// handle the case in which a leader has called Start() for a client RPC,
			// but loses its leadership before the request is committed to the log
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader == false || currentTerm != term {
				log.Print("lose leadership")
				reply.Err = "lose leadership"
				return
			}
			log.Print(kv.me,": timeout put append ", args)
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
	kv.pendingChs = make(map[int64]chan bool)
	kv.marked = make(map[int64]bool)

	go kv.ReceiveApply()

	return kv
}

func (kv *RaftKV) ReceiveApply() {
	var msg raft.ApplyMsg

	for {
		msg = <-kv.applyCh
		_, command := msg.Index, msg.Command.(Op)

		kv.mu.Lock()
		if kv.marked[command.RequestId] == true {
			log.Print(kv.me, ": already finish operation", command)
		} else {
			if command.Type == "Put" {
				kv.data[command.Key] = command.Value
			} else if command.Type == "Append" {
				kv.data[command.Key] += command.Value
			}
			kv.marked[command.RequestId] = true
			if _, isLeader := kv.rf.GetState(); isLeader {
				if ch, ok := kv.pendingChs[command.RequestId]; ok {
					ch <- true
				}
			}
		}
		kv.mu.Unlock()
	}
			//} else if op, ok := kv.pendingOps[idx]; !ok {
		//	//log.Printf("already finish pending operation index %v", idx)
		//} else if command != op.request {
		//	log.Print("!!!!!!!!!!!!")
		//	op.channel <- false
		//} else {
		//	if op.request.Type == "Put" {
		//		kv.data[op.request.Key] = op.request.Value
		//		log.Print(kv.me, "---put: ", op.request.Key, op.request.Value)
		//	} else if op.request.Type == "Append" {
		//		// if key does not exist, act like put
		//		kv.data[op.request.Key] += op.request.Value
		//		log.Print(kv.me, "---append: ", op.request.Key, op.request.Value)
		//	}
		//	delete(kv.pendingOps, idx)
		//	kv.marked[command.RequestId] = true
		//	op.channel <- true
		//}
		//kv.mu.Unlock()

}