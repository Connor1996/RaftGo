package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"time"
	"bytes"
	"log"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck          *shardmaster.Clerk
	pendingChs  map[int64]chan bool
	marked      map[int64]bool
	data        map[string]string // linearizable data
	shards      map[int]bool // the list of shards which server is responsible for

	persister   *raft.Persister
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	shard := key2shard(args.Key)
	if have, ok := kv.shards[shard]; have && ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.marked[args.RequestId]; ok{
		reply.Value = kv.data[args.Key]
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	//DPrintf("kvserver %v: add pendingChs[%v]", kv.me, args.RequestId)
	finishCh := make(chan bool, 1)
	kv.pendingChs[args.RequestId] = finishCh
	kv.mu.Unlock()

	operation := Op{"Get", args.Key, "", args.RequestId}
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
			DPrintf("loop...%v", args)
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	shard := key2shard(args.Key)
	if have, ok := kv.shards[shard]; have && ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if _, ok :=kv.marked[args.RequestId]; ok {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	//DPrintf("kvserver %v: add pendingChs[%v]", kv.me, args.RequestId)
	finishCh := make(chan bool, 1)
	kv.pendingChs[args.RequestId] = finishCh
	kv.mu.Unlock()

	operation := Op{args.Op, args.Key, args.Value, args.RequestId}
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
			DPrintf("loop... %v", args)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.pendingChs = make(map[int64]chan bool)
	kv.marked = make(map[int64]bool)
	kv.shards = make(map[int]bool)

	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.ReceiveApply()
	go kv.DetectChange()

	return kv
}

func (kv *ShardKV) DetectChange() {
	for {
		config := kv.mck.Query(-1)
		kv.mu.Lock()
		for shard, gid := range config.Shards {
			if gid == kv.me && kv.shards[shard] == false {
				// need recevie the shard from $gid
				// If a replica group gains a shard,
				// it needs to wait for the previous owner to send over the old shard data
				// before accepting requests for that shard.
				// TO DO
			} else if gid != kv.me && kv.shards[shard] == true {
				// need send the shard to $gid
				// If a replica group loses a shard,
				// it must stop serving requests to keys in that shard immediately,
				kv.shards[shard] = false
				// and start migrating the data for that shard to the replica group that is taking over ownership
				kv.DoMigration(shard, config.Groups[gid])
			}

			// Make sure that all servers in a replica group do the migration at the same time
			// so that they all either accept or reject concurrent client requests.
			// TO DO
		}
		kv.mu.Unlock()

		time.After(time.Duration(100 * time.Millisecond))
	}
}

// send shard number $shard to group $gid
func (kv *ShardKV) DoMigration(shard int, serverNames []string) {
	// TO DO

	for _, name := range serverNames {
		args := DoMigrationArgs {

		}
		var reply DoMigrationReply
		kv.sendDoMigration(name, args, reply)
	}
}


func (kv *ShardKV) sendDoMigration(serverName string, args DoMigrationArgs, reply *DoMigrationReply) {
	ok := kv.make_end(serverName).Call("ShardKV.DoMigration", args, reply)
	return ok
}

func (kv *ShardKV) MigrateShard() {
	// TO DO
}

func (kv *ShardKV) ReceiveApply() {
	var msg raft.ApplyMsg

	for {
		msg = <-kv.applyCh
		DPrintf("kvserver %v recevie applyCh %v", kv.me, msg)
		if msg.UseSnapshot {
			kv.readPersist(msg.Snapshot)
			continue
		}

		// ignore the first fake log
		if msg.Command == nil {
			continue;
		}
		index, command := msg.Index, msg.Command.(Op)

		kv.mu.Lock()
		if _, ok := kv.marked[command.RequestId]; ok {
			DPrintf("kvserver %v: already finish command %v", kv.me, command)
		} else {
			if command.Type == "Put" {
				kv.data[command.Key] = command.Value
			} else if command.Type == "Append" {
				kv.data[command.Key] += command.Value
			}

			if ch, ok := kv.pendingChs[command.RequestId]; ok {
				DPrintf("kvserver %v finish: %v", kv.me, command)
				ch <- true
				delete(kv.pendingChs, command.RequestId)
			}

			kv.marked[command.RequestId] = true

		}
		kv.mu.Unlock()

		// check state size to make snapshot
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
			DPrintf("server %v making snapshot", kv.me)
			kv.persist()
			go kv.rf.DeleteOldEntries(index)
		}

	}
}

//
// save previously persisted state
//
func (kv *ShardKV) persist() {
	writeBuffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writeBuffer)
	encoder.Encode(&kv.data)
	encoder.Encode(&kv.marked)
	kv.persister.SaveSnapshot(writeBuffer.Bytes())
}

//
// restore previously persisted state.
//
func (kv *ShardKV) readPersist(data []byte) {
	readBuffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(readBuffer)
	decoder.Decode(&kv.data)
	decoder.Decode(&kv.marked)
}