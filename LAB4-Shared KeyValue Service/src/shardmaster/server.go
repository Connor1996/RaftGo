package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"kvraft"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
}


func (sm *ShardMaster) Join(args JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// create a new configuration
	newGroups := make(map[int][]string)
	for key, slice := range sm.configs[len(sm.configs) - 1].Groups {
		newSlice := make([]string, 0)
		copy(newSlice, slice)
		newGroups[key] = newSlice
	}
	// add new GID -> servers mappings
	for gid, servers := range args.Servers {
		newGroups[gid] = servers
		// create new group
		raftkv.StartKVServer(sm.)
	}

	// calculate avg amount of shard in each group
	avg := NShards / len(newGroups)
	remain := NShards % len(newGroups)
	distributeCh := make(chan int, NShards)
	for gid := range args.Servers {
		for i := 1; i <= avg; i++ {
			distributeCh <- gid
		}
	}
	cnt := remain
	for gid := range args.Servers {
		if cnt > 0 {
			distributeCh <- gid
		}
		cnt--
	}


	var newShards [NShards]int
	copy(newShards, sm.configs[len(sm.configs) - 1].Shards)
	count := make(map[int]int, 0)
	for shard, gid := range newShards {
		count[gid]++
		if count[gid] == avg + 1 {
			remain--
		} else {
			// this shard should be distribute to new group
			newShards[shard] = <-distributeCh
			// TO DO
		}
	}

	sm.configs = append(sm.configs, Config{len(sm.configs), newShards, newGroups})



}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
