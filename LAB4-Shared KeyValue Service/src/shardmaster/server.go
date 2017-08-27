package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"time"
	"log"
	"os"
	"io/ioutil"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	pendingChs  map[int64]chan bool
	marked      map[int64]bool

	logger      *log.Logger
}


type Op struct {
	// Your data here.
	Type        string
	Servers     map[int][]string
	GIDs        []int
	Shard       int
	GID         int
	Num         int
	RequestId   int64
}

type PendingOps struct {
	request Op
	channel chan bool
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	reply.WrongLeader, reply.Err = sm.AppendOp(Op{
		Type: "Join",
		Servers: args.Servers,
		RequestId: args.RequestId,
	})
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader, reply.Err = sm.AppendOp(Op{
		Type: "Leave",
		GIDs: args.GIDs,
		RequestId: args.RequestId,
	})
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	reply.WrongLeader, reply.Err = sm.AppendOp(Op{
		Type: "Move",
		GID: args.GID,
		Shard: args.Shard,
		RequestId: args.RequestId,
	})
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	reply.WrongLeader, reply.Err = sm.AppendOp(Op{
		Type: "Query",
		Num: args.Num,
		RequestId: args.RequestId,
	})
	num := args.Num
	if num == -1 || num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs) - 1]
	} else {
		reply.Config = sm.configs[num]
	}

	return
}

func (sm *ShardMaster) AppendOp(op Op) (wrongLeader bool, err Err){
	sm.mu.Lock()
	if _, ok :=sm.marked[op.RequestId]; ok {
		wrongLeader = false
		err = OK
		sm.mu.Unlock()
		return
	}

	finishCh := make(chan bool, 1)
	sm.pendingChs[op.RequestId] = finishCh
	sm.mu.Unlock()

	_, term, isLeader := sm.rf.Start(op)

	// detect whether it is leader or not
	if !isLeader {
		sm.mu.Lock()
		delete(sm.pendingChs, op.RequestId)
		sm.mu.Unlock()
		wrongLeader = true
		sm.logger.SetOutput(ioutil.Discard)
		return
	} else {
		wrongLeader = false
		sm.logger.SetOutput(os.Stdout)
	}


	for {
		select {
		case msg := <-finishCh:
			if msg {
				err = OK
			} else {
				err = "error"
			}
			sm.logger.Printf("smserver %v return request: %v", sm.me, op)
			return
		case <-time.After(time.Duration(time.Second)):
		// handle the case in which a leader has called Start() for a client RPC,
		// but loses its leadership before the request is committed to the log
			currentTerm, isLeader := sm.rf.GetState()
			if isLeader == false || currentTerm != term {
				err = ErrLoseLeader
				return
			}
		}
	}
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

// needed by shardsm tester
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

	// init logger
	sm.logger = log.New(os.Stdout, "", log.LstdFlags)

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.pendingChs = make(map[int64]chan bool)
	sm.marked = make(map[int64]bool)

	// Your code here.
	go sm.ReceiveApply()

	return sm
}


func (sm *ShardMaster) ReceiveApply() {
	for {
		msg := <-sm.applyCh
		sm.logger.Printf("smserver %v: receive applyCh %v", sm.me, msg)
		// ignore the first fake log
		if msg.Command == nil {
			continue;
		}
		_, command := msg.Index, msg.Command.(Op)


		if _, ok := sm.marked[command.RequestId]; ok {
			sm.logger.Printf("smserver %v: already finish command %v", sm.me, command)
		} else {
			if command.Type == "Join" {
				sm.ApplyJoin(command.Servers)
			} else if command.Type == "Leave" {
				sm.ApplyLeave(command.GIDs)
			} else if command.Type == "Move" {
				sm.ApplyMove(command.Shard, command.GID)
			} else if command.Type == "Query" {
				// do nothing
			}
			sm.mu.Lock()
			if ch, ok := sm.pendingChs[command.RequestId]; ok {
				sm.logger.Printf("smserver %v finish: %v", sm.me, command)
				ch <- true
				delete(sm.pendingChs, command.RequestId)
			}

			sm.marked[command.RequestId] = true
			sm.mu.Unlock()
		}

	}
}

func (sm *ShardMaster) ApplyJoin(servers map[int][]string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// create a new configuration
	newGroups := make(map[int][]string)
	for key, slice := range sm.configs[len(sm.configs) - 1].Groups {
		// range is read-only, no need to copy slice
		newGroups[key] = slice
	}
	// add new GID -> servers mappings
	for gid, servers := range servers {
		newGroups[gid] = servers
	}
	// calculate avg amount of shard in each group
	avg := NShards / len(newGroups)
	remain := NShards % len(newGroups)
	distributeCh := make(chan int, NShards)

	// the assignment of array is copy not refer
	newShards := sm.configs[len(sm.configs) - 1].Shards
	count := make(map[int]int, 0)
	for _, gid := range newShards {
		count[gid]++
	}
	for gid, sum := range count {
		if sum < avg {
			for i := 1; i <= avg - sum; i++ {
				distributeCh <- gid
			}
		}
	}
	for gid := range servers {
		for i := 1; i <= avg; i++ {
			distributeCh <- gid
		}
	}

	cnt := remain
	for gid, sum := range count {
		if sum < avg {
			if cnt > 0 {
				distributeCh <- gid
			} else {
				break
			}
			cnt--
		}
	}
	for gid := range servers {
		if cnt > 0 {
			distributeCh <- gid
		} else {
			break
		}
		cnt--
	}

	count = make(map[int]int, 0)
	for shard, gid := range newShards {
		if gid != 0 {
			count[gid]++
		}
		if count[gid] <= avg && gid != 0 {

		} else if count[gid] == avg + 1 && remain > 0 && gid != 0{
			remain--
		} else {
			// this shard should be distribute to new group
			newShards[shard] = <-distributeCh
		}
	}
	sm.configs = append(sm.configs, Config{len(sm.configs), newShards, newGroups})
	sm.logger.Printf("join%v---old config: %v\n new config: %v",servers, sm.configs[len(sm.configs) - 2], sm.configs[len(sm.configs) - 1])
	return
}

func (sm *ShardMaster) ApplyLeave(gids []int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.logger.Print(sm.configs[len(sm.configs) - 1])
	// create a new configuration
	newGroups := make(map[int][]string)
	for key, slice := range sm.configs[len(sm.configs) - 1].Groups {
		// range is read-only, no need to copy slice
		newGroups[key] = slice
	}
	// delete GID from groups mappings
	for _, gid := range gids {
		delete(newGroups, gid)
	}
	// calculate avg amount of shard in each group
	avg := NShards / len(newGroups)
	remain := NShards % len(newGroups)
	distributeCh := make(chan int, NShards)

	// the assignment of array is copy not refer
	newShards := sm.configs[len(sm.configs) - 1].Shards
	count := make(map[int]int, 0)
	for gid := range newGroups {
		count[gid] = 0
	}

	for _, gid := range newShards {
		if _, ok := newGroups[gid]; ok {
			count[gid]++
		}
	}
	for gid, cnt := range count {
		for i := 1; i <= avg - cnt; i++ {
			distributeCh <- gid
		}
		if remain > 0 {
			distributeCh <- gid
			remain--
		}
	}

	for shard, gid := range newShards {
		if _, ok := newGroups[gid]; !ok {
			newShards[shard] = <- distributeCh
		}
	}
	sm.configs = append(sm.configs, Config{len(sm.configs), newShards, newGroups})
	sm.logger.Printf("leave%v---old config: %v\n new config: %v", gids, sm.configs[len(sm.configs) - 2], sm.configs[len(sm.configs) - 1])

	return
}

func (sm *ShardMaster) ApplyMove(shard int, gid int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// create a new configuration
	newGroups := make(map[int][]string)
	for key, slice := range sm.configs[len(sm.configs) - 1].Groups {
		//newSlice := make([]string, 0)
		//copy(newSlice, slice)
		// range is read-only, no need to copy slice
		newGroups[key] = slice
	}

	// the assignment of array is copy not refer
	newShards := sm.configs[len(sm.configs) - 1].Shards
	newShards[shard] = gid

	sm.configs = append(sm.configs, Config{len(sm.configs), newShards, newGroups})
	sm.logger.Printf("Move---old config: %v, new config: %v", sm.configs[len(sm.configs) - 2], sm.configs[len(sm.configs) - 1])

	return
}
