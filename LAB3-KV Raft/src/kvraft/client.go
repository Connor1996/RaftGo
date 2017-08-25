package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers         []*labrpc.ClientEnd
	// You will have to modify this struct.
	recentLeader    int
	mu              sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.recentLeader = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	var leaderIdx int
	if ck.recentLeader != -1 {
		leaderIdx = ck.recentLeader
	} else {
		leaderIdx = int(nrand() % int64(len(ck.servers)))
	}

	args := GetArgs{key, nrand()}
	for {
		var reply GetReply
		// You will have to modify this function.
		DPrintf(": get %v rpc to server %v", args, leaderIdx)
		if ck.servers[leaderIdx].Call("RaftKV.Get", &args, &reply) == false {
			// retry
			//DPrint("get rpc false")
		} else if reply.WrongLeader == false {
			ck.recentLeader = leaderIdx
			if reply.Err == OK {
				DPrintf("clinet get reply %v", args)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("clinet get reply %v with no key", args)
				return ""
			} else {
				DPrintf("[ERROR] %v", reply.Err)
			}
		}
		leaderIdx = (leaderIdx + 1) % len(ck.servers)
		//DPrint("loop get", args)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You will have to modify this function.
	var leaderIdx int
	if ck.recentLeader != -1 {
		leaderIdx = ck.recentLeader
	} else {
		leaderIdx = int(nrand() % int64(len(ck.servers)))
	}
	args := PutAppendArgs{key, value, op, nrand()}
	for {
		var reply PutAppendReply
		// You will have to modify this function.
		DPrintf(": put append %v rpc to server %v", args, leaderIdx)
		if ck.servers[leaderIdx].Call("RaftKV.PutAppend", &args, &reply) == false {

		} else if reply.WrongLeader == false {
			ck.recentLeader = leaderIdx
			if reply.Err == OK {
				DPrintf("clinet get reply %v", args)
				return
			} else {
				DPrintf("[ERROR] %v", reply.Err)
			}
		}
		leaderIdx = (leaderIdx + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
