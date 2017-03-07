package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"log"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	recentLeader int
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
	var leaderIdx int
	if ck.recentLeader != -1 {
		leaderIdx = ck.recentLeader
	} else {
		leaderIdx = int(nrand() % int64(len(ck.servers)))
	}

	for {
		args := GetArgs{key}
		var reply GetReply
		// You will have to modify this function.
		if ck.servers[leaderIdx].Call("RaftKV.Get", &args, &reply) == false {
			leaderIdx = (leaderIdx + 1) % len(ck.servers)
			continue
		}

		if reply.WrongLeader == true {
			leaderIdx = (leaderIdx + 1) % len(ck.servers)
		} else {
			if reply.Err == "" {
				ck.recentLeader = leaderIdx
				return reply.Value
			} else {
				log.Print(reply.Err)
				return ""
			}
		}
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
	// You will have to modify this function.
	var leaderIdx int
	if ck.recentLeader != -1 {
		leaderIdx = ck.recentLeader
	} else {
		leaderIdx = int(nrand() % int64(len(ck.servers)))
	}

	for {
		args := PutAppendArgs{key, value, op}
		var reply PutAppendReply
		// You will have to modify this function.
		ck.servers[leaderIdx].Call("RaftKV.PutAppend", &args, &reply)

		if reply.WrongLeader == true {
			leaderIdx = (leaderIdx + 1) % len(ck.servers)
		} else {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
