package raft

import (
	"time"
)

//
// expect receive a heartbeat in a given interval
// otherwise it will issue a election
//
func (rf *Raft) HeartBeatTimer() {
	for {

		select {
		case <- rf.staleSignal:
			continue
		case msg := <- rf.heartBeatCh:
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.votedFor = msg.ServerId
			rf.persist()
			rf.mu.Unlock()
		case <- time.After(time.Duration(HEARTBEAT_TIMEOUT_BASE +
			rf.rander.Intn(HEARTBEAT_TIMEOUT_RANGE)) * time.Millisecond):
			go rf.Election()
			return
		}

		//if rf.role != FOLLOWER {
		//	debug.PrintStack()
		//
		//	if rf.role == CANDIDATE {
		//		rf.logger.Fatalf("[ERROR] %v call heartBeatTimer, but I'm not a follower but a candidate", rf.me)
		//	} else {
		//		rf.logger.Fatalf("[ERROR] %v call heartBeatTimer, but I'm not a follower but a leader", rf.me)
		//	}
		//}
	}
}