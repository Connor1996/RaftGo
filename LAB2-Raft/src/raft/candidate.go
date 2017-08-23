package raft

import (
	"log"
	"time"
)

func (rf *Raft) Election() {
	// turn into candidate
	// increment currentTerm
	// vote for self
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	log.Printf("server %v issue a new election in term %v\n", rf.me, rf.currentTerm)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}

	// signal when winning election
	winSignal := make(chan bool, 1)

	// vote for itself
	approveNum := 1
	// send requestVote RPC to all other servers
	go func() {
		for index := range rf.peers {
			if index == rf.me {
				continue
			}

			go func(index int) {
				reply := new(RequestVoteReply)
				term := rf.currentTerm
				if rf.sendRequestVote(index, args, reply) == false {
					//log.Printf("candidate %v request vote rpc call to server %v failed in term %v", rf.me, index, term)
				} else if rf.currentTerm == term {
					if reply.VoteGranted == true {
						log.Printf("candidate %v get server %v's vote", rf.me, index)
						approveNum++
						// received from majority of servers: become leader
						if approveNum == len(rf.peers) / 2 + 1{
							winSignal <- true
						}
					} else {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							// response contains term T > currentTerm, convert to follower
							rf.currentTerm = reply.Term
							rf.votedFor = index
							rf.role = FOLLOWER
							rf.persist()
							rf.staleSignal <- true
						}
						rf.mu.Unlock()
					}

				}
			}(index)

		}
	}()


	select {
	case msg := <- rf.heartBeatCh:
		if msg.Term < rf.currentTerm {
			// the heartbeat msg is stale
			// log.Print("AppendEntries Handler fail to ingore stale heartbeat")
		} else {
			// received from new leader: convert to follower
			log.Printf("received from new leader %v: convert to follower", msg.ServerId)
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.votedFor = msg.ServerId
			rf.role = FOLLOWER
			rf.persist()
			rf.mu.Unlock()
			log.Printf("candidate %v convert to follower", rf.me)
			go rf.HeartBeatTimer()
		}
		return
	case <- winSignal:
		rf.mu.Lock()
		rf.role = LEADER
		log.Printf("candidate %v becomes leader in term %v", rf.me, rf.currentTerm)

		// reinit volatile state for leader
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.BroadCastHeartBeat()
		return
	case <- rf.staleSignal:
		rf.role = FOLLOWER
		log.Printf("candidate %v convert to follower in term %v", rf.me, rf.currentTerm)
		go rf.HeartBeatTimer()
		return
	case <- time.After(time.Duration(ELECTION_TIMEOUT_BASE + rf.rander.Intn(ELECTION_TIMEOUT_RANGE)) * time.Millisecond):
		// election timeout elapses: start new election
		go rf.Election()
		return
	}
}
