package raft

import (
	"time"
)

func (rf *Raft) Commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return
	}

	index := -1
	// find the first entry that append in current term
	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term == rf.currentTerm {
			index = i
		} else if rf.log[i].Term < rf.currentTerm {
			break
		}
	}
	// there is no entry appended in current term
	// unsafe to commit
	if (index == -1) {
		return
	}

	// find the upper bound where
	// index > commitIndex, a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm
	upperBound := index
	for upperBound < len(rf.log) {
		count := 1 // the number of servers that have given entry(include itself)
		isSafe := false
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			if rf.matchIndex[i] >= upperBound {
				count++
				if count > len(rf.peers) / 2 {
					isSafe = true
					break
				}
			}
		}
		if !isSafe {
			break
		}
		upperBound++
	}
	upperBound--

	// for situation described in figure 8
	// there is a current term append, but not committed
	// so previous term log still shouldn't be committed
	if upperBound < index {
		return
	}

	// update commmit index to upperbound
	for rf.commitIndex < upperBound {
		rf.commitIndex++
		rf.logger.Printf("leader %v commit %v: %v", rf.me, rf.commitIndex, rf.log[rf.commitIndex])
		rf.applyCh <- ApplyMsg{rf.commitIndex, rf.log[rf.commitIndex].Command, false, nil}
	}

}
//
// leader use AppendEntries RPC to replicate log to all servers
//
func (rf *Raft) Sync(server int) {
	rf.locker[server].Lock()

	if rf.role != LEADER {
		rf.logger.Printf("server %v is not leader any more ", rf.me)
		rf.locker[server].Unlock()
		return
	}

	lastLogIndex := len(rf.log) - 1
	var entries []LogEntry

	// if last log index >= nextIndex
	// send AppendEntries RPC with log entries starting at nextIndex
	if lastLogIndex >= rf.nextIndex[server] {
		entries = rf.log[rf.nextIndex[server] : ]
	} else {
		// nothing to send, namely sending heartbeat
		//rf.logger.Printf("leader %v send heartbeat to server %v", rf.me, server)
	}

	//rf.logger.Printf("PrevLogIndex: %v, length of log: %v in leader %v", rf.nextIndex[server] - 1, len(rf.log), rf.me)
	args := AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevlogTerm: rf.log[rf.nextIndex[server] - 1].Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.locker[server].Unlock()


	reply := new(AppendEntriesReply)
	//term := rf.currentTerm
	if rf.sendAppendEntries(server, args, reply) == false {
		if (len(args.Entries) != 0) {
			//rf.logger.Printf("leader %v append log to server %v failed in term %v", rf.me, server, term)
		}
		return
	} else if reply.Term < rf.currentTerm {
		// the reply is stale, just ignore
		return
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// response contains term T > currentTerm
			// set currentTerm = T, convert to follower

			rf.logger.Printf("leader %v in term %v receive AppendEntries response from server %v with higher term %v",
				rf.me, rf.currentTerm, server, reply.Term)
			rf.currentTerm = reply.Term
			rf.votedFor = server
			rf.role = FOLLOWER
			rf.persist()
			rf.staleSignal <- true
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	rf.locker[server].Lock()
	defer rf.locker[server].Unlock()

	if reply.Success {
		// update nextIndex and matchIndex for follower
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		//if reply.CommitIndex > rf.commitIndex && rf.matchIndex[server] > rf.commitIndex {
		//	for rf.commitIndex < rf.matchIndex[server] {
		//		rf.commitIndex++
		//		//rf.logger.Printf("smaller than, ------leader %v commit %v: %v", rf.me, rf.commitIndex, rf.log[rf.commitIndex])
		//		rf.applyCh <- ApplyMsg{rf.commitIndex, rf.log[rf.commitIndex].Command, false, nil}
		//	}
		//}

	} else {
		// fail because of log inconsistency, then decrement nextIndex and retry
		if (reply.ConflictIndex > 0) {
			rf.nextIndex[server] = reply.ConflictIndex
			rf.logger.Printf("update nextIndex to %v for server %v", rf.nextIndex[server], server)
		} else {
			// fail because of stale
		}

	}

	rf.Commit()
	return
}

func (rf *Raft) BroadCastHeartBeat() {
	interval := time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond

	for {

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				rf.Sync(i)
			}(i)

		}

		select {
		case  <- rf.staleSignal:
			rf.role = FOLLOWER
			rf.logger.Printf("leader %v convert to follower in term %v", rf.me, rf.currentTerm)
			go rf.HeartBeatTimer()
			return
		case <-time.After(interval):
		//rf.logger.Print("fuck")
		}

		//if rf.role != LEADER {
		//	debug.PrintStack()
		//	rf.logger.Fatalf("[ERROR] call broadcast, but server %v in term %v is not a leader", rf.me, rf.currentTerm)
		//}
	}
}
