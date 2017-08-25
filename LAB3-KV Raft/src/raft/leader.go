package raft

import (
	"time"
//	"log"
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

	// take offset into account
	index += rf.lastIncludedIndex + 1

	// find the upper bound where
	// index > commitIndex, a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm
	upperBound := index
	for upperBound < len(rf.log) + rf.lastIncludedIndex + 1{
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
		// take offset into account
		rf.logger.Printf("leader %v commit %v: %v", rf.me, rf.commitIndex, rf.log[rf.commitIndex - rf.lastIncludedIndex - 1])
		rf.applyCh <- ApplyMsg{rf.commitIndex, rf.log[rf.commitIndex - rf.lastIncludedIndex - 1].Command, false, nil}
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

	rf.mu.Lock()
	// take offset into account
	lastLogIndex := len(rf.log) + rf.lastIncludedIndex
	var entries []LogEntry

	// if last log index >= nextIndex
	// send AppendEntries RPC with log entries starting at nextIndex
	// log.Printf("leader %v: nextIndex[%v]:%v, lastIncludedIndex:%v", rf.me, server, rf.nextIndex[server], rf.lastIncludedIndex)
	if lastLogIndex >= rf.nextIndex[server]  {
		// send snapshot to slow follower
		if rf.lastIncludedIndex >= rf.nextIndex[server] {
			rf.logger.Printf("leader %v find server %v slow, send snapshot", rf.me, server)

			args := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm: rf.lastIncludedTerm,
				Data: rf.persister.ReadSnapshot(),
			}
			reply := new(InstallSnapshotReply)
			if rf.sendInstallSnapshot(server, args, reply) {
				if reply.Term > rf.currentTerm {
					// response contains term T > currentTerm
					// set currentTerm = T, convert to follower
					rf.logger.Printf("leader %v in term %v receive InstallSnapshot response from server %v with higher term %v",
						rf.me, rf.currentTerm, server, reply.Term)
					rf.currentTerm = reply.Term
					rf.votedFor = server
					rf.role = FOLLOWER
					rf.persist()
					rf.staleSignal <- true

				}
			}
			rf.mu.Unlock()
			return
		} else {
			entries = rf.log[rf.nextIndex[server] - rf.lastIncludedIndex - 1: ]
		}
	} else {
		// nothing to send, namely sending heartbeat
		//rf.logger.Printf("leader %v send heartbeat to server %v", rf.me, server)
	}

	//rf.logger.Printf("PrevLogIndex: %v, length of log: %v in leader %v", rf.nextIndex[server] - 1, len(rf.log), rf.me)
	args := AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	// take offset into account
	// to support the AppendEntries consistency check for the first log entry following the snapshot
	if args.PrevLogIndex == rf.lastIncludedIndex  {
		args.PrevlogTerm = rf.lastIncludedTerm
	}
	rf.locker[server].Unlock()
	rf.mu.Unlock()

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
			rf.logger.Printf("leader %v heartbeat broadcast", rf.me)
		}

		//if rf.role != LEADER {
		//	debug.PrintStack()
		//	rf.logger.Fatalf("[ERROR] call broadcast, but server %v in term %v is not a leader", rf.me, rf.currentTerm)
		//}
	}
}

func (rf *Raft) DeleteOldEntries(lastIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIndex < rf.lastIncludedIndex {
		rf.logger.Printf("server %v already snapshot", rf.me)
		return
	}

	rf.logger.Printf("server %v delete old log %v-%v", rf.me, rf.lastIncludedIndex + 1, lastIndex)

	// update info
	rf.lastIncludedTerm = rf.log[lastIndex - rf.lastIncludedIndex - 1].Term
	rf.log = rf.log[lastIndex - rf.lastIncludedIndex : ]
	rf.logger.Printf("server %v update lastIncludedIndex to %v", rf.me, lastIndex)
	rf.lastIncludedIndex = lastIndex

	rf.persist()
	return
}