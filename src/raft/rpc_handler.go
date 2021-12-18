package raft

//
// example RequestVote RPC handler.
// 这个函数应该指的是收到 RequestVote 的服务器，如何回复
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	term := args.Term
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm

	if term < currentTerm {
		reply.VoteGranted = false
	} else if term == currentTerm {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			// 本次 term 已经投过票了
			reply.VoteGranted = false
		}
	} else { // term > currentTerm
		rf.currentTerm = term
		// 可能需要切换角色
		if rf.role != "follower" {
			rf.role = "follower"
			rf.resetElectionTimer(randDuration(electionTimeoutLower, electionTimeoutUpper))
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	//DPrintf("%d receives RequestVote. Reply with: voteGranted: %t; Term: %d", rf.me, reply.VoteGranted, reply.Term)
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	term := args.Term
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm
	if term < currentTerm {
		reply.Success = false
		return
	}
	if len(args.Entries) > 0 {
		DPrintf("%d receive log from leader, index: %d", rf.me, args.PrevLogIndex+1)
		found, entryIndex := rf.findMatchingLogEntry(args.PrevLogIndex, args.PrevLogTerm)
		if !found {
			reply.Success = false
		} else {
			if entryIndex != -1 {
				// Conflict Log Entry, delete the entry and all following it
				rf.log = rf.log[:entryIndex]
			}
			// append log entries
			// TODO：是否需要考虑跟随者已经有了一部分 Log
			rf.log = append(rf.log, args.Entries...)
			for i := rf.lastLogIndex + 1; i < len(rf.log); i++ {
				rf.applyCh <- ApplyMsg{
					Index:       i,
					Command:     rf.log[i].Command,
					UseSnapshot: false,
					Snapshot:    []byte{},
				}
			}
			rf.lastLogIndex = len(rf.log) - 1
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			}
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied = rf.commitIndex

			}
			DPrintf("follower %d has received log index: %d", rf.me, args.PrevLogIndex+1)
			reply.Success = true
		}
	}

	rf.currentTerm = term
	if rf.role == "candidate" || rf.role == "leader" {
		// receive AppendEntries from leader
		rf.role = "follower"
	}
	rf.electionTimer.Reset(randDuration(electionTimeoutLower, electionTimeoutUpper))
	DPrintf("%d reply with term: %d, success: %t\n", rf.me, term, reply.Success)
	return
}

//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// simply send AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
