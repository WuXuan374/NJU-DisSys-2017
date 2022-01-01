package raft

//
// example RequestVote RPC handler.
// 这个函数应该指的是收到 RequestVote 的服务器，如何回复
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	term := args.Term
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm

	//DPrintf("follower %d with currentTerm: %d, args.Term: %d", rf.me, currentTerm, term)

	if term < currentTerm {
		reply.VoteGranted = false
	} else if term == currentTerm {
		if rf.votedFor == -1 {
			// 检查 if candidate is as up-to-date as receiver
			upToDate := candidateUpToDate(rf, args.LastLogIndex, args.LastLogTerm)
			//DPrintf("args.LastLogIndex: %d, args.LastLogTerm: %d, candidateUpToDate: %t", args.LastLogIndex, args.LastLogTerm, upToDate)
			if upToDate {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		} else {
			// 本次 term 已经投过票了
			reply.VoteGranted = false
		}
	} else {
		rf.currentTerm = term
		rf.votedFor = -1
		// 可能需要切换角色
		if rf.role != "follower" {
			rf.role = "follower"
			rf.resetElectionTimer(randDuration(electionTimeoutLower, electionTimeoutUpper))
		}
		upToDate := candidateUpToDate(rf, args.LastLogIndex, args.LastLogTerm)
		//DPrintf("args.LastLogIndex: %d, args.LastLogTerm: %d, candidateUpToDate: %t", args.LastLogIndex, args.LastLogTerm, upToDate)
		if upToDate {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
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
	reply.Success = true // 赋一个初值，后面可以改的
	if term < currentTerm {
		reply.Success = false
		//DPrintf("%d's currentTerm is: %d, args.Term is: %d", rf.me, currentTerm, args.Term)
		DPrintf("%d reply with false, because term < currentTerm", rf.me)
		return
	}

	// 日志相关的检查，更新操作
	//DPrintf("%d receive log from leader %d, length: %d", rf.me, args.LeaderId, len(args.Entries))
	match, conflictIndex := rf.findMatchingLogEntry(args.PrevLogIndex, args.PrevLogTerm)
	if !match {
		reply.Success = false
		DPrintf("%d reply with false, because did not find match log", rf.me)
		//DPrintf("args.PrevLogIndex: %d, args.PrevLogTerm: %d, %d has %d logs, conflictIndex: %d ", args.PrevLogIndex, args.PrevLogTerm, rf.me, len(rf.log), conflictIndex)
		if conflictIndex != -1 {
			// -1 代表不含这个日志条目
			// 非 -1: 出现矛盾的日志条目，应该删除这个条目及之后的条目
			rf.log = rf.log[:conflictIndex]
			reply.ConflictIndex = conflictIndex
		}
	} else {
		// 前一个日志条目匹配
		// append log entries
		// TODO：是否需要考虑跟随者已经有了一部分 Log
		for _, item := range args.Entries {
			if !contains(rf.log, item) {
				rf.log = append(rf.log, item)
				rf.lastLogIndex = len(rf.log) - 1
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		DPrintf("follower %d update commitIndex to %d", rf.me, rf.commitIndex)
	}

	if !reply.Success {
		//DPrintf("%d reply with success: false, conflictLogIndex: %d", rf.me, reply.ConflictIndex)
	}
	rf.currentTerm = term
	if rf.role == "candidate" || rf.role == "leader" {
		// receive AppendEntries from leader
		rf.role = "follower"
	}
	rf.electionTimer.Reset(randDuration(electionTimeoutLower, electionTimeoutUpper))
	//DPrintf("%d reply with term: %d, success: %t\n", rf.me, reply.Term, reply.Success)
	// apply log while commitIndex > lastApplied, do it on background
	rf.applyLog()
}

func (rf *Raft) InstallLogs(args AppendEntriesArgs, reply *AppendEntriesReply) {
	term := args.Term
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm
	reply.Success = true // 赋一个初值，后面可以改的
	DPrintf("%d receive InstallLogs", rf.me)
	if term < currentTerm {
		reply.Success = false
		//DPrintf("%d's currentTerm is: %d, args.Term is: %d", rf.me, currentTerm, args.Term)
		//DPrintf("%d reply with false, because term < currentTerm", rf.me)
		return
	}
	rf.log = args.Entries
	rf.lastLogIndex = len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	reply.LastMatchedIndex = rf.lastLogIndex
	DPrintf("follower %d update commitIndex to %d", rf.me, rf.commitIndex)
	rf.applyLog()
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

func (rf *Raft) sendInstallLogs(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.InstallLogs", args, reply)
	return ok
}
