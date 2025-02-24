package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

// 一系列常量定义
const heartbeatDuration = 50
const electionTimeoutLower = 150
const electionTimeoutUpper = 300
const checkMatchIndexTimeout = 50
const campaignTimeout = 100

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here. 需要加锁
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == "leader"
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// Candidate 收集投票信息; 以及 Leader 收集 AppnedEntries 的回复
// 通过 Channel 将 reply 传回。 若 RPC 成功， Channel 包含了 Term 和 VoteGrandted
// 若 RPC 失败， reply 则应该传回 出错信息，以及失败的是哪台服务器
//
func (rf *Raft) collectRequestVote(server int, args RequestVoteArgs, replyCh chan<- RequestVoteReply) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		// 添加错误信息，和服务器编号
		reply.Err, reply.Server = true, server
	}
	replyCh <- reply
}

func (rf *Raft) collectAppendEntries(server int, args AppendEntriesArgs, replyCh chan<- AppendEntriesReply) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		reply.Err = true
	}
	replyCh <- reply
	if reply.Err {
		// RPC 通信失败，重发HeartBeat
		go rf.collectAppendEntries(reply.Server, args, replyCh)
		return
	} else if reply.Term > rf.currentTerm {
		// 发现更大的 Term, 变成 follower
		rf.returnToFollower(reply.Term)
		return
	}
	if reply.Success {
		//for i := 0; i < len(args.Entries); i++ {
		//	rf.replicateVote[args.PrevLogIndex+i+1] += 1
		//
		//}
		// 更新 nextIndex 和 matchIndex
		rf.matchIndex[server] = reply.LastMatchedIndex

		//DPrintf("Leader %d receive replication vote from %d\n", rf.me, reply.Server)
		return
	} else {
		DPrintf("Install log from %d to %d", rf.me, server)
		conflictIndex := reply.ConflictIndex
		go rf.installLogs(server, conflictIndex)
		return
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	currentTerm, isLeader := rf.GetState()
	if !isLeader {
		index := 0
		return index, currentTerm, isLeader
	} else {
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{
			LogTerm: rf.currentTerm,
			Command: command,
		})
		rf.lastLogIndex += 1
		//rf.replicateVote = append(rf.replicateVote, 0)
		DPrintf("leader: %d receive log from client, index: %d, latestLog: %d, commitIndex: %d",
			rf.me, rf.lastLogIndex, len(rf.log)-1, rf.commitIndex)
		rf.mu.Unlock()
		go rf.RealAppendEntries()
		return rf.lastLogIndex, currentTerm, isLeader
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) Campaign() {
	// Your code here, if desired.
	// Start an election
	rf.electionTimer.Reset(randDuration(electionTimeoutLower, electionTimeoutUpper))
	//rf.currentTerm
	rf.role = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me

	//DPrintf("Candidate %d start an election. Term: %d\n", rf.me, rf.currentTerm)
	// 向其他节点发送 RequestVoteRPC
	var args RequestVoteArgs
	currentTerm, _ := rf.GetState()
	args.Term = currentTerm
	args.CandidateId = rf.me
	// TODO: 先不管日志相关信息
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.log[rf.lastLogIndex].LogTerm
	// 新建 Channel, 用于传回 reply. 容量为 len(rf.peers)-1
	// 功能: (1) 计票 （2） 重发没成功的 RPC (3) 检查 term 并更新
	replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
	// 这是一个选举专用的计时器
	timer := time.After(campaignTimeout * time.Millisecond)
	voteCount := 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.collectRequestVote(i, args, replyCh)
		}
	}

	// 计票过程
	for voteCount <= (len(rf.peers) / 2) {
		select {
		case reply := <-replyCh:
			if reply.Err {
				// RPC 失败，则重发消息
				go rf.collectRequestVote(reply.Server, args, replyCh)
			} else if reply.Term > currentTerm {
				// 发现更高的 term, 变成 follower, 结束 Campaign
				rf.returnToFollower(reply.Term)
				return
			} else {
				if reply.VoteGranted {
					voteCount += 1
				}
			}
		case <-timer: // 选举超时，不再等待结果
			return
		}
	}

	// 获得多数票了, 需要校验一下还是不是 candidate
	if rf.role == "candidate" {
		rf.becomeLeader(voteCount)
	}
}

func (rf *Raft) HeartBeat(duration time.Duration) {
	// 成为 Leader 之后，开启一个定时器，每隔 50ms 发送 heartbeat 给其他节点
	// Assignment 2: HeartBeat 时会带上要发送的 log entry
	timer := time.NewTimer(duration)
	currentTerm, _ := rf.GetState()
	replyCh := make(chan AppendEntriesReply, len(rf.peers)-1)
	if rf.commitIndex < rf.lastLogIndex {
		go rf.RealAppendEntries()
	} else {
		var args AppendEntriesArgs

		args.Term = currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		if rf.commitIndex < 1 {
			args.PrevLogIndex = 0
			args.PrevLogTerm = -1
		} else {
			args.PrevLogIndex = rf.commitIndex
			args.PrevLogTerm = rf.log[rf.commitIndex].LogTerm
		}
		args.Entries = []LogEntry{}

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.collectAppendEntries(i, args, replyCh)
			}
		}
	}

	//DPrintf("HeartBeat, message Sent\n")
	go func() {
		for {
			select {
			case <-timer.C:
				// 不再是 leader，停止计时器
				_, isLeader := rf.GetState()
				if !isLeader {
					return
				} else {
					go rf.HeartBeat(heartbeatDuration * time.Millisecond)
					return
				}
			case reply := <-replyCh:
				if reply.Err {
					// RPC 通信失败，重发HeartBeat
					go rf.HeartBeat(heartbeatDuration * time.Millisecond)
				} else if reply.Term > currentTerm {
					// 发现更大的 Term, 变成 follower
					rf.returnToFollower(reply.Term)
					return
				}
			}
		}
	}()
}

func (rf *Raft) RealAppendEntries() {
	var args AppendEntriesArgs
	//DPrintf("leader: %d, toCount: %t, latest log: %d, commit index: %d", rf.me, toCount, len(rf.log)-1, rf.commitIndex)
	currentTerm, _ := rf.GetState()
	args.Term = currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	if rf.commitIndex < 1 {
		args.PrevLogIndex = 0
		args.PrevLogTerm = -1
	} else {
		args.PrevLogIndex = rf.commitIndex
		args.PrevLogTerm = rf.log[rf.commitIndex].LogTerm
	}

	args.Entries = rf.log[rf.commitIndex+1:]
	//DPrintf("Leader %d sent HeartBeat with log index: %d\n", rf.me, rf.commitIndex+1)

	replyCh := make(chan AppendEntriesReply, len(rf.peers)-1) // 不需要向自己发消息
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.collectAppendEntries(i, args, replyCh)
		}
	}

	//go func() {
	//	// 只取计算本次发送的那一条日志，是否复制到超过半数节点上
	//	for rf.replicateVote[args.PrevLogIndex+1] < len(rf.peers)/2 {
	//	}
	//	// 领导者执行条目，并通知其他节点
	//	//DPrintf("Leader %d receive %d votes on log index %d.\n", rf.me, rf.replicateVote[args.PrevLogIndex+1], args.PrevLogIndex+1)
	//	if args.PrevLogIndex+1 > rf.commitIndex {
	//		rf.commitIndex = args.PrevLogIndex + 1
	//	}
	//	rf.applyLog()
	//}()

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())
	// 需要做的内容
	// 1. 初始化为 follower
	// 2. 设定一个Timer, 超时没有收到 heartbeat, 那么就竞选 campaign()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	// initialize other states in rf
	rf.currentTerm = 0
	rf.role = "follower"
	rf.votedFor = -1 // initial value is -1
	// Campaign when electionTimer triggered
	rf.electionTimer = time.NewTimer(randDuration(electionTimeoutLower, electionTimeoutUpper))
	// Log 相关属性的初始化
	// 由于 rf.log 从 下标从1开始，所以 0 处赋一个空值
	rf.log = append(rf.log, LogEntry{-1, nil})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// background goroutine
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				if rf.role != "leader" {
					// if the server does not receive heartbeat in time, begin election
					rf.Campaign()
				}
			}
		}
	}()

	return rf
}
