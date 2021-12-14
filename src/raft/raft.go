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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role          string // "leader", "follower", "candidate"
	currentTerm   int
	votedFor      int
	electionTimer *time.Timer
}

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
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here. See paper figure 2.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here. See paper figure 2.
	Term        int
	VoteGranted bool
	Err         bool // 后期可以改成具体的 error
	Server      int
}

//
// AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {
	// See figure 2.
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []byte // referring to raftstate in persister.go
	LeaderCommit int
}

//
// AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself (revert to follower state)
	success bool //
	Err     bool // 后期可以改成具体的 error
	Server  int
}

//
// Helper function.
//
func (rf *Raft) resetElectionTimer(duration time.Duration) {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
	log.Printf("Node %d Reset Election Timer\n", rf.me)
}

//
// Helper function. Rand duration in a range
//
func randDuration(start int, end int) time.Duration {
	return time.Duration(rand.Intn(end-start)+start) * time.Millisecond
}

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
			reply.VoteGranted = false
		}
	} else { // term > currentTerm
		rf.currentTerm = term
		// 可能需要切换角色
		if rf.role != "follower" {
			rf.role = "follower"
			rf.resetElectionTimer(randDuration(150, 300))
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	log.Printf("%d receives RequestVote. Reply with: voteGranted: %t; Term: %d", rf.me, reply.VoteGranted, reply.Term)
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

func (rf *Raft) collectAppendEntries(server int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	//currentTerm, _ := rf.GetState()
	rf.sendAppendEntries(server, args, &reply)
	//if ok && reply.Term > currentTerm {
	//	rf.role = "follower"
	//	rf.
	//}
	//replyCh <- reply
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: 暂时只考虑 heartbeat
	term := args.Term
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm
	if term < currentTerm {
		reply.success = false
		return
	} else {
		rf.currentTerm = term
		if rf.role == "candidate" || rf.role == "leader" {
			// receive AppendEntries from leader
			rf.role = "follower"
		}
		rf.electionTimer.Reset(randDuration(150, 300))
		reply.success = true
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
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
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
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
	rf.role = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimer.Reset(randDuration(150, 300))
	log.Printf("Candidate %d start an election. Term: %d\n", rf.me, rf.currentTerm)
	// 向其他节点发送 RequestVoteRPC
	var args RequestVoteArgs
	currentTerm, _ := rf.GetState()
	args.Term = currentTerm
	args.CandidateId = rf.me
	// TODO: 先不管日志相关信息
	args.LastLogIndex = -1
	args.LastLogTerm = -1
	// 新建 Channel, 用于传回 reply. 容量为 len(rf.peers)-1
	replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
	timer := time.After(randDuration(150, 300)) // 选举专用的计时器
	// (1) receive votes from majority and become leader
	// (2) find larger term and become follower
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
				rf.role = "follower"
				rf.currentTerm = reply.Term
				rf.resetElectionTimer(randDuration(150, 300))
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

	// 获得多数票了
	rf.role = "leader"
	rf.votedFor = -1
	rf.electionTimer.Stop()
	go rf.LeaderSendHeartBeat()
	rf.HeartBeat(50 * time.Millisecond)

	log.Printf("%d has become leader. receive %d votes, currentTerm: %d", rf.me, voteCount, currentTerm)

	// TODO: 没有成为 Leader, term -= 1?
	//rf.currentTerm -= 1
}

func (rf *Raft) HeartBeat(duration time.Duration) {
	// 成为 Leader 之后，开启一个定时器，每隔 50ms 发送 heartbeat 给其他节点
	timer := time.NewTimer(duration)
	go func() {
		for {
			select {
			case <-timer.C:
				// 不再是 leader，停止计时器
				_, isLeader := rf.GetState()
				if !isLeader {
					return
				}
				go rf.LeaderSendHeartBeat()
				timer.Reset(duration)
			}
		}
	}()
}

func (rf *Raft) LeaderSendHeartBeat() {
	// 向其他节点发送 AppendEntries (heartbeat)
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	// TODO: 先不管日志相关信息
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			//ok := rf.sendAppendEntries(i, args, &reply) // ok: true if RPC delivered
			//if ok && reply.Term > rf.currentTerm {
			//	log.Printf("Leader %d receive higher term", rf.me)
			//	log.Printf("current Term: %d, reply.Term: %d\b", rf.currentTerm, reply.Term)
			//	rf.currentTerm = reply.Term
			//	rf.votedFor = -1
			//	rf.role = "follower"
			//	return
			//}
			go rf.collectAppendEntries(i, args)
		}
	}

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
	// time related
	rf.electionTimer = time.NewTimer(randDuration(150, 300))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// background goroutine
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				if rf.role == "follower" || rf.role == "candidate" {
					// if the server does not receive heartbeat in time, begin election
					rf.Campaign()
				}
			}
		}
	}()

	return rf
}
