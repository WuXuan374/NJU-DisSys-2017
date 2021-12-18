package raft

import (
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

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
	// log 需要按顺序摆放（也就是每次都 append 到尾部）
	// log 的下标从 1 开始
	log          []LogEntry
	commitIndex  int
	lastApplied  int
	lastLogIndex int
	nextIndex    []int
	matchIndex   []int
	pendingLog   []LogEntry // 等待 leader 发送的 log entries
	applyCh      chan ApplyMsg
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
	Err         bool // TODO: 后期可以改成具体的 error
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
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself (revert to follower state)
	Success bool //
	Err     bool // TODO: 后期可以改成具体的 error
	Server  int
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
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
// Helper function
//
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

//
// 角色变化相关的函数
//
func (rf *Raft) returnToFollower(term int) {
	rf.role = "follower"
	rf.votedFor = -1
	rf.currentTerm = term
	rf.resetElectionTimer(randDuration(electionTimeoutLower, electionTimeoutUpper))
}

func (rf *Raft) becomeLeader(voteCount int) {
	rf.role = "leader"
	// leader 不需要 electionTimer 了
	rf.electionTimer.Stop()
	rf.HeartBeat(heartbeatDuration * time.Millisecond)
	// nextIndex, matchIndex 初始化
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	DPrintf("%d has become leader. receive %d votes, currentTerm: %d", rf.me, voteCount, rf.currentTerm)
}

//
// 日志检查相关函数
//
func (rf *Raft) findMatchingLogEntry(logIndex int, logTerm int) (bool, int) {
	// bool: false 代表没找到
	// int: 如果是缺少这个 Entry, 此项为 -1; 如果是该 index 存在 Entry, 返回该 Entry 的 index（无论是否 conflict）
	// 如果这是第一个日志，就不用校验了
	if logIndex == 0 {
		return true, -1
	}
	if logIndex > len(rf.log)-1 {
		return false, -1
	}
	if logTerm != rf.log[logIndex].LogTerm {
		return true, logIndex
	}

	return true, -1

}

//
// Data structures
//
type LogEntry struct {
	LogTerm int // 初值为 0
	Command interface{}
}
