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
	log           []LogEntry
	commitIndex   int
	lastApplied   int
	lastLogIndex  int
	nextIndex     []int
	matchIndex    []int
	replicateVote []int // 记录日志被成功复制到多少个 follower 上
	applyCh       chan ApplyMsg
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
	Term             int  // currentTerm, for leader to update itself (revert to follower state)
	Success          bool //
	Err              bool // TODO: 后期可以改成具体的 error
	Server           int
	ConflictIndex    int // the index of successfully replicated log
	LastMatchedIndex int
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

	go rf.checkMatchIndex()
}

func (rf *Raft) checkMatchIndex() {
	ticker := time.NewTicker(checkMatchIndexTimeout * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			_, isLeader := rf.GetState()
			if !isLeader {
				ticker.Stop()
				return
			} else {
				count := 0
				minimum := 0
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						DPrintf("%d: matchIndex[%d]: %d", rf.me, i, rf.matchIndex[i])
						if rf.matchIndex[i] > rf.commitIndex {
							count += 1
							if minimum == 0 {
								minimum = rf.matchIndex[i]
							} else {
								minimum = min(minimum, rf.matchIndex[i])
							}
						}
					}
				}

				currentTerm, _ := rf.GetState()
				if count >= len(rf.peers)/2 && minimum > rf.commitIndex && minimum < len(rf.log) && rf.log[minimum].LogTerm == currentTerm {
					rf.commitIndex = minimum
					rf.applyLog()
				}
			}
		}
	}
}

//
// 日志检查相关函数
//
func (rf *Raft) findMatchingLogEntry(logIndex int, logTerm int) (bool, int) {
	// bool: false 代表不匹配
	// int: 如果不匹配，返回不匹配的 index, 方便调用程序删除之后的日志条目
	// 如果这是第一个日志，就不用校验了
	if logIndex < 0 {
		return false, -1
	}
	if logIndex == 0 {
		return true, -1
	}
	if logIndex > len(rf.log)-1 {
		return false, 0
	}
	if logTerm != rf.log[logIndex].LogTerm {
		return false, logIndex
	}

	return true, -1
}

func candidateUpToDate(receiver *Raft, candidateLogIndex int, candidateLogTerm int) bool {
	// up-to-date: 5.4.1, 比较 last log entry
	// term 不同，term 更大的，更 up-to-date
	// same term: log 更长，更加 up-to-date
	// candidate 至少应该 as up to date as receiver, true 代表 as up to date
	if receiver.log[receiver.lastLogIndex].LogTerm != candidateLogTerm {
		return candidateLogTerm >= receiver.log[receiver.lastLogIndex].LogTerm
	}
	// same term
	return candidateLogIndex >= receiver.lastLogIndex
}

func (rf *Raft) applyLog() {
	go func() {
		for {
			if rf.lastApplied >= rf.commitIndex {
				break
			}
			rf.lastApplied += 1
			rf.applyCh <- ApplyMsg{
				Index:       rf.lastApplied,
				Command:     rf.log[rf.lastApplied].Command,
				UseSnapshot: false,
				Snapshot:    []byte{},
			}
			DPrintf("%d apply log entry: %d", rf.me, rf.lastApplied)
		}
	}()
}

func (rf *Raft) installLogs(server int, conflictIndex int) {
	DPrintf("%d install logs on %d", rf.me, server)
	var followerReply AppendEntriesReply
	currentTerm, _ := rf.GetState()
	var prevLogTerm int
	if conflictIndex > 1 {
		prevLogTerm = rf.log[conflictIndex-1].LogTerm
	} else {
		prevLogTerm = 0
	}
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: conflictIndex - 1,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.log[:rf.commitIndex+1],
	}
	ok := rf.sendInstallLogs(server, args, &followerReply)
	if !ok {
		followerReply.Err = true
	}
	if followerReply.Err {
		// RPC 通信失败，重发HeartBeat
		go rf.installLogs(server, conflictIndex)
		return
	}
	if followerReply.Term > currentTerm {
		// 发现更大的 Term, 变成 follower
		rf.returnToFollower(followerReply.Term)
		return
	}
	if followerReply.Success {
		rf.matchIndex[server] = followerReply.LastMatchedIndex
		//DPrintf("Leader %d receive replication vote from %d\n", rf.me, server)
		return
	}
}

// Helper function
func contains(s []LogEntry, e LogEntry) bool {
	for _, a := range s {
		if a.LogTerm == e.LogTerm && a.Command == e.Command {
			return true
		}
	}
	return false
}

//
// Data structures
//
type LogEntry struct {
	LogTerm int // 初值为 0
	Command interface{}
}
