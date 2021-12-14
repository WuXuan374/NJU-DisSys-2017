package raft

import (
	"log"
	"math/rand"
	"time"
)

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
// Helper function. When larger term found, return to follower
func (rf *Raft) returnToFollower(term int) {
	rf.role = "follower"
	rf.votedFor = -1
	rf.currentTerm = term
	rf.resetElectionTimer(randDuration(electionTimeoutLower, electionTimeoutUpper))
}
