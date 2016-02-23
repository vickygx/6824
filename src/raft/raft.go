package raft

//
// API Raft exposes to the service (or tester)
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
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
)

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

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Persistent
	currentTerm	int 	// latest term server has seen (init: 0 on boot)
	votedFor 	int 	// candidate index in peers of server that received vote in current term (or null if none)
	log 		[]LogEntry	// log entries; each entry contains command for state machine	

	// Volatile
	commitIndex int 	// index of highest log entry known to be committed (init: 0)
	lastApplied int 	// index of highest log entry applied to state machine (init 0)
	state 		int 	// state of either LEADER, FOLLOWER, or CANDIDATE

	// Only applicable to leaders
	nextIndex   int[] 	// for each server, index of the next log entry to send to that server (init: leader's last log index + 1)
	matchIndex	int[]	// for each server, index of highest log entry known to be replicated on server (init: 0, increases monotonically)
						// used to figure out commitIndex

	//startElectionTime?
	//withoutVotesUntil?
	// exiting (bool)

}

type LogEntry struct {
	Command 	interface()		// command for state machine
	Term 		int 			// term when entry was received by leader (first index is 1)
}


//
// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// Restores previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}


// LEADERS: (periodically)
// Upon election: send initial empty AppendEntries RPCs
// (heartbeat) to each server; repeat during idle periods to
// prevent election timeouts (§5.2)

func (rt *Raft) sendEmptyHeartBeatsToFollowers() {

}

func (rf *Raft) executeAllMachineAction() {
	// 1- If commitIndex > lastApplied: apply all necessary logs and increment lastApplied.Appy
	// log[lastApplied] to state machine (§5.3)
	// 2- If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)

	// If machine is leader and just applied, send 
	// an ApplyMsg to the applyCh argument to Make()
}

func (rf *Raft) executeFollowerMachineAction() {
	// 1- Respond to RPCs from candidates and leaders
	// 2- If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate:
	// convert to candidate
}

func (rf *Raft) executeCandidateMachineAction() {
	// 1- On conversion to candidate, start election:
	// • Increment currentTerm
	// • Vote for self
	// • Reset election timer
	// • Send RequestVote RPCs to all other servers
	// 2- If votes received from majority of servers: become leader
	// 3- If AppendEntries RPC received from new leader: convert to
	// follower
	// 4-If election timeout elapses: start new election
}

// Leader periodically sends heartbeat message only if it is idle

//	Function that executes all the necessary actions
//  a leader needs to take care of for each period.
//  This includes updating followers with current log, or sending empty heartbeats if needed.
func (rf *Raft) executePeriodicLeaderAction() {
	// 1- Upon election: send initial empty AppendEntries RPCs
	// (heartbeat) to each server; repeat during idle periods to
	// prevent election timeouts (§5.2)


	// Check all followers and 
	// 2- If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
		// • If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		// • If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
	// (If not, send empty HeartBeat Message.) - vicky

	// (With every successful reply: (this can be put in a separate periodic))
		// 3- If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N **

	// (Could try to apply here once we have committed amount to ensure leader is sending,
	// but might not be necessary to prevent duplicats.)
}



//====================== RPC between each server

type RequestVoteArgs struct {
	Term 			int 	// candidate's term
	CandidateId 	int  	// index in peers of candidate requesting vote
	LastLogIndex	int 	// index in log of candidate's last log entry
	LastLogTerm 	int 	// term of candidate's last log entry
}

type RequestVoteReply struct {
	Term 			int 	// currentTerm, for candidate to update itself
	VoteGranted		bool	// True means candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: Might need to lock
	reply.Term = rf.currentTerm

	// Only grant vote if we have not voted OR we have already voted for requested
	// AND the requester's log and term is at least as up-to-date as requestee's log.
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		
		isMoreUpTodate := func (lastLogTermA, lastLogIndexA, lastLogTermB, lastLogIndexB int) bool {
			if lastLogTermA == lastLogTermB {
				return lastLogIndexA <= lastLogIndexB
			} else {
				return lastLogTermA < lastLogTermB
			}
		}

		if isMoreUpTodate(rf.log[len(rf.log - 1)].Term, len(rf.log - 1), args.LastLogTerm, args.LastLogIndex) {
			reply.VoteGranted = true
		}
	}
	
	// Do not grant vote
	reply.VoteGranted = false
}

//
// Function to send a RequestVote RPC to a server.
// 		@param(server) is the index of the target server in rf.peers[].
// 		@param(args) expects RPC arguments in args.
// 		@param(reply) *reply is filled with RPC reply, so caller should probably pass &reply.
//
// @returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//////////////////// HEARTBeats (empty AppendEntries call)

type AppendEntriesArgs struct {
	Term 			int 		// leader's term
	LeaderId		int     	// index in "peers" of leader requesting the append entries
	PrevLogIndex	int 		// index of log entry immediately preceding new ones
	PrevLogTerm		int 		// term of PrevLogIndex entry
	Entries 		[]LogEntry 	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 	int 		// leader's commitIndex
}

type AppendEntriesReply struct {
	Term 			int 		// currentTerm, for leader to update itself
	Success 		bool 		// true if follower contained entry matching prevLogIndexa and prevLogTerm
}

//
// AppendEntries RPC handler.
//
func (rt *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// todo: withhold leaderID
	// If args.Term > current term, and the machine is either a leader or candidate, return to follower state, and continue

	// Do not append if the leader is stale or if there is no entry in its log with the same index and term
	if args.Term < rt.currentTerm || args.PrevLogIndex > len(rt.log) - 1 ||
	   args.PrevLogTerm != rt.log[args.PrevLogIndex].Term  {
		reply.Term = rt.currentTerm
		reply.Success = false
		// TODO: Update current term to larger value.
		return
	}

	// Delete all existing entries in the log that conflicts with the args.Entries
	startIndex := 0 				// Index (in args.Entries) of first entry to append to current log 
	numEntries := len(args.Entries)
	indexLastNewEntry := -1
	for i := args.PrevLogIndex + 1; i < len(rt.log); i++ {
		if startIndex >= numEntries {
			break
		}

		// Check entries are equal, otherwise remove unequal entry and following entries
		if rt.log[i] == args.Entries[startIndex] {
			startIndex++
			indexLastNewEntry = i
		} else {
			// Remove all following entries
			rt.log = rt.log[:i]
		}
	}
	
	// Append any new entries not already in log
	for i := startIndex; i < numEntries; i++ {
		rt.log = append(rt.log, args.Entries[i])
		indexLastNewEntry = i
	}

	// Apply all newly commited entries and update commitIndex
	if args.LeaderCommit > rt.commitIndex {
		rt.commitIndex = min(args.LeaderCommit, indexLastNewEntry)

		// OR have a channel where you notify that state has changed
		// TODO: apply newly commited entries 
		// call function to advanceCommitIndex()

	}

	reply.Term = rt.currentTerm
	reply.Success = true
	// TODO: Update current term to larger value.
}

//
// Function to send a AppendEntries RPC to a server.
// 		@param(server) is the index of the target server in rf.peers[].
// 		@param(args) expects RPC arguments in args.
// 		@param(reply) *reply is filled with RPC reply, so caller should probably pass &reply.
//
// @returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}



//====================== API functions available by raft client to use

//
// Returns currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// Note: the periodic heartbeats of the server will automatically try to update
// the other followers with log. Therefore no need to do it here. (TODO)
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// If this server isn't the leader, return false
	if !isLeader {
		return index, term, isLeader
	}

	// Append entry to local log
	rf.log = append(rf.log, LogEntry{Command:command, Term:term})
	return index, term, isLeader
}

//
// The tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	// TODO(1): create a background goroutine that starts en election
	// (by sending out RequestVote RPCs) when it hasn't heard from another
	// peer in a while ** election timeouts should be randomized **
	
	// TODO: if necessary, read from persister state
	// if so, update log

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = FOLLOWER
	// set random election timeout?




	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}


func (rf *Raft) startNewElection() bool {
	// must be candidate
	// update currentTerm
	// reset leaderId to -1
	// vote for self, set votedfor to self
	// for each follower: beginRequestVote (parallel?)

}

// Returns true if the user got all the votes
// Returns false if timeout occurs or receives ping from leader
// variable like next_election_timeout_time. (ping from leader should cause this
// variable to increase by election_timeout)
// last ACK time?
func (rf *Raft) beginRequestVote() bool {
	// switch statement

	time step_down int;		// should step down if an election timeout goes
							// by without confirming leadership
	var currentTerm int;    // save term when request vote beings
							// in order to stop request vote once we have
							// been pinged by new leader
	for {
		switch {
		case rf.term > currentTerm // todo might need some locking?
			break;
		case step_down < time.now():
			break;
		case getEnoughVotes:
			// do something
			break;
			//TODO
		}
	}
}

// Calls append entries to the followers
func (rf *Raft) beginAppendEntries() bool {
	// note: if fails, count how many failures
	// note: maybe count number of threads??

	// build up request
	// execute RPC
	switch (status [did it send or not]) {
	case OK:
		break;
	case FAIL:
		maybe have a backoff until variable. for each peer.
	}

	// respond to response

	// if response.term > our own term, step down
	// Otherwise
	// if success update peer.matchIndex, and try to advance commit index
	// uppdate nextIndex +1

	// if the peer isn't caught up, initiate something to catch the peer up
	// (e.g. many requests in another thread)
}


func (rf *Raft) becomeLeader() bool {
	// make sure state is candidate
	// print out you are leader
	// update state, leaderId
	// start election, and update startEleectionAt variable.
	// update witholdVotesUntil variable.

	// Becoming a leader
// 1 - set each nextIndex to be LastLogIndex + 1
// 2 - set all match index to be -1
// 


}
// todo might need a # ass
// have more printing please

// Have a main timer thread that just runs (if it doesn't exit) 
// such that if time.now > startEleectionsAt, it starts new election


func (rf *Raft) mainThread() { // have a peer thread between each peer. might be alot
	// maybe lock
	// have a for loop unless exiting
	// each iteration (can use wait's)
	// depending on switch state, allows issuing of RPC's (leader, candidate)
	// candidate: requestVote if not done
	// leader sends appendEntries if needed or heartbeat if nextHeartBeat Time is hit
}

// maybe have another thread for the leader (leader can create thread)
// to keep the log updated with commited (while loop for exiting. and is leader)


func (rf *Raft) convertToFollower() {
	
}