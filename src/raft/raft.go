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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"

	"../labrpc"
)

// import "bytes"

const ELECTION_TIMEOUT_BASE = 500   //ms
const ELECTION_TIMEOUT_RANGE = 500  //ms
const HEARTBEAT_CHECK_INTERVAL = 50 //ms
const HEARTBEAT_SEND_INTERVAL = 50  //ms
const INITIAL_LOG_TERM = 0
const INVALID_LEADER_ID = -1

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int32
	Index   int
	Content interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	last_applied  int32
	apply_channel chan ApplyMsg
	apply_cond    *sync.Cond

	append_cond []*sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term      int32
	voted_for int
	log       []LogEntry

	commit_index int32

	next_index  []int
	match_index []int

	election_timeout int

	is_leader    bool
	is_candidate bool
	is_follower  bool

	leader_id int

	last_heartbeat_time time.Time
	heartbeat_mu        sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return int(rf.get_term()), rf.is_leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist_without_lock() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	//DPrintf("[persist] Server-%d: Start to do persistent.", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.get_term())
	e.Encode(rf.voted_for)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	LPrintf(rf.me, "Start to read persist.")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var _term int32
	var _voted_for int
	var _log []LogEntry
	if d.Decode(&_term) != nil ||
		d.Decode(&_voted_for) != nil ||
		d.Decode(&_log) != nil {
		LPrintf(rf.me, "Read persistent state error.")
		panic("Read persistent state error.")
	} else {
		rf.set_term(_term)
		rf.voted_for = _voted_for
		rf.log = _log
	}
	LPrintf(rf.me, "Restore form persister: term %d, voted_for: %d, log: %+v, rf.log: %+v.", _term, _voted_for, _log, rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

//
// compare candidate's log with current server
// return true for candidate's log is equal or newer than current server
// otherwise return false
//
func (rf *Raft) compare_log_without_lock(last_log_index int, last_log_term int32) bool {
	if len(rf.log) == 0 {
		return true
	}

	if rf.log[len(rf.log)-1].Term > last_log_term {
		LPrintf(rf.me, "Candidate's last log(index-%d term-%d) is older than me(index-%d term-%d)",
			last_log_index, last_log_term,
			len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		return false
	} else if rf.log[len(rf.log)-1].Term < last_log_term {
		return true
	}

	if rf.log[len(rf.log)-1].Term == last_log_term &&
		len(rf.log)-1 > last_log_index {
		LPrintf(rf.me, "Candidate's last log(index-%d term-%d) is older than me(index-%d term-%d)",
			last_log_term, last_log_index,
			len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		return false
	}
	return true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	LPrintf(rf.me, "receive RequestVote request.")
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LPrintf(rf.me, "receive vote request: %+v; current term is %d, voted for is %d.", args, rf.get_term(), rf.voted_for)

	if args.Term < rf.get_term() {
		reply.Term = rf.get_term()
		reply.VoteGranted = false
		return
	}

	log_permit := rf.compare_log_without_lock(args.LastLogIndex, args.LastLogTerm)

	if args.Term > rf.get_term() {
		rf.convert_to_follower(args.Term, INVALID_LEADER_ID, true)
		reply.Term = rf.get_term()
		if log_permit {
			reply.VoteGranted = true
			rf.voted_for = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
		rf.persist_without_lock()
	} else if args.Term == rf.get_term() {
		if rf.voted_for == INVALID_LEADER_ID {
			reply.Term = rf.get_term()
			if log_permit {
				reply.VoteGranted = true
				rf.voted_for = args.CandidateId
				rf.persist_without_lock()
			} else {
				reply.VoteGranted = false
			}
		} else if rf.voted_for == args.CandidateId {
			reply.Term = rf.get_term()
			reply.VoteGranted = true
		} else if rf.voted_for != args.CandidateId {
			reply.Term = rf.get_term()
			reply.VoteGranted = false
		}
	}

	if reply.VoteGranted {
		rf.update_heartbeat()
		LPrintf(rf.me, "Grant vote to candidate: %d", args.CandidateId)
	}

	if reply.Term != rf.get_term() {
		LPrintf(rf.me, "Term not match, reply term %d local term %d", reply.Term, rf.get_term())
		panic("Term not match")
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term                       int32
	Success                    bool
	FollowerPrevTerm           int32
	FollowerPrevTermFirstIndex int
	FollowerLogLen             int
}

func (rf *Raft) first_index_for_term_without_lock(term int32) int {
	for i, v := range rf.log {
		if term == v.Term {
			return i
		}
	}
	return -1
}

func (rf *Raft) last_index_for_term_without_lock(term int32) int {
	for i, v := range rf.log {
		if v.Term == term {
			if i >= len(rf.log)-1 {
				return i
			}
			if rf.log[i+1].Term != term {
				return i
			}
		}
	}

	return -1
}

func (rf *Raft) merge_entry_into_log_without_lock(start int, entries []LogEntry) {
	left := len(entries)
	for i, j := start, 0; i < len(rf.log) && j < len(entries); i, j = i+1, j+1 {
		if rf.log[i].Term != entries[j].Term {
			rf.log = rf.log[0:i]
			left_entries := entries[j:]
			rf.log = append(rf.log, left_entries...)
			left = 0
			break
		}

		left--
	}

	if left > 0 {
		left_entries := entries[len(entries)-left:]
		rf.log = append(rf.log, left_entries...)
	}

	rf.persist_without_lock()
}

func (rf *Raft) do_append_entries_without_lock(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	LPrintf(rf.me, "receive append entries, args: %+v.", args)

	max_entry_index := len(rf.log) - 1
	if args.PrevLogIndex != -1 && args.PrevLogIndex > max_entry_index {
		reply.Term = rf.get_term()
		reply.Success = false
		reply.FollowerLogLen = len(rf.log)
		if len(rf.log) != 0 {
			reply.FollowerPrevTerm = rf.log[max_entry_index].Term
			first_index := rf.first_index_for_term_without_lock(rf.log[max_entry_index].Term)
			assert(first_index != -1)
			reply.FollowerPrevTermFirstIndex = first_index
		}

		LPrintf(rf.me, "append entries fail, "+
			"cause: args.PrevLogIndex > max_entry_index,  max_entry_index: %d, args: %+v", max_entry_index, args)
		return
	}

	if args.PrevLogIndex != -1 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term = rf.get_term()
		reply.Success = false

		reply.FollowerLogLen = len(rf.log)
		first_index := rf.first_index_for_term_without_lock(rf.log[args.PrevLogIndex].Term)
		assert(first_index != -1)

		reply.FollowerPrevTerm = rf.log[args.PrevLogIndex].Term
		reply.FollowerPrevTermFirstIndex = first_index

		stale_term := rf.log[args.PrevLogIndex].Term
		rf.log = rf.log[0:args.PrevLogIndex]
		rf.persist_without_lock()

		LPrintf(rf.me, "args.PrevLogTerm != rf.log[args.PrevLogIndex].Term, "+
			"preLog term: %d, args: %+v", stale_term, args)
		return
	}

	rf.merge_entry_into_log_without_lock(args.PrevLogIndex+1, args.Entries)

	reply.Term = rf.get_term()
	reply.Success = true
	LPrintf(rf.me, "append log success, args: %+v.", args)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	LPrintf(rf.me, "receive append entries request: %+v.", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.get_term()
	if args.Term >= term {
		rf.leader_id = args.LeaderId
		if !rf.is_follower && args.Term >= term {
			rf.convert_to_follower(args.Term, args.LeaderId, true)
		} else if rf.is_follower && args.Term > term {
			rf.convert_to_follower(args.Term, args.LeaderId, true)
		}

		if len(args.Entries) == 0 {
			reply.Term = rf.get_term()
			reply.Success = true
		} else {
			rf.do_append_entries_without_lock(args, reply)
		}

		rf.update_follower_commit_index_without_lock(args.Term, args.LeaderCommit)

		rf.update_heartbeat()
	} else if args.Term < term {
		reply.Term = term
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	LPrintf(rf.me, "start to agree on command %+v", command)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrintf(rf.me, "start to agree on command %+v return", command)

	term := rf.get_term()

	if !rf.is_leader {
		return int(term), -1, false
	}

	//persist log locally
	index := len(rf.log)
	entry := LogEntry{term, index, command}
	rf.log = append(rf.log, entry)
	rf.persist_without_lock()
	rf.match_index[rf.me] = index

	rf.signal_append_entries()

	//there is shift 1 between entry index of log slice and entry raft index
	return index + 1, int(term), true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) Live() {
	atomic.StoreInt32(&rf.dead, 0)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) set_term(term int32) {
	atomic.StoreInt32(&rf.term, term)
}
func (rf *Raft) get_term() int32 {
	return atomic.LoadInt32(&rf.term)
}
func (rf *Raft) inc_term() int32 {
	return atomic.AddInt32(&rf.term, 1)
}

func (rf *Raft) set_commit_index(index int32) {
	atomic.StoreInt32(&rf.commit_index, index)
}
func (rf *Raft) get_commit_index() int32 {
	return atomic.LoadInt32(&rf.commit_index)
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

	LPrintf(me, "Make a raft.")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Live()

	rf.apply_channel = applyCh
	rf.last_applied = -1
	var apply_cond_mu sync.Mutex
	rf.apply_cond = sync.NewCond(&apply_cond_mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.is_candidate = false
	rf.is_leader = false
	rf.is_follower = true

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.set_commit_index(-1)

	for i := 0; i < len(rf.peers); i++ {
		next := 0
		if len(rf.log) != 0 {
			next = len(rf.log) - 1
		}
		rf.next_index = append(rf.next_index, next)
		rf.match_index = append(rf.match_index, -1)

		var append_cond_mu sync.Mutex
		rf.append_cond = append(rf.append_cond, sync.NewCond(&append_cond_mu))
	}

	go rf.check_heartbeat_timeout(rf.set_election_timeout_without_lock(), rf.term)
	go rf.apply_entry()

	return rf
}

func (rf *Raft) update_heartbeat() {
	rf.heartbeat_mu.Lock()
	defer rf.heartbeat_mu.Unlock()
	rf.last_heartbeat_time = time.Now()
}
func (rf *Raft) get_heartbeat() time.Time {
	rf.heartbeat_mu.Lock()
	defer rf.heartbeat_mu.Unlock()
	return rf.last_heartbeat_time
}

func (rf *Raft) apply_entry() {
	LPrintf(rf.me, "Start apply.")
	rf.apply_cond.L.Lock()
	defer rf.apply_cond.L.Unlock()

	for {
		for rf.last_applied >= rf.get_commit_index() {
			LPrintf(rf.me, "No entry to apply now, last applied: %d commited: %d", rf.last_applied, rf.get_commit_index())
			rf.apply_cond.Wait()
		}

		for rf.last_applied < rf.get_commit_index() {
			rf.last_applied++
			msg := ApplyMsg{
				true,
				rf.log[rf.last_applied].Content,
				int(rf.last_applied + 1), //shift 1 between command index and slice index
			}
			LPrintf(rf.me, "Apply index %d msg: %+v", rf.last_applied, msg)
			rf.apply_channel <- msg
		}

	}
}

func (rf *Raft) check_apply() {
	rf.apply_cond.L.Lock()
	defer rf.apply_cond.L.Unlock()

	if rf.get_commit_index() > rf.last_applied {
		LPrintf(rf.me, "Find new msg commited, signal apply rountine.")
		rf.apply_cond.Signal()
	}
}

//periodically actions
func (rf *Raft) check_heartbeat_timeout(timeout int, term int32) {
	for {
		if rf.killed() {
			LPrintf(rf.me, "get killed.")
			return
		}

		is_break := func() bool {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !rf.is_follower {
				LPrintf(rf.me, "Not follower now, quit heartbeat timeout check.")
				return true
			}
			if term != rf.term {
				LPrintf(rf.me, "term not match: now term %d expected term %d quit heartbeat timeout check.", rf.term, term)
				return true
			}

			last_heartbeat_time := rf.get_heartbeat()
			now := time.Now()
			interval := now.Sub(last_heartbeat_time).Milliseconds()
			if interval > int64(timeout) {
				LPrintf(rf.me, "heartbeat timeout.")
				go rf.convert_to_candidate()
				return true
			}

			return false
		}()

		if is_break {
			break
		}

		time.Sleep(HEARTBEAT_CHECK_INTERVAL * time.Millisecond)
	}
}

func (rf *Raft) keep_heartbeat_with_followers(term int32) {
	for {
		if rf.killed() {
			LPrintf(rf.me, "get killed.")
			return
		}

		var args AppendEntriesArgs
		is_break := func() bool {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !rf.is_leader {
				LPrintf(rf.me, "quit case not leader now")
				return true
			}
			if term != rf.term {
				LPrintf(rf.me, "quit case term now %d doesn't match term expected %d", rf.term, term)
				return true
			}

			args = AppendEntriesArgs{
				term,
				rf.me,
				0,
				0,
				nil,
				rf.get_commit_index(),
			}

			return false
		}()

		if is_break {
			break
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(idx int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(idx, &args, &reply)
			}(i)
		}
		time.Sleep(HEARTBEAT_SEND_INTERVAL * time.Millisecond)
	}
}

func (rf *Raft) update_follower_commit_index_without_lock(term int32, index int32) {
	if index == -1 {
		return
	}
	if index <= rf.get_commit_index() {
		LPrintf(rf.me, "Leader commit index is old, recorder commited index: %d leader send: %d", rf.get_commit_index(), index)
	}

	var min_index int
	if len(rf.log)-1 < int(index) {
		min_index = len(rf.log) - 1
	} else {
		min_index = int(index)
	}

	if term == rf.log[min_index].Term {
		rf.set_commit_index(int32(min_index))
		LPrintf(rf.me, "update commited index to %d log length is %d", rf.get_commit_index(), len(rf.log))
	}

	rf.check_apply()
}

func (rf *Raft) update_leader_commit_index_without_lock() {
	peer_num := len(rf.peers)
	quorum := peer_num/2 + 1
	var idxs []int

	for _, index := range rf.match_index {
		assert(index < len(rf.log))
		if index == -1 {
			continue
		}
		if rf.log[index].Term == rf.term {
			idxs = append(idxs, index)
		}
	}

	if len(idxs) < quorum {
		return
	}

	sort.Sort(sort.Reverse(sort.IntSlice(idxs)))
	rf.set_commit_index(int32(idxs[quorum-1]))
	LPrintf(rf.me, "Indexs are: %+v, update commit index to %d", idxs, idxs[quorum-1])

	go rf.check_apply()
}

func (rf *Raft) start_append_entries_to_peers() {
	for idx := range rf.peers {
		if idx != rf.me {
			LPrintf(rf.me, "Start to append entries to Server-%d go rountine.", idx)
			go rf.append_entries_single(idx, rf.get_term())
		}
	}
}

func (rf *Raft) signal_append_entries() {
	for idx := range rf.peers {
		if idx != rf.me {
			LPrintf(rf.me, "Signal Server-%d append entries go rountine to work.", idx)
			go rf.signal_append_entires_single(idx)
		}
	}
}

func (rf *Raft) signal_append_entires_single(idx int) {
	LPrintf(rf.me, "append_cond_lock_%d: try to wake up waiter for new entry", idx)
	rf.append_cond[idx].L.Lock()
	defer rf.append_cond[idx].L.Unlock()

	rf.append_cond[idx].Broadcast()
	LPrintf(rf.me, "append_cond_lock_%d: new entry come, wake up waiter", idx)
}
func (rf *Raft) get_append_log_length_without_lock() int {
	return len(rf.log)
}

func (rf *Raft) append_entries_single(idx int, term int32) {
	LPrintf(rf.me, "append_cond_lock_%d: try to get lock to start append entry", idx)
	rf.append_cond[idx].L.Lock()
	LPrintf(rf.me, "append_cond_lock_%d: get lock to start append entry", idx)
	defer rf.append_cond[idx].L.Unlock()

	for {
		if rf.killed() {
			LPrintf(rf.me, "be killed.")
			return
		}

		is_leader := true
		var args AppendEntriesArgs
		var next int
		var log_length int

		is_break := func() bool {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			is_leader = rf.is_leader
			if !is_leader {
				LPrintf(rf.me, "Not leader now, stop appending entries")
				return true
			}
			if term != rf.get_term() {
				LPrintf(rf.me, "Term changed, stop appending entries, record term: %d term now: %d", term, rf.get_term())
				return true
			}
			next = rf.next_index[idx]
			log_length = rf.get_append_log_length_without_lock()
			LPrintf(rf.me, "Follower idx is %d, next to send is %d, log_length is %d", idx, next, log_length)
			assert(next <= log_length) // rf.me is leader
			if next == log_length {
				return false
			}

			entries_to_send := rf.log[next:log_length]
			args = AppendEntriesArgs{
				rf.get_term(),
				rf.me,
				next - 1,
				rf.get_log_term_without_lock(next - 1),
				entries_to_send,
				rf.get_commit_index(),
			}

			return false
		}()

		if is_break {
			break
		}

		if next < log_length {
			LPrintf(rf.me, "start to append entries to Server-%d", idx)
			go rf.do_append_entries_single(&args, idx, term)
		} else {
			LPrintf(rf.me, "Server-%d next(%d) >= len(rf.log)(%d)", idx, next, log_length)
		}

		LPrintf(rf.me, "append_entry_cond_%d: waiter go to sleep", idx)
		rf.append_cond[idx].Wait()
		LPrintf(rf.me, "append_entry_cond_%d: waiter wake up", idx)
	}
}

func (rf *Raft) check_leader_and_term_without_lock(term int32) bool {
	if !rf.is_leader {
		LPrintf(rf.me, "Not leader now")
		return false
	}

	if term != rf.term {
		LPrintf(rf.me, "Term not match, term recored: %d term now: %d", term, rf.term)
		return false
	}

	return true
}

func (rf *Raft) update_follower_index_without_lock(idx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.next_index[idx]-1 != args.PrevLogIndex {
		LPrintf(rf.me, "Another rountine has updated indexes, next_index: %d args' prev index: %d", rf.next_index[idx], args.PrevLogIndex)
		return
	}
	if !reply.Success {
		if reply.FollowerLogLen < rf.next_index[idx] {
			rf.next_index[idx] = reply.FollowerLogLen
			LPrintf(rf.me, "append to Server-%d fail, follower log too short, args: %+v, reply: %+v", idx, args, reply)
			return
		}

		LPrintf(rf.me, "append to Server-%d fail, prev term not match, args: %+v, reply: %+v", idx, args, reply)
		term_index := rf.last_index_for_term_without_lock(reply.FollowerPrevTerm)
		if term_index == -1 {
			rf.next_index[idx] = reply.FollowerPrevTermFirstIndex
			return
		} else {
			rf.next_index[idx] = term_index
			return
		}
	} else {
		LPrintf(rf.me, "append to Server-%d success, args: %+v, reply: %+v", idx, args, reply)
		rf.next_index[idx] += len(args.Entries)

		old_match_idx := rf.match_index[idx]
		rf.match_index[idx] = rf.next_index[idx] - 1
		LPrintf(rf.me, "idx %d next index is: %d, match index is: %d", idx, rf.next_index[idx], rf.match_index[idx])
		assert(old_match_idx <= rf.match_index[idx])

		rf.update_leader_commit_index_without_lock()
	}

}

func (rf *Raft) do_append_entries_single(args *AppendEntriesArgs, idx int, term int32) {
	LPrintf(rf.me, "AppendEntriesArgs to Server-%d: %+v.", idx, args)
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(idx, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		go func() {
			LPrintf(rf.me, "append_cond_lock_%d: try to get lock to wake up waiter.", idx)
			rf.append_cond[idx].L.Lock()
			defer rf.append_cond[idx].L.Unlock()

			rf.append_cond[idx].Broadcast()
			LPrintf(rf.me, "append_cond_lock_%d: wake up waiter.", idx)
		}()
	}()

	if !rf.check_leader_and_term_without_lock(term) {
		return
	}

	if !ok {
		LPrintf(rf.me, "send to Server-%d failed, need retry", idx)
		return
	}

	if reply.Term > rf.get_term() {
		LPrintf(rf.me, "find higher term from Server-%d, convert to follower, reply: %+v", idx, reply)
		rf.convert_to_follower(reply.Term, INVALID_LEADER_ID, true)
		return
	}

	rf.update_follower_index_without_lock(idx, args, &reply)
}

func (rf *Raft) set_election_timeout_without_lock() int {
	r := rand.New(rand.NewSource(int64(rf.me) + time.Now().Unix()))
	rf.election_timeout = int(ELECTION_TIMEOUT_BASE + r.Int31()%ELECTION_TIMEOUT_RANGE)
	LPrintf(rf.me, "New election_timeout value is : %d ms.", rf.election_timeout)
	return rf.election_timeout
}

func (rf *Raft) get_log_term_without_lock(index int) int32 {
	assert(index >= -1 && index < len(rf.log))

	var term int32
	if index == -1 {
		term = INITIAL_LOG_TERM
	} else {
		term = rf.log[index].Term
	}

	return term
}

func (rf *Raft) get_last_log_term_without_lock() int32 {
	return rf.get_log_term_without_lock(len(rf.log) - 1)
}

func (rf *Raft) do_election(term int32) {
	if term == -1 {
		term = rf.inc_term()
	}
	LPrintf(rf.me, "start election for term %d.", term)

	var election_timeout int
	var peer_num int

	var vote_mu sync.Mutex
	vote_count := 1 //1 for self-vote

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.voted_for = rf.me
		rf.persist_without_lock()

		election_timeout = rf.set_election_timeout_without_lock()
		peer_num = len(rf.peers)
		log_length := len(rf.log)
		last_log_term := rf.get_last_log_term_without_lock()

		args := RequestVoteArgs{
			term,
			rf.me,
			log_length - 1,
			last_log_term,
		}
		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			go func(_idx int, _vote_term int32) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(_idx, &args, &reply)
				if !ok {
					return
				}

				vote_mu.Lock()
				defer vote_mu.Unlock()
				if reply.VoteGranted {
					LPrintf(rf.me, "receive vote from server %d.", _idx)
					vote_count += 1
				} else {
					if reply.Term > rf.get_term() {
						rf.convert_to_follower(reply.Term, INVALID_LEADER_ID, false)
					}
				}
			}(idx, term)
		}
	}()

	time_start := time.Now()
	for {
		is_break := func() bool {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !rf.is_candidate {
				LPrintf(rf.me, "Not candidate now, stop election")
				return true
			}
			if term != rf.get_term() {
				LPrintf(rf.me, "Term changed, stop election, term now: %d term recorded: %d", rf.get_term(), term)
				return true
			}

			t := time.Now()
			interval := t.Sub(time_start).Milliseconds()
			if interval > int64(election_timeout) {
				LPrintf(rf.me, "election timeout.")
				go rf.do_election(rf.inc_term())
				return true
			}

			return false
		}()

		if is_break {
			break
		}

		is_break = func() bool {
			vote_mu.Lock()
			defer vote_mu.Unlock()
			if vote_count > peer_num/2 {
				LPrintf(rf.me, "win election.")
				rf.convert_to_leader(term)
				return true
			}

			return false
		}()

		if is_break {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) convert_to_leader(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrintf(rf.me, "enter convert to leader")

	if term != rf.get_term() {
		LPrintf(rf.me, "convert leader fail, term not match")
		return
	}
	if !rf.is_candidate {
		LPrintf(rf.me, "conver leader fail, not candidate")
		return
	}

	assert(rf.is_candidate && !rf.is_follower)
	rf.is_leader = true
	rf.is_candidate = false

	rf.set_commit_index(-1)

	for i := 0; i < len(rf.peers); i++ {
		next := 0
		if len(rf.log) != 0 {
			next = len(rf.log) - 1
		}
		rf.next_index[i] = next
		rf.match_index[i] = -1
	}

	go rf.start_append_entries_to_peers()
	go rf.keep_heartbeat_with_followers(term)

	LPrintf(rf.me, "convert to leader now, term: %d", rf.get_term())
}

func (rf *Raft) convert_to_follower(term int32, leader_id int, hold_lock bool) {
	if !hold_lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	// if term <= rf.get_term() {
	// 	LPrintf(rf.me, "Stop to convert to follower cause input term %d le than current term %d", term, rf.get_term())
	// 	return
	// }
	LPrintf(rf.me, "Enter convert to follower.")

	if rf.is_leader {
		assert(!rf.is_candidate && !rf.is_follower)
		//FIXME do not use go rountine in convert function
		rf.wake_up_append_entry_waiter()
	} else if rf.is_candidate {
		assert(!rf.is_leader && !rf.is_follower)
	} else if rf.is_follower {
		assert(!rf.is_leader && !rf.is_candidate)
		if term == rf.term {
			LPrintf(rf.me, "")
			return
		}
	}

	rf.is_leader = false
	rf.is_candidate = false
	rf.is_follower = true

	rf.leader_id = leader_id
	prev_term := rf.get_term()
	rf.set_term(term)
	if prev_term > term {
		rf.voted_for = INVALID_LEADER_ID
	}
	rf.persist_without_lock()

	go rf.check_heartbeat_timeout(rf.set_election_timeout_without_lock(), term)

	LPrintf(rf.me, "convert to follower now, term: %d", term)
}

func (rf *Raft) convert_to_candidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrintf(rf.me, "enter convert to candidate now.")

	assert(rf.is_follower && !rf.is_leader)
	rf.is_candidate = true
	rf.is_follower = false

	go rf.do_election(-1)

	LPrintf(rf.me, "convert to candidate now.")
}

func (rf *Raft) wake_up_append_entry_waiter() {
	for i := 0; i < len(rf.peers); i++ {
		go func(i int) {
			LPrintf(rf.me, "append_cond_lock_%d: try to get lock to wake up waiter.", i)
			rf.append_cond[i].L.Lock()
			defer rf.append_cond[i].L.Unlock()

			rf.append_cond[i].Broadcast()
			LPrintf(rf.me, "append_cond_lock_%d: get lock to signal append entry quit", i)
		}(i)
	}
}
