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
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

const gFollower, gCandidate, gLeader = 0, 1, 2
const gSleepMsc = 5
const gNullCmd = "NULL"
const gBeatTimeOut = 300

// Debugging
const GDebug = 0

/*as each Raft peer becomes aware that successive log entries are
committed, the peer should send an ApplyMsg to the service (or
tester) on the same server, via the applyCh passed to Make(). set
CommandValid to true to indicate that the ApplyMsg contains a newly
committed log entry.

in Lab 3 you'll want to send other kinds of messages (e.g.,
snapshots) on the applyCh; at that point you can add fields to
ApplyMsg, but set CommandValid to false for these other uses.
*/

// ApplyMsg 导出数据
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        int        // 当前角色,follower, candidate, leader
	currentTerm int        // 当前所处的term
	votedFor    int        // 当前term向谁投票，默认为-1
	log         []LogEntry // log日志
	commitIndex int        // server commit的日志的索引
	lastApplied int        // server 最后applied的日志索引
	timeout     int64      // server 等待心跳超时的时间
	beatTime    int64      // 设置心跳发送时间
	// leader 专属变量
	nextIndex   []int // 向每个follower同步的日志起始索引
	matchIndex  []int // 与每个follower匹配的日志起始索引
	beatSentCnt int   // 发送心跳次数
	// 向client通知执行情况
	ch chan ApplyMsg // 通道声明
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if GDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

// "return currentTerm and whether this server
// believes it is the leader."
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = false
	if rf.role == gLeader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.lastApplied)
	encoder.Encode(rf.log)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var commitIndex, currentTerm, lastApplied, votedFor int
	var log []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&lastApplied) != nil ||
		decoder.Decode(&log) != nil {
		rf.DPrintf("Error in unmarshal raft state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		rf.log = log
		// rf.lastLogs = lastlogs
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
}

// 请求投票的参数
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int // candidate's term
	CandidateID   int // candidate requesting vote
	LastLogIndex  int // index of candidate's last log entry
	LastLogTerm   int // term of candidate's last log entry
}

// "投票返回结果"
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// "每个server处理其他server投票请求的函数"
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.DPrintf("[%d] receive vote request from %d\n", rf.me, args.CandidateID)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.CandidateTerm >= rf.currentTerm {
		// 如果是同一个term，我方没有参与投票过才行
		// 对方term 大于我方term也可以
		if (args.CandidateTerm == rf.currentTerm && rf.votedFor == -1) || (args.CandidateTerm > rf.currentTerm) {
			lgIdx := len(rf.log) - 1
			if args.LastLogTerm > rf.log[lgIdx].Term || (args.LastLogTerm == rf.log[lgIdx].Term && args.LastLogIndex >= lgIdx) {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				rf.currentTerm = args.CandidateTerm
				rf.ChangetoFollower()
			} else if args.CandidateTerm > rf.currentTerm {
				rf.currentTerm = args.CandidateTerm
				rf.votedFor = -1
				rf.ChangetoFollower()
			}
		}
	}
	rf.DPrintf("[%d] vote to %d :%t\n", rf.me, args.CandidateID, reply.VoteGranted)
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

// "定义server的日志结构"
type LogEntry struct {
	Cmd  interface{} // 日志的命令
	Term int         // 日志对应的term
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
	index := 1
	term, isLeader := rf.GetState()
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if isLeader {
		index = len(rf.log)
		entry := LogEntry{command, term}
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.nextIndex[rf.me] = len(rf.log)
		rf.log = append(rf.log, entry)
		// rf.DPrintf("[%d] add new log (%v), index: %d, term: %d,log:%v", rf.me, command, index, term, rf.log)

		rf.persist() // 添加log后persist
		rf.DPrintf("[%d] add new log (%v), index: %d, term: %d, 持久化成功!", rf.me, command, index, term)
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	// atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.dead = 1
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// SetTimeOut: 为follower设置等待心跳的超时，为candidate设置下次竞选时间
func (rf *Raft) SetTimeOut() {
	rand.Seed(time.Now().UnixNano() + (int64)(rf.me))
	var waitMillsec int = rand.Intn(150) + gBeatTimeOut
	var t time.Time = time.Now().Add(time.Duration(waitMillsec) * time.Millisecond)
	rf.timeout = t.UnixNano()
	// rf.DPrintf("[%d] 设置 %d ms 后超时\n", rf.me, waitMillsec)
}

// SetBeatTime: 为leader设置心跳发送间隔
func (rf *Raft) SetBeatTime(msc int) {
	var t time.Time = time.Now().Add(time.Duration(msc) * time.Millisecond)
	rf.beatTime = t.UnixNano()
}

// "作为follower，循环检查是否超时"
func (rf *Raft) WaitBeatRoutine() {
	for {
		rf.mu.Lock()
		if rf.dead == 1 {
			rf.mu.Lock()
			break
		} else if rf.role == gFollower && time.Now().UnixNano() > rf.timeout {
			rf.ChangetoCandidate()
		}
		rf.mu.Unlock()
		time.Sleep(gSleepMsc * time.Millisecond) // 休息, 继续
	}
	rf.DPrintf("[%d] WaitBeatRoutine func exit_________", rf.me)
}

// 向所有follower依此发送投票请求, curTerm是当前candidate的term
func (rf *Raft) sendRequestVoteToFollower(sentRequestCnt int, args RequestVoteArgs, curTerm int) {
	// 挨个发送投票请求
	var tmpMu sync.Mutex
	cond := sync.NewCond(&tmpMu)
	// ch := make(chan RequestVoteReply, len(rf.peers)) // 建立通道用于写入投票结果
	replyArray := make([]RequestVoteReply, len(rf.peers))
	replyCnt := 1 // 统计收到的回复数，自己投一票
	acCnt := 1    // 统计收到赞成票的数，自己投一票
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.DPrintf("[%d] send vote request to peer %d, term: %d \n", rf.me, i, curTerm)
			go func(i int) {
				rf.sendRequestVote(i, &args, &replyArray[i])
				tmpMu.Lock()
				defer tmpMu.Unlock()
				replyCnt++
				if replyArray[i].VoteGranted {
					acCnt++
				}
				cond.Broadcast()
			}(i)
		}
	}
	tmpMu.Lock()
	for acCnt < (len(rf.peers)/2+1) && replyCnt < len(rf.peers) {
		cond.Wait()
	}
	voteCnt := acCnt
	tmpMu.Unlock()

	rf.mu.Lock()
	// 已经发起了一轮新的投票，这轮不管了
	if rf.beatSentCnt != sentRequestCnt {
		rf.DPrintf("[%d] 第 %d 轮投票超时，当前已经发起了第%d 轮投票，不再处理", rf.me, sentRequestCnt, rf.beatSentCnt)
		rf.mu.Unlock()
		return
	}
	// 如果收到超过一半的投票，当前身份仍然是candidate && 当前term仍然投票给自己
	// 上述条件满足，切换为leader身份，并且初始化其他server的nextIndex
	if voteCnt > len(rf.peers)/2 && rf.votedFor == rf.me && rf.role == gCandidate {
		rf.ChangetoLeader()
	} else {
		rf.DPrintf("[%d] 竞选Leader失败\n", rf.me)
		sz := len(replyArray)
		for i := 0; i < sz; i++ {
			data := replyArray[i]
			if data.Term > rf.currentTerm {
				rf.currentTerm = data.Term
				rf.votedFor = -1 // 更新term，也更新投票信息
				rf.ChangetoFollower()
			}
		}
	}
	rf.mu.Unlock()
}

// "作为candidate循环请求投票的线程"
func (rf *Raft) RequestForVoteRoutine() {
	for {
		rf.mu.Lock()
		if rf.dead == 1 || rf.role != gCandidate {
			rf.mu.Unlock()
			break
		} else if time.Now().UnixNano() >= rf.timeout {
			rf.beatSentCnt++
			rf.DPrintf("[%d] 发起第 %d 次竞选\n", rf.me, rf.beatSentCnt)
			rf.currentTerm++
			rf.votedFor = rf.me
			var lgIdx int = len(rf.log) - 1
			args := RequestVoteArgs{rf.currentTerm, rf.me, lgIdx, rf.log[lgIdx].Term}
			sentCnt := rf.beatSentCnt
			rf.SetTimeOut() //重新设置下一轮投票时间
			curTerm := rf.currentTerm
			rf.mu.Unlock()
			go rf.sendRequestVoteToFollower(sentCnt, args, curTerm)
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(gSleepMsc * time.Millisecond)
	}
	rf.DPrintf("%d RequestForVoteRoutine func exit_________", rf.me)
}

// "定义leader发送的心跳包——AppendEntries"
type AppendEntries struct {
	Term         int // leader's term
	LeaderID     int
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Log          []LogEntry // log
	LeaderCommit int        // leader's commitIndex
}

// "follower对leader的heatbeat的反馈"
type BeatReply struct {
	Server   int  // server标记
	Term     int  // 心跳返回follower当前term，用于leader更新
	Success  bool // 如果leader发送的prevLogTerm和prevLogIndex与follower匹配，则返回true
	FirstIdx int  // 如果日志和follower的不匹配，follower返回leader的Term第一次出现的log
}

func (rf *Raft) Apply() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{true, rf.log[i].Cmd, i}
		rf.ch <- msg
	}
	rf.lastApplied = rf.commitIndex
	if rf.commitIndex > 0 {
		rf.persist()
		rf.DPrintf("[%d] Applied index 从 %d 变为 %d, persist 成功", rf.me, rf.lastApplied, rf.commitIndex)
	}
}

// "follower 处理来自leader的heartbeat"
func (rf *Raft) HandleHeartBeat(args *AppendEntries, reply *BeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Server = rf.me
	// rf.DPrintf("[%d] 收到%d的心跳包，日志大小为： %d\n", rf.me, args.LeaderID, len(args.Log))
	if args.Term >= rf.currentTerm {
		lgIndex := len(rf.log) - 1
		// leader日志和follower日志成功匹配
		if args.PrevLogIndex <= lgIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			reply.Success = true
			//将匹配索引后面的日志删除，替换为当前日志
			// rf.DPrintf("%d receive new log ", rf.me)
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Log[:]...)
		} else {
			reply.Success = false
			// leader的prevLogIndex大于follower的日志最后一个索引
			if args.PrevLogIndex > lgIndex {
				rf.DPrintf("[%d] leader %d 的PrevLogIndex %d 大于我的log索引:%d, 日志不匹配,返回%d", rf.me, args.LeaderID, args.PrevLogIndex, lgIndex, lgIndex)
				reply.FirstIdx = lgIndex
				// 如果是Term不同，则返回folower的当下term的第一个log所在的index
			} else {
				// 进入else语句, 必然是 args.PrevLogIndex <= lgIndex
				lastTerm := rf.log[args.PrevLogIndex].Term
				idx := args.PrevLogIndex - 1
				if idx < 0 {
					idx = 0 // 防止idx越界
				}
				for ; idx >= 1; idx-- {
					if rf.log[idx].Term != lastTerm {
						break
					}
				}
				reply.FirstIdx = idx
				rf.DPrintf("[%d] 与leader %d 日志不匹配,返回日志匹配索引:%d", rf.me, args.LeaderID, (idx))
				// reply.FirstIdx = idx
			}
		}
		// 只有日志匹配的前提下才能修改commit的index
		//
		if reply.Success && args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if rf.commitIndex > len(rf.log)-1 {
				rf.commitIndex = len(rf.log) - 1
			}
			rf.Apply()
		} else if reply.Success && len(args.Log) > 0 {
			rf.persist() // 存储日志
		}
		rf.ChangetoFollower()
	}
	rf.DPrintf("[%d] 收到 %d 的日志大小: %d,日志匹配: %v, 返回的匹配索引: %d \n", rf.me, args.LeaderID, len(args.Log), reply.Success, reply.FirstIdx)
}

func (rf *Raft) ChangetoCandidate() {
	if rf.role != gCandidate {
		rf.DPrintf("[%d] 超时, 准备竞选Leader", rf.me)
		rf.role = gCandidate
		rf.beatSentCnt = 0 // 这里表示发起投票次数
		rf.role = gCandidate
		go rf.RequestForVoteRoutine()
	}
	//rf.SetTimeOut()
}

func (rf *Raft) ChangetoFollower() {
	if rf.role != gFollower {
		rf.role = gFollower
		rf.DPrintf("[%d] 切换为follower", rf.me)
	}
	rf.SetTimeOut()
}

func (rf *Raft) ChangetoLeader() {
	if rf.role != gLeader {
		rf.DPrintf("[%d] 成为leader, log: %v", rf.me, rf.log)
	}
	// 立即发送心跳
	rf.beatSentCnt = 0
	rf.SetBeatTime(1)
	// 初始化nextIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = len(rf.log) - 1
		rf.nextIndex[i] = len(rf.log)
		// rf.DPrintf("[%d] %d 的matchIndex :%d", rf.me, i, rf.matchIndex[i])
	}
	rf.role = gLeader
	go rf.SendBeatRoutine()
}

// "leader 向其他follower发送heartbeat信息"
func (rf *Raft) sendHeartBeat(server int, args *AppendEntries, reply *BeatReply) bool {
	ok := rf.peers[server].Call("Raft.HandleHeartBeat", args, reply)
	return ok
}

// "leader 向server构建heartbeat信息"
func (rf *Raft) constructHeartBeat(server int) AppendEntries {
	var args AppendEntries
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LeaderCommit = rf.commitIndex
	// 构建心跳包
	args.PrevLogIndex = rf.matchIndex[server]
	args.PrevLogTerm = rf.log[rf.matchIndex[server]].Term
	args.Log = append(args.Log, rf.log[rf.matchIndex[server]+1:]...)
	return args
}

// 向所有follower发送心跳
func (rf *Raft) SendHeartBeatToFollower(appendEntryArray []AppendEntries, leaderTerm int, beatSentCnt int) {
	// 逐个发送心跳
	var tmpMu sync.Mutex
	ch := make(chan BeatReply)
	cond := sync.NewCond(&tmpMu)
	replyCnt := 0 // 提前保存自己的回复
	finished := 1 // 表示心跳接收是否结束

	// 逐个发送心跳，为什么把commitIndexArray提这么前，就是为了加锁，解锁只进行一次
	// 按照software engineering的思想，应该在609行初始化commitIndexArray
	rf.mu.Lock()
	commitIndexArray := make([]int, 0, len(rf.peers))
	commitIndexArray = append(commitIndexArray, len(rf.log))
	rf.DPrintf("[%d] 我的日志 %v", rf.me, (rf.log))
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.DPrintf("[%d] 发送给 %d的日志: %v", rf.me, i, (appendEntryArray[i].Log))
		go func(i int, args *AppendEntries) {
			var reply BeatReply
			res := rf.sendHeartBeat(i, args, &reply)
			tmpMu.Lock()
			finished++
			if res {
				replyCnt++
			}
			tmpMu.Unlock()
			cond.Broadcast()
			if res {
				ch <- reply
			}
		}(i, &appendEntryArray[i])
	}

	// rf.DPrintf("[%d] 我的日志:%v", rf.me, rf.log)
	tmpMu.Lock()
	for finished < len(rf.peers) {
		cond.Wait()
		if replyCnt > 0 {
			reply := <-ch
			replyCnt--
			rf.mu.Lock()
			// 发送心跳次数已经改变，或者不再是leader, 不再进行处理
			if rf.beatSentCnt != beatSentCnt || rf.role != gLeader {
				rf.DPrintf("[%d] 已经开始发送第%d次心跳，不再处理第%d次心跳结果", rf.me, rf.beatSentCnt, beatSentCnt)
				rf.mu.Unlock()
				tmpMu.Unlock()
				return
			} else if reply.Success {
				rf.matchIndex[reply.Server] += len(appendEntryArray[reply.Server].Log)
				if rf.matchIndex[reply.Server] >= len(rf.log) {
					rf.matchIndex[reply.Server] = len(rf.log) - 1
				}
				commitIndexArray = append(commitIndexArray, rf.matchIndex[reply.Server])
				rf.DPrintf("[%d] %d 日志匹配,matchIndex更新为:%d", rf.me, reply.Server, rf.matchIndex[reply.Server])
			} else if reply.Term > rf.currentTerm {
				rf.DPrintf("[%d] %d 的term大于我方term", rf.me, reply.Server)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.ChangetoFollower()
				rf.mu.Unlock()
				tmpMu.Unlock()
				return
			} else {
				rf.matchIndex[reply.Server] = reply.FirstIdx
				if rf.matchIndex[reply.Server] >= len(rf.log) {
					rf.matchIndex[reply.Server] = len(rf.log) - 1
				}
				rf.DPrintf("[%d] %d 日志不匹配,matchIndex更新为:%d", rf.me, reply.Server, rf.matchIndex[reply.Server])
			}
			if len(commitIndexArray) > len(rf.peers)/2 {
				rf.AdjustCommitIndex((commitIndexArray))
			}
			rf.mu.Unlock()
		}
	}
	tmpMu.Unlock()
}

func (rf *Raft) AdjustCommitIndex(commitIndexArray []int) {
	// 单独拉出来更新commitIndex
	sz := len(commitIndexArray)
	if sz > len(rf.peers)/2 {
		sort.Sort(sort.Reverse(sort.IntSlice(commitIndexArray)))
		tmpIdx := commitIndexArray[(len(rf.peers))/2]
		if rf.log[tmpIdx].Term == rf.currentTerm && tmpIdx > rf.commitIndex {
			rf.commitIndex = tmpIdx
			rf.Apply()
		}
	}
}

// "作为leader，持续性的向follower发送心跳的线程"
func (rf *Raft) SendBeatRoutine() {
	for {
		rf.mu.Lock()
		if rf.dead == 1 || rf.role != gLeader {
			rf.mu.Unlock()
			break
		} else if time.Now().UnixNano() >= rf.beatTime {
			rf.beatSentCnt++
			// 构建心跳包
			var appendEntryArray []AppendEntries = make([]AppendEntries, len(rf.peers))
			var logSZArray []int = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					appendEntryArray[i] = rf.constructHeartBeat(i)
				}
				logSZArray[i] = len(appendEntryArray[i].Log)
			}
			leaderTerm := rf.currentTerm // 记录leader发送日志时的term
			rf.SetBeatTime(100)
			beatSentCnt := rf.beatSentCnt
			rf.mu.Unlock()
			rf.DPrintf("[%d] 第 %d 次发送心跳\n", rf.me, rf.beatSentCnt)
			go rf.SendHeartBeatToFollower(appendEntryArray, leaderTerm, beatSentCnt)
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(gSleepMsc * time.Millisecond)
	}
	rf.DPrintf("[%d] SendBeatRoutine func exit_________", rf.me)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	// 增加一条空log
	tmplog := LogEntry{gNullCmd, rf.currentTerm}
	rf.log = append(rf.log, tmplog)
	rf.ch = applyCh
	// 初始化nextIndex 和 matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.dead = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.readPersist(persister.ReadRaftState())
	rf.Apply()
	rf.DPrintf("[%d] 初始化为follower, 日志: %v\n", rf.me, rf.log)
	rf.ChangetoFollower()
	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	go rf.WaitBeatRoutine()
	return rf
}
