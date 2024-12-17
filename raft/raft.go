// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"golang.org/x/exp/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, conState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = conState.Nodes
	}
	r := &Raft{
		id:                  c.ID,
		Term:                hardState.Term,    // Term 和 Vote 从持久化存储中读取
		Vote:                hardState.Vote,    // Term 和 Vote 从持久化存储中读取
		RaftLog:             newLog(c.Storage), // Log 也从持久化存储中读取
		Prs:                 map[uint64]*Progress{},
		State:               StateFollower,
		votes:               map[uint64]bool{},
		msgs:                nil,
		Lead:                0,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		randElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:    0,
		electionElapsed:     0,
	}

	r.Prs = make(map[uint64]*Progress)
	for _, id := range c.peers {
		r.Prs[id] = &Progress{}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	// prevLog has been compacted(not exisit in raftlog)
	if err != nil {
		r.sendSnapshot(to)
		return false
	}
	entries := r.RaftLog.getEntries(prevLogIndex+1, r.RaftLog.LastIndex()+1)
	pents := make([]*pb.Entry, len(entries))
	for i := 0; i < len(entries); i++ {
		pents[i] = &entries[i]
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: pents,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		// 生成 Snapshot 的工作是由 region worker 异步执行的，如果 Snapshot 还没有准备好
		// 此时会返回 ErrSnapshotTemporarilyUnavailable 错误，此时 leader 应该放弃本次 Snapshot Request
		// 等待下一次再请求 storage 获取 snapshot（通常来说会在下一次 heartbeat response 的时候发送 snapshot）
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

func (r *Raft) sendAppendEntriesResponse(to uint64, reject bool, idx uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   idx,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) sendHeartbeatResponse(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	LastIndex := r.RaftLog.LastIndex()
	LastTerm, _ := r.RaftLog.Term(LastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: LastTerm,
		Index:   LastIndex,
	}
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1 // 初始化为 leader 的最后一条日志索引（后续出现冲突会往前移动）
		r.Prs[id].Match = 0                        // 初始化为 0 就可以了
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      r.id,
		Entries: []*pb.Entry{{}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		//fmt.Printf("r.Term = %d \n", r.Term)
		r.becomeFollower(m.Term, None)
		// ??? why None , I forget it
	}
	if m.Term != None && m.Term < r.Term {
		return nil
	}
	/*v := reflect.ValueOf(m)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		fieldName := t.Field(i).Name
		fieldValue := v.Field(i).Interface()
		fmt.Printf("%s:%v ", fieldName, fieldValue)
	}
	fmt.Println()*/
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}
func (r *Raft) stepFollower(m pb.Message) {
	r.electionElapsed = 0
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleSnapshot(m)
	}
}
func (r *Raft) stepCandidate(m pb.Message) {
	r.electionElapsed = 0
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	}
}
func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleBeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
}
func (r *Raft) handleHup() {
	// Your Code Here (2A).
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	} else {
		for id := range r.Prs {
			if id != r.id {
				r.sendRequestVote(id)
			}
		}
	}
}

// handleBeat handle Beat message
func (r *Raft) handleBeat() {
	// Your Code Here (2A).
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// handlePropose handle Propose RPC request
func (r *Raft) handlePropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	entries := m.Entries
	// wrapped entry and append
	for i, entry := range entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Next = lastIndex + uint64(len(entries)) + 1
	r.Prs[r.id].Match = lastIndex + uint64(len(entries))
	r.broadcastAppend()
}
func (r *Raft) broadcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	// important: when leader is single, it should commit all entries
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
		r.sendRequestVoteResponse(m.From, false)
		r.Vote = m.From
	} else {
		r.sendRequestVoteResponse(m.From, true)
	}
}

// handleRequestVoteResponse handle RequestVote RPC response
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Your Code Here (2A).
	r.votes[m.From] = !m.Reject
	agree := 0
	half := len(r.Prs) / 2
	for _, v := range r.votes {
		if v {
			agree++
		}
	}
	if agree > half {
		r.becomeLeader()
	} else if len(r.votes)-agree > half {
		r.becomeFollower(r.Term, None)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.sendHeartbeatResponse(m.From)
}

// handleHeartBeatResponse handle Heartbeat RPC response
func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	// Your Code Here (2A).
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	lg := r.RaftLog
	firstIndex := lg.dummyIndex
	lastIndex := lg.LastIndex()
	if m.Index > lastIndex {
		r.sendAppendEntriesResponse(m.From, true, lg.LastIndex())
		return
	}
	localTerm, _ := lg.Term(m.Index)
	if localTerm != m.LogTerm {
		//fmt.Printf("Idx = %d,  localTerm = %d\n", m.Index, localTerm)
		for _, ent := range lg.entries {
			if ent.Term == localTerm {
				//fmt.Printf("ent.Index = %d lastIndex = %d dummyIndex = %d\n", ent.Index, lastIndex, firstIndex)
				r.sendAppendEntriesResponse(m.From, true, ent.Index-1)
				return
			}
		}
	}
	// firstIndex   m.Index    lastIndex
	// 			entry
	for i, ent := range m.Entries {
		if ent.Index < firstIndex {
			continue
		} else if ent.Index <= lg.LastIndex() {
			//fmt.Printf("ent.Index = %d lastIndex = %d dummyIndex = %d\n", ent.Index, lastIndex, firstIndex)
			//fmt.Println(len(lg.entries))
			localTerm, err := lg.Term(ent.Index)
			if err != nil {
				log.Error("get term error")
			}
			if ent.Term != localTerm {
				j := ent.Index - firstIndex
				lg.entries[j] = *ent
				lg.entries = lg.entries[:j+1]
				lg.stabled = min(lg.stabled, ent.Index-1)
			}
		} else {
			for j := i; j < len(m.Entries); j++ {
				lg.entries = append(lg.entries, *m.Entries[j])
			}
			break
		}
	}
	// 同步提交索引
	if m.Commit > lg.committed {
		lg.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendEntriesResponse(m.From, false, lg.LastIndex())
}

// handleAppendEntriesResponse handle AppendEntries RPC response
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {

	// Your Code Here (2A).
	if m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.LeaderCommit()
	}
}
func (r *Raft) LeaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	//fmt.Println(match)
	// 5 2,  4 1
	half := match[(len(r.Prs)-1)/2]
	if half > r.RaftLog.committed {
		if halfTerm, _ := r.RaftLog.Term(half); halfTerm == r.Term {
			r.RaftLog.committed = half
			r.broadcastAppend()
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		// 要从提交日志之后开始追加日志（已提交的一定会提交，不会被覆盖）
		r.sendAppendEntriesResponse(m.From, true, r.RaftLog.committed)
		return
	}
	//r.becomeFollower(m.Term, m.From)
	r.RaftLog.dummyIndex = meta.Index + 1
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.entries = make([]pb.Entry, 0)
	r.RaftLog.pendingSnapshot = m.Snapshot
	log.Infof("meta.Index = %v \n ", meta.Index)
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{Next: r.RaftLog.LastIndex() + 1}
	}

	r.sendAppendEntriesResponse(m.From, false, meta.Index)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
