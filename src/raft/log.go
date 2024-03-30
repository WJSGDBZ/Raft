package raft

import (
	"log"
)

type Log struct {
	Entries           []entry
	LastIncludedIndex int
	LastIncludedTerm  int
}

type entry struct {
	Command interface{}
	Term    int
}

func NewLog() *Log {
	log := Log{
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	}
	log.Entries = append(log.Entries, entry{ //Sentinel variable
		Term:    0,
		Command: nil,
	})

	return &log
}

func (l *Log) Get(i int) entry {
	i = i - l.LastIncludedIndex

	return l.Entries[i]
}

func (l *Log) GetTerm(i int) int {
	if i <= l.LastIncludedIndex {
		return l.LastIncludedTerm
	}

	return l.Get(i).Term
}

func (l *Log) GetLastEntryState() (index, term int) {
	length := len(l.Entries)

	if length == 1 {
		return l.LastIncludedIndex, l.LastIncludedTerm
	}

	return l.LastIncludedIndex + length - 1, l.Entries[length-1].Term
}

func (l *Log) LastIndex() int {

	return l.LastIncludedIndex + len(l.Entries) - 1
}

func (l *Log) LastTerm() int {
	if len(l.Entries) == 1 {
		return l.LastIncludedTerm
	}

	return l.Entries[len(l.Entries)-1].Term
}

func (l *Log) IsExitEntry(index int, term int) bool {
	idx := index - l.LastIncludedIndex

	if idx == 0 {
		return l.LastIncludedTerm == term
	}

	if idx > len(l.Entries)-1 || idx < 0 {
		return false
	}

	return l.Entries[idx].Term == term
}

func (l *Log) Put(index int, items []entry) bool {
	index = index - l.LastIncludedIndex

	if index < 1 {
		return false
	}

	if index < len(l.Entries) {
		l.Entries = l.Entries[:index]
	}

	l.Entries = append(l.Entries, items...)
	return true
}

func (l *Log) Append(command interface{}, term int) int {
	l.Entries = append(l.Entries, entry{
		Command: command,
		Term:    term,
	})

	return l.LastIndex()
}

func (l *Log) SliceCopy(start, end int) []entry {
	start = start - l.LastIncludedIndex
	end = end - l.LastIncludedIndex

	if start < 0 || end < 0 || end-start < 0 {
		log.Fatalf("Unexcept SliceCopy start = %d, end = %d", start, end)
	}

	if end > len(l.Entries) {
		end = len(l.Entries)
	}

	en := make([]entry, end-start)
	copy(en, l.Entries[start:end])
	return en
}

func (l *Log) StartSnapShot(start int) bool {
	if start <= l.LastIncludedIndex || start >= l.LastIncludedIndex+len(l.Entries) {
		return false
	}

	l.LastIncludedTerm = l.GetTerm(start)
	l.Entries = append(l.Entries[:1], l.Entries[start-l.LastIncludedIndex+1:]...)
	l.LastIncludedIndex = start

	return true
}

func (l *Log) ReadSnapShotState() (LastIncludedIndex, LastIncludedTerm int) {
	return l.LastIncludedIndex, l.LastIncludedTerm
}

func (l *Log) SaveSnapShotState(index, term int) {
	l.LastIncludedIndex = index
	l.LastIncludedTerm = term
}

func (l *Log) clear() {
	if len(l.Entries) == 1 {
		return
	}

	l.Entries = l.Entries[:1]
}
