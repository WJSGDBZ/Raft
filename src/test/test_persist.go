package main

func test_persist() {
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(1)
	//e.Encode("2")
	//da := w.Bytes()
	//rf := raft.Persister{}
	//rf.SaveRaftState(da)
	//
	//data := rf.ReadRaftState()
	//if data == nil || len(data) < 1 { // bootstrap without any state?
	//	return
	//}
	//// Your code here (2C).
	//r := bytes.NewBuffer(data)
	//d := labgob.NewDecoder(r)
	//var voteFor int
	//var term string
	//if d.Decode(&voteFor) != nil ||
	//	d.Decode(&term) != nil {
	//	panic("fail to read machine state")
	//} else {
	//	fmt.Printf("voteFor = %v, term = %v", voteFor, term)
	//}
}
