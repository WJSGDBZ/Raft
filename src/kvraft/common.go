package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

func (e *Err) toString() string {
	return string(*e)
}

func (e *Err) Err(str string) {
	*e = Err(str)
}

// PutAppendArgs PutL or AppendL
type PutAppendArgs struct {
	Key   string
	Value []byte
	Op    int // "PutL" or "AppendL"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CID    int64
	TaskId int
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CID    int64
	TaskId int
}

type GetReply struct {
	Err      Err
	Value    []byte
	LeaderId int
}
