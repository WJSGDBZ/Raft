package kvraft

type Database struct {
	M       map[string][]byte
	Command map[string]*bitmap // record each client command id ; bitmap
}

func NewDatabase() *Database {
	db := Database{}
	db.M = make(map[string][]byte)
	db.Command = make(map[string]*bitmap)

	return &db
}

func (d *Database) GetReplicaL(key string) []byte {
	if res, ok := d.M[key]; ok {
		r := make([]byte, len(res))
		copy(r, res)
		return r
	}

	return nil
}

func (d *Database) GetL(key string) []byte {
	if res, ok := d.M[key]; ok {
		return res
	}

	return nil
}

func (d *Database) AppendL(key string, value []byte, opname string, taskId int) {
	if _, ok := d.Command[opname]; !ok {
		d.Command[opname] = NewBitmap()
	}

	d.M[key] = append(d.M[key], value...)
	d.Command[opname].Set(uint64(taskId))
}

func (d *Database) PutL(key string, value []byte, opname string, taskId int) {
	if _, ok := d.Command[opname]; !ok {
		d.Command[opname] = NewBitmap()
	}

	d.M[key] = value
	d.Command[opname].Set(uint64(taskId))
}

func (d *Database) RepeatCommandL(opname string, taskId int) bool {
	if _, ok := d.Command[opname]; !ok {
		return false
	}

	return d.Command[opname].Contain(uint64(taskId))
}
