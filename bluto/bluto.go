package bluto

import (
	"git.alibaba.ir/rd/zebel-the-sailor-bluto/commander"
	"git.alibaba.ir/rd/zebel-the-sailor-bluto/pooler"
	"github.com/gomodule/redigo/redis"
)

//Bluto create ready to use redis client
type Bluto struct {
	pool *redis.Pool
}

//New create new Bluto instance
func New(poolConfig pooler.Config) (*Bluto, error) {
	pool, err := pooler.GetPool(poolConfig)
	if err != nil {
		return nil, err
	}
	bl := &Bluto{pool: pool}
	return bl, nil
}

//Borrow start redis command
func (bl *Bluto) Borrow() *commander.Commander {
	conn := bl.pool.Get()
	commander := commander.New(conn)
	return commander
}

//Close close redis pool
func (bl *Bluto) Close() error {
	return bl.pool.Close()
}
