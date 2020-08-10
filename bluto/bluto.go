package bluto

import (
	"git.alibaba.ir/rd/zebel-the-sailor-bluto/commander"
	"github.com/gomodule/redigo/redis"
)

// Bluto is a wrapper over redis pool
type Bluto struct {
	pool *redis.Pool
}

// New creates new Bluto instance
func New(config Config) (*Bluto, error) {
	pool, err := GetPool(config)
	if err != nil {
		return nil, err
	}
	bl := &Bluto{pool: pool}
	return bl, nil
}

// Borrow borrows a redis connection from pool
func (bl *Bluto) Borrow() *commander.Commander {
	conn := bl.pool.Get()
	commander := commander.New(conn)
	return commander
}

// Close closes redis pool
func (bl *Bluto) Close() error {
	return bl.pool.Close()
}
