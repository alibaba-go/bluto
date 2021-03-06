package bluto

import (
	"github.com/alibaba-go/bluto/commander"
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

// ClosePool closes redis pool
func (bl *Bluto) ClosePool() error {
	return bl.pool.Close()
}
