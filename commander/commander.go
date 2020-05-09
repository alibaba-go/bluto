package commander

import "github.com/gomodule/redigo/redis"

// Commander provides a means to command redigo easily
type Commander struct {
	conn redis.Conn
	err  error
}

// New returns a new commander
func New(conn redis.Conn) *Commander {
	return &Commander{
		conn: conn,
	}
}

// Command commands the redis connection
func (c *Commander) Command(name string, args ...interface{}) *Commander {
	// if there has been an error don't do anything
	if c.err != nil {
		return c
	}

	// send the command
	c.err = c.conn.Send(name, args...)
	return c
}

// Commit returns the results of all the commands
func (c *Commander) Commit() ([]interface{}, error) {
	// if there has been an error don't do anything
	if c.err != nil {
		return nil, c.err
	}

	// execute the commands
	results, err := redis.Values(c.conn.Do(""))
	defer c.conn.Close()
	if err != nil {
		return nil, err
	}
	return results, nil
}
