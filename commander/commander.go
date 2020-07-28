package commander

import "github.com/gomodule/redigo/redis"

// Commander provides a means to command redigo easily
type Commander struct {
	conn           redis.Conn
	pendingResults []interface{}
	err            error
}

// New returns a new commander
func New(conn redis.Conn) *Commander {
	return &Commander{
		conn: conn,
	}
}

// Command commands the redis connection
func (c *Commander) Command(result interface{}, name string, args ...interface{}) *Commander {
	result = nil
	// if there has been an error don't do anything
	if c.err != nil {
		return c
	}
	//add query result to pending result list
	c.pendingResults = append(c.pendingResults, result)
	// send the command
	c.err = c.conn.Send(name, args...)
	return c
}

// Commit returns the results of all the commands
func (c *Commander) Commit() error {
	defer c.conn.Close()
	// if there has been an error don't do anything
	if c.err != nil {
		return c.err
	}
	// execute the commands
	results, err := redis.Values(c.conn.Do(""))
	if err != nil {
		return err
	}
	//evaluate all pending results
	_, err = redis.Scan(results, c.pendingResults...)
	if err != nil {
		return err
	}
	return nil
}
