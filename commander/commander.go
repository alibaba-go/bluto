package commander

import (
	"github.com/gomodule/redigo/redis"
)

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

//SetOption define option args for redis Set command
type SetOption struct {
	EX      int   //EX seconds -- Set the specified expire time, in seconds.
	PX      int64 //PX milliseconds -- Set the specified expire time, in milliseconds.
	NX      bool  //NX -- Only set the key if it does not already exist.
	XX      bool  //XX -- Only set the key if it already exist.
	KEEPTTL bool  //KEEPTTL -- Retain the time to live associated with the key.
}

// Command commands the redis connection
func (c *Commander) Command(result interface{}, name string, args ...interface{}) *Commander {
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

//Select perform redis command
func (c *Commander) Select(result *string, index int) *Commander {
	return c.Command(result, "SELECT", index)
}

//Get perform redis command
func (c *Commander) Get(result interface{}, key string) *Commander {
	return c.Command(result, "GET", key)
}

//Expire perform redis command
func (c *Commander) Expire(result *int, key string, seconds int) *Commander {
	return c.Command(result, "EXPIRE", key, seconds)
}

//Del perform redis command
func (c *Commander) Del(result *int, keys ...string) *Commander {
	iKeys := make([]interface{}, len(keys))
	for i := range keys {
		iKeys[i] = keys[i]
	}
	return c.Command(result, "DEL", iKeys...)
}

//Decr perform redis command
func (c *Commander) Decr(result *int, key string) *Commander {
	return c.Command(result, "DECR", key)
}

//Incr perform redis command
func (c *Commander) Incr(result *int, key string) *Commander {
	return c.Command(result, "INCR", key)
}

//FlushAll perform redis command
func (c *Commander) FlushAll(result *string, async bool) *Commander {
	var optionCmd []interface{}
	if async {
		optionCmd = append(optionCmd, "ASYNC")
	}
	return c.Command(result, "FLUSHALL", optionCmd...)
}

//XAdd perform redis command
func (c *Commander) XAdd(result *string, streamConfig, field interface{}) *Commander {
	return c.Command(
		result,
		"XADD",
		redis.Args{}.Add(streamConfig).Add("*").AddFlat(&field)...,
	)
}

//Keys perform redis command
func (c *Commander) Keys(result *[]string, pattern string) *Commander {
	return c.Command(result, "KEYS", pattern)
}

//Ping perform redis command
func (c *Commander) Ping(result *string, message string) *Commander {
	var optionCmd []interface{}
	if message != "" {
		optionCmd = append(optionCmd, message)
	}
	return c.Command(result, "PING", optionCmd)
}

//Set perform redis command
func (c *Commander) Set(result *string, key string, value interface{}, options SetOption) *Commander {
	command := []interface{}{key, value}
	if options.EX > 0 {
		command = append(command, "EX", options.EX)
	}
	if options.PX > 0 {
		command = append(command, "PX", options.PX)
	}
	if options.NX {
		command = append(command, "NX")
	}
	if options.XX {
		command = append(command, "XX")
	}
	if options.KEEPTTL {
		command = append(command, "KEEPTTL")
	}
	return c.Command(result, "SET", command...)
}
