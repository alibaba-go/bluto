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

//SetOption define option args for redis SET command
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

//SELECT perform redis command
func (c *Commander) SELECT(result *string, index int) *Commander {
	return c.Command(result, "SELECT", index)
}

//GET perform redis command
func (c *Commander) GET(result interface{}, key string) *Commander {
	return c.Command(result, "GET", key)
}

//EXPIRE perform redis command
func (c *Commander) EXPIRE(result interface{}, key string, seconds int) *Commander {
	return c.Command(result, "EXPIRE", key, seconds)
}

//DEL perform redis command
func (c *Commander) DEL(result interface{}, keys ...string) *Commander {
	iKeys := make([]interface{}, len(keys))
	for i := range keys {
		iKeys[i] = keys[i]
	}
	return c.Command(result, "DEL", iKeys...)
}

//DECR perform redis command
func (c *Commander) DECR(result *int, key string) *Commander {
	return c.Command(result, "DECR", key)
}

//INCR perform redis command
func (c *Commander) INCR(result *int, key string) *Commander {
	return c.Command(result, "INCR", key)
}

//FLUSHALL perform redis command
func (c *Commander) FLUSHALL(result *string, async bool) *Commander {
	var optionCmd []interface{}
	if async {
		optionCmd = append(optionCmd, "ASYNC")
	}
	return c.Command(result, "FLUSHALL", optionCmd...)
}

//XADD perform redis command
func (c *Commander) XADD(result *string, streamConfig, field interface{}) *Commander {
	return c.Command(
		result,
		"XADD",
		redis.Args{}.Add(streamConfig).Add("*").AddFlat(&field)...,
	)
}

//KEYS perform redis command
func (c *Commander) KEYS(result *string, pattern string) *Commander {
	return c.Command(result, "KEYS", pattern)
}

//PING perform redis command
func (c *Commander) PING(result *string, message string) *Commander {
	var optionCmd []interface{}
	if message != "" {
		optionCmd = append(optionCmd, message)
	}
	return c.Command(result, "PING", optionCmd)
}

//SET perform redis command
func (c *Commander) SET(result *string, key string, value interface{}, options SetOption) *Commander {
	var optionCmd []interface{}
	if options.EX > 0 {
		optionCmd = append(optionCmd, "EX", options.EX)
	}
	if options.PX > 0 {
		optionCmd = append(optionCmd, "PX", options.PX)
	}
	if options.NX {
		optionCmd = append(optionCmd, "NX")
	}
	if options.XX {
		optionCmd = append(optionCmd, "XX")
	}
	if options.KEEPTTL {
		optionCmd = append(optionCmd, "KEEPTTL")
	}
	return c.Command(result, "SET", []interface{}{key, value, optionCmd}...)
}
