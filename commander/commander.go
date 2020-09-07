package commander

import (
	"time"

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

// SetOption define option args for redis Set command
type SetOption struct {
	EX      uint64 // EX seconds -- Set the specified expire time, in seconds.
	PX      uint64 // PX milliseconds -- Set the specified expire time, in milliseconds.
	NX      bool   // NX -- Only set the key if it does not already exist.
	XX      bool   // XX -- Only set the key if it already exist.
	KeepTTL bool   // KeepTTL -- Retain the time to live associated with the key.
}

// XAddOption define option for redis stream XAdd command
type XAddOption struct {
	MaxLen      uint64
	Approximate bool
}

// XReadOption define option for redis stream XRead command
type XReadOption struct {
	Count uint64
	Block time.Duration
}

//XGroupCreateOption define option for redis stream XGroup Create command
type XGroupCreateOption struct {
	MKStream bool
}

// XReadGroupOption define option for redis stream XReadGroup command
type XReadGroupOption struct {
	Count uint64
	Block time.Duration
	NoAck bool
}

// XClaimOption define option for redis stream XClaim command
type XClaimOption struct {
	Idle       uint64
	Time       uint64
	RetryCount uint64
	Force      bool
	Justid     bool
}

// XPendingOption define option for redis stream XPending command
type XPendingOption struct {
	StartID  string
	EndID    string
	Count    uint64
	Consumer string
}

//FlushAllOption define option for redis stream FLUSHALL command
type FlushAllOption struct {
	Async bool
}

// Command commands the redis connection
func (c *Commander) Command(result interface{}, name string, args ...interface{}) *Commander {
	// if there has been an error don't do anything
	if c.err != nil {
		return c
	}
	// add query result to pending result list
	c.pendingResults = append(c.pendingResults, result)
	// send the command to buffer
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
	// evaluate all pending results
	_, err = redis.Scan(results, c.pendingResults...)
	if err != nil {
		return err
	}
	return nil
}

// Select the Redis logical database having the specified zero-based numeric index.
func (c *Commander) Select(result *string, index int) *Commander {
	return c.Command(result, "SELECT", index)
}

// Get the value of key. If the key does not exist the special value nil is returned.
func (c *Commander) Get(result interface{}, key string) *Commander {
	return c.Command(result, "GET", key)
}

// Expire set a timeout on key. After the timeout has expired, the key will automatically be deleted.
func (c *Commander) Expire(result *int, key string, seconds int) *Commander {
	return c.Command(result, "EXPIRE", key, seconds)
}

// Del removes the specified keys. A key is ignored if it does not exist.
func (c *Commander) Del(result *int, keys ...string) *Commander {
	iKeys := make([]interface{}, len(keys))
	for i := range keys {
		iKeys[i] = keys[i]
	}
	return c.Command(result, "DEL", iKeys...)
}

// Decr decrements the number stored at key by one. If the key does not exist, it is set to 0.
func (c *Commander) Decr(result *int64, key string) *Commander {
	return c.Command(result, "DECR", key)
}

// Incr Increments the number stored at key by one. If the key does not exist, it is set to 0.
func (c *Commander) Incr(result *int64, key string) *Commander {
	return c.Command(result, "INCR", key)
}

// FlushAll delete all the keys of all the existing databases, not just the currently selected one.
func (c *Commander) FlushAll(result *string, options *FlushAllOption) *Commander {
	var optionCmd []interface{}
	if options != nil && options.Async {
		optionCmd = append(optionCmd, "ASYNC")
	}
	return c.Command(result, "FLUSHALL", optionCmd...)
}

// Keys returns all keys matching pattern.
func (c *Commander) Keys(result *[]string, pattern string) *Commander {
	return c.Command(result, "KEYS", pattern)
}

// Ping returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk.
func (c *Commander) Ping(result *string, message string) *Commander {
	var optionCmd []interface{}
	if message != "" {
		optionCmd = append(optionCmd, message)
	}
	return c.Command(result, "PING", optionCmd...)
}

// Set key to hold the string value. If key already holds a value, it is overwritten.
func (c *Commander) Set(result *string, key string, value interface{}, options *SetOption) *Commander {
	command := redis.Args{}
	command = command.Add(key)
	command = command.Add(value)
	if options != nil {
		if options.EX != 0 {
			command = command.Add("EX")
			command = command.Add(options.EX)
		}
		if options.PX != 0 {
			command = command.Add("PX")
			command = command.Add(options.PX)
		}
		if options.NX {
			command = command.Add("NX")
		}
		if options.XX {
			command = command.Add("XX")
		}
		if options.KeepTTL {
			command = command.Add("KEEPTTL")
		}
	}
	return c.Command(result, "SET", command...)
}

// XAdd appends the specified stream entry to the stream at the specified key.
func (c *Commander) XAdd(result *string, streamName, streamID string, fields interface{}, options *XAddOption) *Commander {
	command := redis.Args{}.Add(streamName)
	if options != nil && options.MaxLen != 0 {
		command = command.Add("MAXLEN")
		if options.Approximate {
			command = command.Add("~")
		}
		command = command.Add(options.MaxLen)
	}
	command = command.Add(streamID).AddFlat(fields)
	return c.Command(
		result,
		"XADD",
		command...,
	)
}

// XGroupCreate is used in order to manage the consumer groups associated with a stream data structure.
func (c *Commander) XGroupCreate(result *string, streamName, groupName, streamID string, options *XGroupCreateOption) *Commander {
	cmd := redis.Args{}.Add("CREATE").Add(streamName).Add(groupName).Add(streamID)
	if options != nil && options.MKStream {
		cmd = cmd.Add("MKSTREAM")
	}
	return c.Command(
		result,
		"XGROUP",
		cmd...,
	)
}

// XGroupDestroy is used in order to manage the consumer groups associated with a stream data structure.
func (c *Commander) XGroupDestroy(result *int, streamName, groupName string) *Commander {
	cmd := redis.Args{}.Add("DESTROY").Add(streamName).Add(groupName)
	return c.Command(
		result,
		"XGROUP",
		cmd...,
	)
}

// XGroupDelConsumer is used in order to manage the consumer groups associated with a stream data structure.
func (c *Commander) XGroupDelConsumer(result *int, streamName, groupName, consumerName string) *Commander {
	cmd := redis.Args{}.Add("DELCONSUMER").Add(streamName).Add(groupName).Add(consumerName)
	return c.Command(
		result,
		"XGROUP",
		cmd...,
	)
}

// XRead read data from one or multiple streams, only returning entries with an ID greater than the last received ID reported by the caller.
func (c *Commander) XRead(result interface{}, streamList, idList []string, options *XReadOption) *Commander {
	cmd := redis.Args{}
	if options != nil {
		if options.Count != 0 {
			cmd = cmd.Add("COUNT")
			cmd = cmd.Add(options.Count)
		}
		if options.Block != 0 {
			cmd = cmd.Add("BLOCK")
			cmd = cmd.Add(options.Block.Milliseconds())
		}
	}
	cmd = cmd.Add("STREAMS")
	for _, stream := range streamList {
		cmd = cmd.Add(stream)
	}
	for _, id := range idList {
		cmd = cmd.Add(id)
	}
	return c.Command(
		result,
		"XREAD",
		cmd...,
	)
}

// XReadGroup s a special version of the XREAD command with support for consumer groups.
func (c *Commander) XReadGroup(result interface{}, groupName, consumerName string, streamList, idList []string, options *XReadGroupOption) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add("GROUP")
	cmd = cmd.Add(groupName)
	cmd = cmd.Add(consumerName)
	if options != nil {
		if options.Count != 0 {
			cmd = cmd.Add("COUNT")
			cmd = cmd.Add(options.Count)
		}
		if options.Block != 0 {
			cmd = cmd.Add("BLOCK")
			cmd = cmd.Add(options.Block.Milliseconds())
		}
		if options.NoAck {
			cmd = cmd.Add("NOACK")
		}
	}

	cmd = cmd.Add("STREAMS")
	for _, stream := range streamList {
		cmd = cmd.Add(stream)
	}
	for _, id := range idList {
		cmd = cmd.Add(id)
	}
	return c.Command(
		result,
		"XREADGROUP",
		cmd...,
	)
}

// XAck  removes one or multiple messages from the pending entries list (PEL) of a stream consumer group.
func (c *Commander) XAck(result interface{}, streamName, groupName string, idList []string) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add(streamName)
	cmd = cmd.Add(groupName)
	for _, id := range idList {
		cmd = cmd.Add(id)
	}
	return c.Command(
		result,
		"XACK",
		cmd...,
	)
}

// XPending fetching data from a stream via a consumer group, and not acknowledging such data, has the effect of creating pending entries.
func (c *Commander) XPending(result interface{}, streamName, groupName string, options *XPendingOption) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add(streamName)
	cmd = cmd.Add(groupName)
	if options != nil {
		if options.StartID != "" {
			cmd = cmd.Add(options.StartID)
		}
		if options.EndID != "" {
			cmd = cmd.Add(options.EndID)
		}
		if options.Count != 0 {
			cmd = cmd.Add(options.Count)
		}
		if options.Consumer != "" {
			cmd = cmd.Add(options.Consumer)
		}
	}
	return c.Command(
		result,
		"XPENDING",
		cmd...,
	)
}

// XClaim this command changes the ownership of a pending message, so that the new owner is the consumer specified as the command argument.
func (c *Commander) XClaim(result interface{}, streamName, groupName, consumerName string, minIdleTime time.Duration, idList []string, options *XClaimOption) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add(streamName)
	cmd = cmd.Add(groupName)
	cmd = cmd.Add(consumerName)
	cmd = cmd.Add(minIdleTime.Milliseconds())
	for _, id := range idList {
		cmd = cmd.Add(id)
	}
	if options != nil {
		if options.Idle != 0 {
			cmd = cmd.Add("IDLE")
			cmd = cmd.Add(options.Idle)
		}
		if options.Time != 0 {
			cmd = cmd.Add("TIME")
			cmd = cmd.Add(options.Time)
		}
		if options.RetryCount != 0 {
			cmd = cmd.Add("RETRYCOUNT")
			cmd = cmd.Add(options.RetryCount)
		}
		if options.Force {
			cmd = cmd.Add("force")
		}
		if options.Justid {
			cmd = cmd.Add("justid")
		}
	}

	return c.Command(
		result,
		"XCLAIM",
		cmd...,
	)
}
