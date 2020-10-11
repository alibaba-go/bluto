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

// PingOption define option interface for redis Ping command.
type PingOption interface {
	pingOption() []interface{}
}

// PingOptionMessage returns a copy of the message.
type PingOptionMessage struct {
	Message string
}

// pingOption satisfies pingOption interface.
func (po PingOptionMessage) pingOption() []interface{} {
	return []interface{}{po.Message}
}

// FlushAllOption define option interface for redis FlushAll command.
type FlushAllOption interface {
	flushAllOption() []interface{}
}

// FlushAllOptionAsync let the entire dataset or a single database to be freed asynchronously.
type FlushAllOptionAsync struct {
}

// flushAllOption satisfies FlushAllOption interface.
func (fo FlushAllOptionAsync) flushAllOption() []interface{} {
	return []interface{}{"ASYNC"}
}

// SetOption define option interface for redis Set command.
type SetOption interface {
	setOption() []interface{}
}

// SetOptionEX (EX seconds) Set the specified expire time, in seconds.
type SetOptionEX struct {
	EX uint64
}

// setOption satisfies setOption interface.
func (so SetOptionEX) setOption() []interface{} {
	return []interface{}{"EX", so.EX}
}

// SetOptionPX (PX milliseconds) Set the specified expire time, in milliseconds.
type SetOptionPX struct {
	PX uint64
}

// setOption satisfies setOption interface.
func (so SetOptionPX) setOption() []interface{} {
	return []interface{}{"PX", so.PX}
}

// SetOptionNX Only set the key if it does not already exist.
type SetOptionNX struct {
}

// setOption satisfies setOption interface.
func (so SetOptionNX) setOption() []interface{} {
	return []interface{}{"NX"}
}

// SetOptionXX Only set the key if it already exist.
type SetOptionXX struct {
}

// setOption satisfies setOption interface.
func (so SetOptionXX) setOption() []interface{} {
	return []interface{}{"XX"}
}

// SetOptionKeepTTL (Redis>=6.0) Retain the time to live associated with the key.
type SetOptionKeepTTL struct {
}

// setOption satisfies setOption interface.
func (so SetOptionKeepTTL) setOption() []interface{} {
	return []interface{}{"KEEPTTL"}
}

// XAddOption define option interface for redis XADD command.
type XAddOption interface {
	xaddOption() []interface{}
}

// XAddOptionMaxLen limit the size of the stream to a maximum number of elements.
type XAddOptionMaxLen struct {
	MaxLen      uint64
	Approximate bool
}

// xaddOption satisfies xaddOption interface.
func (xo XAddOptionMaxLen) xaddOption() []interface{} {
	option := []interface{}{"MAXLEN"}
	if xo.Approximate {
		option = append(option, "~")
	}
	option = append(option, xo.MaxLen)
	return option
}

// XReadOption define option interface for redis XREAD command.
type XReadOption interface {
	xreadOption() []interface{}
}

// XReadOptionCount set maximum return count elements per stream.
type XReadOptionCount struct {
	Count uint64
}

// xreadOption satisfies xreadOption interface.
func (xo XReadOptionCount) xreadOption() []interface{} {
	return []interface{}{"COUNT", xo.Count}
}

// XReadOptionBlock (Block milliseconds) set block duartion if items are not available.
type XReadOptionBlock struct {
	Block uint64
}

// xreadOption satisfies xreadOption interface
func (xo XReadOptionBlock) xreadOption() []interface{} {
	return []interface{}{"BLOCK", xo.Block}
}

// XGroupCreateOption define option interface for redis XGROUP CREATE command.
type XGroupCreateOption interface {
	xgroupCreateOption() []interface{}
}

// XGroupCreateOptionMKStream creates stream if does not exists.
type XGroupCreateOptionMKStream struct {
}

// xgroupCreateOption satisfies xgroupCreateOption interface.
func (xo XGroupCreateOptionMKStream) xgroupCreateOption() []interface{} {
	return []interface{}{"MKSTREAM"}
}

// XReadGroupOption define option interface for redis XREADGROUP command.
type XReadGroupOption interface {
	xreadGroupOption() []interface{}
}

// XReadGroupOptionCount set maximum return count elements per stream.
type XReadGroupOptionCount struct {
	Count uint64
}

// xreadGroupOption satisfies xreadGroupOption interface.
func (xo XReadGroupOptionCount) xreadGroupOption() []interface{} {
	return []interface{}{"COUNT", xo.Count}
}

// XReadGroupOptionBlock (Block milliseconds) set block duartion if items are not available.
type XReadGroupOptionBlock struct {
	Block uint64
}

// xreadGroupOption satisfies xreadGroupOption interface.
func (xo XReadGroupOptionBlock) xreadGroupOption() []interface{} {
	return []interface{}{"BLOCK", xo.Block}
}

// XReadGroupOptionNoAck avoid adding the message to the PEL.
type XReadGroupOptionNoAck struct {
}

// xreadGroupOption satisfies xreadGroupOption interface.
func (xo XReadGroupOptionNoAck) xreadGroupOption() []interface{} {
	return []interface{}{"NOACK"}
}

// XClaimOption define option interface for redis XCLAIM command.
type XClaimOption interface {
	xclaimOption() []interface{}
}

// XClaimOptionIdle (Idle milliseconds) set the idle time (last time it was delivered) of the message.
type XClaimOptionIdle struct {
	Idle uint64
}

// xclaimOption satisfies xclaimOption interface.
func (xo XClaimOptionIdle) xclaimOption() []interface{} {
	return []interface{}{"IDLE", xo.Idle}
}

// XClaimOptionTime (Time milliseconds) is the same as IDLE but instead of a relative amount of milliseconds, it sets the idle time to a specific Unix time
type XClaimOptionTime struct {
	Time uint64
}

// xclaimOption satisfies xclaimOption interface.
func (xo XClaimOptionTime) xclaimOption() []interface{} {
	return []interface{}{"Time", xo.Time}
}

// XClaimOptionRetryCount set the retry counter to the specified value.
type XClaimOptionRetryCount struct {
	RetryCount uint64
}

// xclaimOption satisfies xclaimOption interface.
func (xo XClaimOptionRetryCount) xclaimOption() []interface{} {
	return []interface{}{"RETRYCOUNT", xo.RetryCount}
}

// XClaimOptionForce creates the pending message entry in the PEL even if certain specified IDs are not already in the PEL assigned to a different client.
type XClaimOptionForce struct {
}

// xclaimOption satisfies xclaimOption interface.
func (xo XClaimOptionForce) xclaimOption() []interface{} {
	return []interface{}{"FORCE"}
}

// XClaimOptionJustID return just an array of IDs of messages successfully claimed, without returning the actual message.
type XClaimOptionJustID struct {
}

// xclaimOption satisfies xclaimOption interface.
func (xo XClaimOptionJustID) xclaimOption() []interface{} {
	return []interface{}{"JUSTID"}
}

// XPendingOption define option interface for redis XPENDING command.
type XPendingOption interface {
	xpendingOption() []interface{}
}

// XPendingOptionStartEndCount define pass a range of IDs, and a non optional count argument.
type XPendingOptionStartEndCount struct {
	StartID string
	EndID   string
	Count   uint64
}

// xpendingOption satisfies xpendingOption interface.
func (xo XPendingOptionStartEndCount) xpendingOption() []interface{} {
	return []interface{}{xo.StartID, xo.EndID, xo.Count}
}

// XPendingOptionConsumer summary about the pending messages in a given consumer group
type XPendingOptionConsumer struct {
	Consumer string
}

// xpendingOption satisfies xpendingOption interface.
func (xo XPendingOptionConsumer) xpendingOption() []interface{} {
	return []interface{}{xo.Consumer}
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
func (c *Commander) Expire(result *bool, key string, seconds int) *Commander {
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
func (c *Commander) FlushAll(result *string, options ...FlushAllOption) *Commander {
	cmd := redis.Args{}
	for _, option := range options {
		cmd = cmd.Add(option.flushAllOption()...)
	}
	return c.Command(result, "FLUSHALL", cmd...)
}

// Keys returns all keys matching pattern.
func (c *Commander) Keys(result *[]string, pattern string) *Commander {
	return c.Command(result, "KEYS", pattern)
}

// Ping returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk.
func (c *Commander) Ping(result *string, options ...PingOption) *Commander {
	cmd := redis.Args{}
	for _, option := range options {
		cmd = cmd.Add(option.pingOption()...)
	}
	return c.Command(result, "PING", cmd...)
}

// Set key to hold the string value. If key already holds a value, it is overwritten.
func (c *Commander) Set(result *string, key string, value interface{}, options ...SetOption) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add(key).Add(value)
	for _, option := range options {
		cmd = cmd.Add(option.setOption()...)
	}
	return c.Command(result, "SET", cmd...)
}

// XAdd appends the specified stream entry to the stream at the specified key.
func (c *Commander) XAdd(result *string, streamName, streamID string, fields interface{}, options ...XAddOption) *Commander {
	cmd := redis.Args{}.Add(streamName)
	for _, option := range options {
		cmd = cmd.Add(option.xaddOption()...)
	}
	cmd = cmd.Add(streamID).AddFlat(fields)
	return c.Command(
		result,
		"XADD",
		cmd...,
	)
}

// XGroupCreate is used in order to manage the consumer groups associated with a stream data structure.
func (c *Commander) XGroupCreate(result *string, streamName, groupName, streamID string, options ...XGroupCreateOption) *Commander {
	cmd := redis.Args{}.Add("CREATE").Add(streamName).Add(groupName).Add(streamID)
	for _, option := range options {
		cmd = cmd.Add(option.xgroupCreateOption()...)
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
func (c *Commander) XRead(result interface{}, streamList, idList []string, options ...XReadOption) *Commander {
	cmd := redis.Args{}
	for _, option := range options {
		cmd = cmd.Add(option.xreadOption()...)
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
func (c *Commander) XReadGroup(result interface{}, groupName, consumerName string, streamList, idList []string, options ...XReadGroupOption) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add("GROUP").Add(groupName).Add(consumerName)
	for _, option := range options {
		cmd = cmd.Add(option.xreadGroupOption()...)
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
func (c *Commander) XPending(result interface{}, streamName, groupName string, options ...XPendingOption) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add(streamName)
	cmd = cmd.Add(groupName)
	for _, option := range options {
		cmd = cmd.Add(option.xpendingOption()...)
	}
	return c.Command(
		result,
		"XPENDING",
		cmd...,
	)
}

// XClaim this command changes the ownership of a pending message, so that the new owner is the consumer specified as the command argument.
func (c *Commander) XClaim(result interface{}, streamName, groupName, consumerName string, minIdleTime uint64, idList []string, options ...XClaimOption) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add(streamName)
	cmd = cmd.Add(groupName)
	cmd = cmd.Add(consumerName)
	cmd = cmd.Add(minIdleTime)
	for _, id := range idList {
		cmd = cmd.Add(id)
	}
	for _, option := range options {
		cmd = cmd.Add(option.xclaimOption()...)
	}
	return c.Command(
		result,
		"XCLAIM",
		cmd...,
	)
}

// Exists if key exists
func (c *Commander) Exists(result *int, keys ...string) *Commander {
	iKeys := make([]interface{}, len(keys))
	for i := range keys {
		iKeys[i] = keys[i]
	}
	return c.Command(result, "EXISTS", iKeys...)
}

// HSet sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
func (c *Commander) HSet(result *int, key string, field []string, value []interface{}) *Commander {
	cmd := redis.Args{}
	cmd = cmd.Add(key)
	for index := range field {
		cmd = cmd.Add(field[index])
		cmd = cmd.Add(value[index])
	}
	return c.Command(result, "HSET", cmd...)
}

// HGet Returns the value associated with field in the hash stored at key.
func (c *Commander) HGet(result interface{}, key, field string) *Commander {
	return c.Command(result, "HGET", key, field)
}
