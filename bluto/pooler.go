package bluto

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
)

// GetPool returns a redis connection pool
// which the users can use to borrows a connection from the pool
func GetPool(config Config) (*redis.Pool, error) {
	// TODO: use reflect to set the defaults
	// set defaults
	if config.Network == "" {
		config.Network = "tcp"
	}
	if config.ConnectTimeoutSeconds == 0 {
		config.ConnectTimeoutSeconds = 5
	}
	if config.ReadTimeoutSeconds == 0 {
		config.ReadTimeoutSeconds = 5
	}
	if config.WriteTimeoutSeconds == 0 {
		config.WriteTimeoutSeconds = 5
	}
	if config.KeepAliveSeconds == 0 {
		config.KeepAliveSeconds = 300
	}
	if config.MaxIdle == 0 {
		config.MaxIdle = 10
	}
	if config.MaxActive == 0 {
		config.MaxActive = 10
	}
	if config.IdleTimeoutSeconds == 0 {
		config.IdleTimeoutSeconds = 60
	}
	if config.MaxConnLifetimeSeconds == 0 {
		config.MaxConnLifetimeSeconds = 120
	}

	// time based dial options
	connectTimeout := time.Duration(config.ConnectTimeoutSeconds) * time.Second
	readTimeout := time.Duration(config.ReadTimeoutSeconds) * time.Second
	writeTimeout := time.Duration(config.WriteTimeoutSeconds) * time.Second
	keepAlive := time.Duration(config.KeepAliveSeconds) * time.Second

	// time based pool options
	idleTimeout := time.Duration(config.IdleTimeoutSeconds) * time.Second
	maxConnLifetime := time.Duration(config.MaxConnLifetimeSeconds) * time.Second

	// create the redis connection pool
	pool := &redis.Pool{
		// Dial is used for creating and configuring a connection.
		Dial: func() (redis.Conn, error) {
			return redis.Dial(
				config.Network,
				config.Address,
				redis.DialPassword(config.Password),
				redis.DialConnectTimeout(connectTimeout),
				redis.DialReadTimeout(readTimeout),
				redis.DialWriteTimeout(writeTimeout),
				redis.DialKeepAlive(keepAlive),
			)
		},
		// TestOnBorrow is optional and is used for checking
		// the health of an idle connection before the connection is used again by
		// the application. Argument t is the time that the connection was returned
		// to the pool. If the function returns an error, then the connection is
		// closed.
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			// keep alive 0 means the connection will be closed after each command
			if keepAlive == 0 {
				return errors.New("Connection is closed after each command")
			}
			// the connection hasn't been used in less than keep alive: it's alive
			if keepAlive > time.Second && time.Since(t) < keepAlive-time.Second {
				return nil
			}
			// the connection has been used in more than keep alive: it may be alive
			// check if it's alive
			_, err := c.Do("Ping")
			return err
		},
		// Maximum number of idle connections in the pool.
		MaxIdle: config.MaxIdle,
		// Maximum number of connections allocated by the pool at a given time.
		// When zero, there is no limit on the number of connections in the pool.
		MaxActive: config.MaxActive,
		// Close connections after remaining idle for this duration. If the value
		// is zero, then idle connections are not closed. Applications should set
		// the timeout to a value less than the server's timeout.
		IdleTimeout: idleTimeout,
		// If Wait is true and the pool is at the MaxActive limit, then Get() waits
		// for a connection to be returned to the pool before returning.
		Wait: true,
		// Close connections older than this duration. If the value is zero, then
		// the pool does not close connections based on age.
		MaxConnLifetime: maxConnLifetime,
	}
	return pool, nil
}
