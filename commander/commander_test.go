package commander_test

import (
	"errors"
	"os"
	"time"

	"git.alibaba.ir/rd/zebel-the-sailor-bluto/bluto"
	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rafaeljusto/redigomock"

	. "git.alibaba.ir/rd/zebel-the-sailor-bluto/commander"
)

var _ = Describe("Commander", func() {

	// --------------------------------- global vars

	var pool *redis.Pool
	var getConn func() redis.Conn

	// --------------------------------- global functions

	var getCorrectConfig = func() bluto.Config {
		address := os.Getenv("REDIS_ADDRESS")
		return bluto.Config{
			Address:               address,
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	// --------------------------------- before and after hooks

	BeforeSuite(func() {
		newPool, err := bluto.GetPool(getCorrectConfig())
		if err != nil {
			panic(err)
		}
		pool = newPool
		getConn = func() redis.Conn {
			return pool.Get()
		}
	})

	AfterSuite(func() {
		err := pool.Close()
		if err != nil {
			panic(err)
		}
	})

	BeforeEach(func() {
		conn := getConn()
		commander := New(conn)
		var flushResult string
		err := commander.
			FlushAll(&flushResult, false).
			Commit()
		if err != nil {
			panic(err)
		}
	})

	// --------------------------------- tests

	Describe("New method", func() {
		It("should return a new commander", func() {
			pool, err := bluto.GetPool(getCorrectConfig())
			defer func() {
				err := pool.Close()
				if err != nil {
					panic(err)
				}
			}()
			if err != nil {
				panic(err)
			}
			conn := pool.Get()
			defer func() {
				err := conn.Close()
				if err != nil {
					panic(err)
				}
			}()
			commander := New(conn)
			Expect(commander).To(Not(BeNil()))
			Expect(commander).To(BeAssignableToTypeOf(&Commander{}))
		})
	})

	Describe("Set", func() {
		It("should return the real results of a valid SET", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			commander := New(conn)
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{NX: true}).
				Commit()
			conn = getConn()
			var getResult int
			errSend := conn.Send("GET", key)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &getResult)
			if err != nil {
				panic(err)
			}
			conn.Close()
			Expect(errSend).To(BeNil())
			Expect(err).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(9))
		})

		It("should return the results of a valid SET", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			commander := New(conn)
			key := "SomeKey"
			value := 9
			conn.Command("SET", key, value).Expect("OK")
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{}).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
		})

		It("should return the results of a valid SET With option", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			commander := New(conn)
			key := "SomeKey"
			value := 9
			conn.Command("SET", key, value, "EX", 10, "NX", "KEEPTTL").Expect("OK")
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{EX: 10, NX: true, KEEPTTL: true}).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))

			conn = redigomock.NewConn()
			setResult = ""
			conn.Command("SET", key, value, "PX", 10000, "XX").Expect("OK")
			commander = New(conn)
			cmdErr = commander.
				Set(&setResult, key, value, SetOption{PX: 10000, XX: true}).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
		})

		It("should return the error of a invalid SET option", func() {
			syntaxErr := errors.New("ERR syntax error")
			conn := redigomock.NewConn()
			defer conn.Close()
			commander := New(conn)
			key := "SomeKey"
			value := 9
			conn.Command("SET", key, value, "NX", "XX").ExpectError(syntaxErr)
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{NX: true, XX: true}).
				Commit()
			Expect(cmdErr).To(Equal(syntaxErr))
			Expect(setResult).To(Equal(""))

			conn = redigomock.NewConn()
			setResult = ""
			conn.Command("SET", key, value, "EX", 1, "PX", 1000).ExpectError(syntaxErr)
			commander = New(conn)
			cmdErr = commander.
				Set(&setResult, key, value, SetOption{EX: 1, PX: 1000}).
				Commit()
			Expect(cmdErr).To(Equal(syntaxErr))
			Expect(setResult).To(Equal(""))
		})
	})

	Describe("Get", func() {
		It("should return the real results of a valid GET", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &setResult)
			if err != nil {
				panic(err)
			}
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var getResult int
			cmdErr := commander.
				Get(&getResult, key).
				Commit()
			Expect(errSend).To(BeNil())
			Expect(err).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(9))
		})

		It("should return the results of a valid GET", func() {
			key := "SomeKey"
			value := int64(9)
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("GET", key).Expect(value)
			commander := New(conn)
			var getResult int64
			cmdErr := commander.Get(&getResult, key).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(getResult).To(Equal(value))

			value2 := "SomeValue"
			conn = redigomock.NewConn()
			conn.Command("GET", key).Expect(value2)
			commander = New(conn)
			var getResult2 string
			cmdErr = commander.Get(&getResult2, key).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(getResult2).To(Equal(value2))
		})

		It("should return the error of a invalid GET", func() {
			key := "SomeKey"
			value := "SomeValue"
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("GET", key).Expect(value)
			commander := New(conn)
			var getResult int64
			cmdErr := commander.Get(&getResult, key).Commit()
			Expect(cmdErr).To(Equal(errors.New("redigo.Scan: cannot assign to dest 0: cannot convert from Redis simple string to *int64")))
			Expect(getResult).To(Equal(int64(0)))
		})
	})

	Describe("Select", func() {

		It("should return the real results of a valid SELECT", func() {
			conn := getConn()
			commander := New(conn)
			var selectResult string
			cmdErr := commander.
				Select(&selectResult, 3).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
		})

		It("should return the results of a valid SELECT", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("SELECT", 0).Expect("OK")
			commander := New(conn)
			var selectResult string
			cmdErr := commander.Select(&selectResult, 0).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
		})

		It("should return the error of a invalid SELECT", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("SELECT", -1).ExpectError(errors.New("ERR DB index is out of range"))
			commander := New(conn)
			var selectResult string
			cmdErr := commander.Select(&selectResult, -1).Commit()
			Expect(cmdErr).To(Equal(errors.New("ERR DB index is out of range")))
			Expect(selectResult).To(Equal(""))
		})
	})

	Describe("Expire", func() {
		It("should return the real results of a valid GET", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &setResult)
			if err != nil {
				panic(err)
			}
			Expect(errSend).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var expireResult int
			cmdErr := commander.Expire(&expireResult, key, 1).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(expireResult).To(Equal(1))

			time.Sleep(1100 * time.Millisecond)

			conn = getConn()
			var getResult int
			errSend = conn.Send("GET", key)
			results, err = redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &getResult)
			if err != nil {
				panic(err)
			}
			conn.Close()
			Expect(errSend).To(BeNil())
			Expect(err).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(getResult).To(Equal(0))
		})

		It("should return the results of a valid EXPIRE", func() {
			key := "SomeKey"
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("EXPIRE", key, 5).Expect(int64(1))
			commander := New(conn)
			var expireResult int
			cmdErr := commander.Expire(&expireResult, key, 5).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(expireResult).To(Equal(1))

			conn = redigomock.NewConn()
			conn.Command("EXPIRE", "NotExistKey", 5).Expect(int64(0))
			commander = New(conn)
			var expireResult2 int
			cmdErr = commander.Expire(&expireResult2, "NotExistKey", 5).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(expireResult2).To(Equal(0))
		})
	})

	Describe("Del", func() {
		It("should return the real results of a valid DEL", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &setResult)
			if err != nil {
				panic(err)
			}
			Expect(errSend).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var delResult int
			cmdErr := commander.Del(&delResult, key).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(delResult).To(Equal(1))

			conn = getConn()
			var getResult int
			errSend = conn.Send("GET", key)
			results, err = redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &getResult)
			if err != nil {
				panic(err)
			}
			conn.Close()
			Expect(errSend).To(BeNil())
			Expect(err).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(getResult).To(Equal(0))
		})

		It("should return the results of a valid DEL", func() {
			key := "SomeKey"
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("DEL", key, "NotExistKey").Expect(int64(1))
			commander := New(conn)
			var delResult int
			cmdErr := commander.Del(&delResult, key, "NotExistKey").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(delResult).To(Equal(1))

			conn = redigomock.NewConn()
			conn.Command("DEL", "NotExistKey").Expect(int64(0))
			commander = New(conn)
			var delResult2 int
			cmdErr = commander.Del(&delResult2, "NotExistKey").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(delResult2).To(Equal(0))
		})
	})

	Describe("Incr", func() {
		It("should return the real results of a valid INCR", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &setResult)
			if err != nil {
				panic(err)
			}
			Expect(errSend).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var incrResult int
			cmdErr := commander.Incr(&incrResult, key).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(incrResult).To(Equal(10))

			conn = getConn()
			var getResult int
			errSend = conn.Send("GET", key)
			results, err = redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &getResult)
			if err != nil {
				panic(err)
			}
			conn.Close()
			Expect(errSend).To(BeNil())
			Expect(err).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(getResult).To(Equal(10))
		})

		It("should return the results of a valid INCR", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("INCR", "ValueEqual10").Expect(int64(11))
			commander := New(conn)
			var delResult int
			cmdErr := commander.Incr(&delResult, "ValueEqual10").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(delResult).To(Equal(11))
		})

		It("should return the error of a invalid INCR", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("INCR", "ValueEqualString").
				ExpectError(errors.New("ERR value is not an integer or out of range"))
			commander := New(conn)
			var delResult int
			cmdErr := commander.Incr(&delResult, "ValueEqualString").Commit()
			Expect(cmdErr).To(Equal(errors.New("ERR value is not an integer or out of range")))
			Expect(delResult).To(Equal(0))
		})
	})

	Describe("Decr", func() {
		It("should return the real results of a valid DECR", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &setResult)
			if err != nil {
				panic(err)
			}
			Expect(errSend).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var decrResult int
			cmdErr := commander.Decr(&decrResult, key).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(decrResult).To(Equal(8))

			conn = getConn()
			var getResult int
			errSend = conn.Send("GET", key)
			results, err = redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &getResult)
			if err != nil {
				panic(err)
			}
			conn.Close()
			Expect(errSend).To(BeNil())
			Expect(err).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(getResult).To(Equal(8))
		})

		It("should return the results of a valid DECR", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("DECR", "ValueEqual10").Expect(int64(9))
			commander := New(conn)
			var delResult int
			cmdErr := commander.Decr(&delResult, "ValueEqual10").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(delResult).To(Equal(9))
		})

		It("should return the error of a invalid DECR", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("DECR", "ValueEqualString").
				ExpectError(errors.New("ERR value is not an integer or out of range"))
			commander := New(conn)
			var delResult int
			cmdErr := commander.Decr(&delResult, "ValueEqualString").Commit()
			Expect(cmdErr).To(Equal(errors.New("ERR value is not an integer or out of range")))
			Expect(delResult).To(Equal(0))
		})
	})

	Describe("FlushAll", func() {
		It("should return the real results of a valid DECR", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &setResult)
			if err != nil {
				panic(err)
			}
			Expect(errSend).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var flushResult string
			cmdErr := commander.FlushAll(&flushResult, true).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(flushResult).To(Equal("OK"))

			conn = getConn()
			var getResult int
			errSend = conn.Send("GET", key)
			results, err = redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &getResult)
			if err != nil {
				panic(err)
			}
			conn.Close()
			Expect(errSend).To(BeNil())
			Expect(err).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(getResult).To(Equal(0))
		})

		It("should return the results of a valid FLUSHALL", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("FLUSHALL", "ASYNC").Expect("OK")
			commander := New(conn)
			var flushResult string
			cmdErr := commander.FlushAll(&flushResult, true).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(flushResult).To(Equal("OK"))
		})
	})

	Describe("Keys", func() {
		It("should return the real results of a valid KEYS", func() {
			key1 := "SomeKey1"
			value1 := 9
			key2 := "SomeKey2"
			value2 := "SomeValue"
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key1, value1)
			errSend = conn.Send("SET", key2, value2)
			results, err := redis.Values(conn.Do(""))
			_, err = redis.Scan(results, &setResult)
			if err != nil {
				panic(err)
			}
			Expect(errSend).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var keysResult []string
			cmdErr := commander.Keys(&keysResult, "*Key*").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(keysResult).To(ContainElements("SomeKey1", "SomeKey2"))
		})

		It("should return the results of a valid KEYS", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("KEYS", "*pattern*").
				ExpectStringSlice("1pattern", "pattern1", "1pattern1")
			commander := New(conn)
			var keysResult []string
			cmdErr := commander.Keys(&keysResult, "*pattern*").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(keysResult).To(ContainElements("1pattern", "pattern1", "1pattern1"))
		})
	})

	Describe("Ping", func() {
		It("should return the real results of a valid PING", func() {
			conn := getConn()
			commander := New(conn)
			var pingResult string
			cmdErr := commander.Ping(&pingResult, "PingMsg").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(pingResult).To(Equal("PingMsg"))
		})

		It("should return the results of a valid PING", func() {
			conn := redigomock.NewConn()
			defer conn.Close()
			conn.Command("PING").Expect("PingMsg")
			commander := New(conn)
			var pingResult string
			cmdErr := commander.Ping(&pingResult, "").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(pingResult).To(Equal("PingMsg"))
		})
	})

	Describe("XAdd", func() {
		It("should return the results of a valid XAdd", func() {
		})
	})

	Describe("Integration test command and commit", func() {

		// --------------------------------- tests

		It("should return the results of a valid chain of expire commands", func() {
			conn := getConn()
			commander := New(conn)
			key := "SomeKey"
			var selectResult string
			var setResult string
			var expireResult1 int
			var expireResult2 int
			var getResult1 int
			var getResult2 int

			cmdErr := commander.
				Select(&selectResult, 0).
				Set(&setResult, key, 9, SetOption{}).
				Expire(&expireResult1, key, 1).
				Expire(&expireResult2, "NotExistKey", 1).
				Get(&getResult1, key).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(expireResult1).To(Equal(1))
			Expect(expireResult2).To(Equal(0))
			Expect(getResult1).To(Equal(9))

			//sleep 2 second to expire key
			time.Sleep(2 * time.Second)

			conn = getConn()
			commander = New(conn)
			cmdErr = commander.
				Select(&selectResult, 0).
				Get(&getResult2, key).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(getResult2).To(Equal(0))
		})

		It("should return the results of a valid chain of del and flush commands", func() {
			conn := getConn()
			commander := New(conn)
			key1 := "SomeKey1"
			key2 := "SomeKey2"
			var selectResult string
			var setResult1 string
			var setResult2 string
			var keysResult []string
			var delResult int
			var getResult1 int
			var getResult2 int
			var flushResult string

			cmdErr := commander.
				Select(&selectResult, 0).
				Set(&setResult1, key1, 9, SetOption{}).
				Set(&setResult2, key2, 9, SetOption{}).
				Keys(&keysResult, "*Key*").
				Del(&delResult, key1, "NotExistKey").
				Get(&getResult1, key1).
				FlushAll(&flushResult, true).
				Get(&getResult2, key2).
				Commit()

			Expect(cmdErr).To(BeNil())
			Expect(setResult1).To(Equal("OK"))
			Expect(setResult2).To(Equal("OK"))
			Expect(keysResult).To(ContainElements("SomeKey1", "SomeKey2"))
			Expect(delResult).To(Equal(1))
			Expect(getResult1).To(Equal(0))
			Expect(flushResult).To(Equal("OK"))
			Expect(getResult2).To(Equal(0))
		})

		It("should return the results of a valid chain of commands", func() {
			conn := getConn()
			commander := New(conn)

			key := "someKey"
			pingMsg := "PingMessage"
			var selectResult string
			var setResult string
			var incrResult int
			var getResult int
			var decrResult int
			var pingResult string

			cmdErr := commander.
				Select(&selectResult, 0).
				Set(&setResult, key, 9, SetOption{}).
				Incr(&incrResult, key).
				Get(&getResult, key).
				Decr(&decrResult, key).
				Ping(&pingResult, pingMsg).
				Commit()

			Expect(cmdErr).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
			Expect(setResult).To(Equal("OK"))
			Expect(incrResult).To(Equal(10))
			Expect(getResult).To(Equal(10))
			Expect(decrResult).To(Equal(9))
			Expect(pingResult).To(Equal(pingMsg))
		})

		It("should return the errors of an invalid chain of commands", func() {
			conn := getConn()
			commander := New(conn)

			key := "someKey"

			var selectResult string
			var setResult string
			var nonExistentResult interface{}
			var incrResult int
			var getResult int

			cmdErr := commander.
				Select(&selectResult, 0).
				Set(&setResult, key, 9, SetOption{}).
				Command(&nonExistentResult, "SOMENONEXISTENTCOMMAND", key, 9).
				Incr(&incrResult, key).
				Get(&getResult, key).
				Commit()

			Expect(cmdErr).To(Not(BeNil()))
			Expect(selectResult).To(Equal("OK"))
			Expect(setResult).To(Equal("OK"))
			Expect(nonExistentResult).To(BeNil())
			Expect(incrResult).To(Equal(0))
			Expect(getResult).To(Equal(0))
		})
	})

})
