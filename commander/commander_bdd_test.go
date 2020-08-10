package commander_test

import (
	"os"
	"time"

	"git.alibaba.ir/rd/zebel-the-sailor-bluto/bluto"
	"github.com/bxcodec/faker/v3"
	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "git.alibaba.ir/rd/zebel-the-sailor-bluto/commander"
)

var _ = Describe("Commander", func() {

	// --------------------------------- global vars

	var pool *redis.Pool
	var getConn func() redis.Conn
	var getWrongConn func() redis.Conn

	// --------------------------------- global functions

	var getCorrectConfig = func() bluto.Config {
		address := os.Getenv("REDIS_ADDRESS")
		return bluto.Config{
			Address:               address,
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	var getWrongConfig = func() bluto.Config {
		return bluto.Config{
			Address:               "invalidAddress:1234",
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
		wrongPool, _ := bluto.GetPool(getWrongConfig())
		getWrongConn = func() redis.Conn {
			return wrongPool.Get()
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
		It("should return the results of a valid SET", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{}).
				Commit()
			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errRsult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errRsult).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value))
		})

		It("should return the results of a expired SET with EX option", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{EX: 1}).
				Commit()
			time.Sleep(1100 * time.Millisecond)
			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errRsult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errRsult).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
		})

		It("should return the results of a expired SET with PX option", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{PX: 1000}).
				Commit()
			time.Sleep(1100 * time.Millisecond)
			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errRsult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errRsult).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
		})

		It("should return the results of a expired SET with NX option", func() {
			key := "SomeKey"
			value := faker.Word()
			newValue := 10
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var newSetResult string
			cmdErr := commander.
				Set(&setResult, key, newValue, SetOption{NX: true}).
				Commit()
			conn = getConn()
			var getResult string
			newErrSend := conn.Send("GET", key)
			results, newErrResult := redis.Values(conn.Do(""))
			_, newErrScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(newErrSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(newErrResult).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(newErrScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(newSetResult).To(Equal(""))
			Expect(getResult).To(Equal(value))
		})

		It("should return the results of a expired SET with XX option", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			cmdErr := commander.
				Set(&setResult, key, value, SetOption{XX: true}).
				Commit()

			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal(""))
			Expect(getResult).To(Equal(""))
		})

	})

	Describe("Get", func() {
		It("should return the results of a valid GET", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var getResult string
			cmdErr := commander.
				Get(&getResult, key).
				Commit()
			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value))
		})
	})

	Describe("Select", func() {
		It("should return the results of a valid SELECT", func() {
			conn := getConn()
			commander := New(conn)
			var selectResult string
			cmdErr := commander.
				Select(&selectResult, 3).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
		})
	})

	Describe("Expire", func() {
		It("should return the results of a valid EXPIRE", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var expireResult int
			cmdErr := commander.Expire(&expireResult, key, 1).Commit()
			time.Sleep(1100 * time.Millisecond)
			conn = getConn()
			var getResult string
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(cmdErr).To(BeNil())
			Expect(expireResult).To(Equal(1))
			Expect(errSend).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(getResult).To(Equal(""))
		})
	})

	Describe("Del", func() {
		It("should return the results of a valid DEL", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var delResult int
			cmdErr := commander.Del(&delResult, key).Commit()
			Expect(cmdErr).To(BeNil())
			Expect(delResult).To(Equal(1))
			conn = getConn()
			var getResult string
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
			Expect(delResult).To(Equal(1))
		})
	})

	Describe("Incr", func() {
		It("should return the real results of a valid INCR", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var incrResult int
			cmdErr := commander.Incr(&incrResult, key).Commit()
			conn = getConn()
			var getResult int
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value + 1))
			Expect(incrResult).To(Equal(value + 1))
		})
	})

	Describe("Decr", func() {
		It("should return the real results of a valid DECR", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var decrResult int
			cmdErr := commander.Decr(&decrResult, key).Commit()
			conn = getConn()
			var getResult int
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value - 1))
			Expect(decrResult).To(Equal(value - 1))
		})
	})

	Describe("FlushAll", func() {
		It("should return the real results of a valid FLUSHALL", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var flushResult string
			cmdErr := commander.FlushAll(&flushResult, true).Commit()
			conn = getConn()
			var getResult string
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
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
			errSend1 := conn.Send("SET", key1, value1)
			errSend2 := conn.Send("SET", key2, value2)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var keysResult []string
			cmdErr := commander.Keys(&keysResult, "*Key*").Commit()

			Expect(errSend1).To(BeNil())
			Expect(errSend2).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(keysResult).To(ContainElements("SomeKey1", "SomeKey2"))
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

			//wait to expire key
			time.Sleep(1100 * time.Millisecond)

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

		It("should return the errors of an invalid redis config", func() {
			conn := getWrongConn()
			commander := New(conn)
			var selectResult string
			cmdErr := commander.
				Select(&selectResult, 0).
				Commit()

			Expect(cmdErr).To(Not(BeNil()))
			Expect(selectResult).To(Equal(""))
		})

		It("should return the errors of an invalid result type", func() {
			conn := getConn()
			commander := New(conn)
			var pingResult int
			cmdErr := commander.
				Command(&pingResult, "PING").
				Commit()

			Expect(cmdErr).To(Not(BeNil()))
			Expect(pingResult).To(Equal(0))
		})

	})

})
