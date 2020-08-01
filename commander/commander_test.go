package commander_test

import (
	"git.alibaba.ir/rd/zebel-the-sailor-bluto/bluto"
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

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
		pool.Close()
	})

	BeforeEach(func() {
		conn := getConn()
		commander := New(conn)
		var flushResult string
		err := commander.
			Command(&flushResult, "FLUSHALL").
			Commit()
		if err != nil {
			panic(err)
		}
	})

	// --------------------------------- tests

	Describe("New method", func() {
		It("should return a new commander", func() {
			conn := getConn()
			commander := New(conn)

			Expect(commander).To(Not(BeNil()))
			Expect(commander).To(BeAssignableToTypeOf(&Commander{}))
		})
	})

	Describe("Command and commit", func() {
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
				SELECT(&selectResult, 0).
				SET(&setResult, key, 9, SetOption{}).
				EXPIRE(&expireResult1, key, 1).
				EXPIRE(&expireResult2, "NotExistKey", 1).
				GET(&getResult1, key).
				Commit()
			Expect(cmdErr).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(expireResult1).To(Equal(1))
			Expect(expireResult2).To(Equal(0))
			Expect(getResult1).To(Equal(9))
			//sleep 1 second to expire key

			time.Sleep(2 * time.Second)
			conn = getConn()
			commander = New(conn)
			cmdErr = commander.
				SELECT(&selectResult, 0).
				GET(&getResult2, key).
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
				SELECT(&selectResult, 0).
				SET(&setResult1, key1, 9, SetOption{}).
				SET(&setResult2, key2, 9, SetOption{}).
				KEYS(&keysResult, "*Key*").
				DEL(&delResult, key1, "NotExistKey").
				GET(&getResult1, key1).
				FLUSHALL(&flushResult, true).
				GET(&getResult2, key2).
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
			pingMsg := "Ping Message"
			var selectResult string
			var setResult string
			var incrResult int
			var getResult int
			var decrResult int
			var pingResult string

			cmdErr := commander.
				SELECT(&selectResult, 0).
				SET(&setResult, key, 9, SetOption{}).
				INCR(&incrResult, key).
				GET(&getResult, key).
				DECR(&decrResult, key).
				PING(&pingResult, pingMsg).
				Commit()

			Expect(cmdErr).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
			Expect(setResult).To(Equal("OK"))
			Expect(incrResult).To(Equal(10))
			Expect(getResult).To(Equal(10))
			Expect(decrResult).To(Equal(9))
			Expect(pingResult).To(Equal("[" + pingMsg + "]"))
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
				SELECT(&selectResult, 0).
				SET(&setResult, key, 9, SetOption{}).
				Command(&nonExistentResult, "SOMENONEXISTENTCOMMAND", key, 9).
				INCR(&incrResult, key).
				GET(&getResult, key).
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
