package commander_test

import (
	"os"

	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "git.alibaba.ir/rd/zebel-the-sailor-bluto/commander"
	"git.alibaba.ir/rd/zebel-the-sailor-bluto/pooler"
)

var _ = Describe("Commander", func() {

	// --------------------------------- global vars

	var pool *redis.Pool
	var getConn func() redis.Conn

	// --------------------------------- global functions

	var getCorrectConfig = func() pooler.Config {
		address := os.Getenv("REDIS_ADDRESS")
		return pooler.Config{
			Address:               address,
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	// --------------------------------- before and after hooks

	BeforeSuite(func() {
		newPool, err := pooler.GetPool(getCorrectConfig())
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
