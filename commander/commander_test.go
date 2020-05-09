package commander_test

import (
	"os"

	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "git.alibaba.ir/rd/zebel-the-sailor-bluto/commander"
	. "git.alibaba.ir/rd/zebel-the-sailor-bluto/pool"
)

var _ = Describe("Commander", func() {

	// --------------------------------- global vars

	var pool *redis.Pool
	var getConn func() redis.Conn

	// --------------------------------- global funcs

	var getCorrectConfig = func() Config {
		address := os.Getenv("REDIS_ADDRESS")
		return Config{
			Address:               address,
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	// --------------------------------- before and after hooks

	BeforeSuite(func() {
		newPool, err := GetPool(getCorrectConfig())
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
		cmdResults, err := commander.
			Command("FLUSHALL").
			Commit()
		if err != nil {
			panic(err)
		}
		// convert results
		var flushResult string
		_, err = redis.Scan(cmdResults, &flushResult)
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

			key := "somekey"
			cmdResults, cmdErr := commander.
				Command("SELECT", 0).
				Command("SET", key, 9).
				Command("INCR", key).
				Command("GET", key).
				Commit()

			// convert results
			var selectResult string
			var setResult string
			var incrResult int
			var getResult int
			_, err := redis.Scan(
				cmdResults,
				&selectResult, &setResult,
				&incrResult, &getResult,
			)

			Expect(cmdErr).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
			Expect(setResult).To(Equal("OK"))
			Expect(incrResult).To(Equal(10))
			Expect(getResult).To(Equal(10))
			Expect(err).To(BeNil())
		})

		It("should return the errors of an invalid chain of commands", func() {
			conn := getConn()
			commander := New(conn)

			key := "somekey"
			cmdResults, cmdErr := commander.
				Command("SELECT", 0).
				Command("SET", key, 9).
				Command("SOMENONEXISTENTCOMMAND", key, 9).
				Command("INCR", key).
				Command("GET", key).
				Commit()

			// convert results
			var selectResult string
			var setResult string
			var nonExistentResult interface{}
			var incrResult int
			var getResult int
			_, err := redis.Scan(
				cmdResults,
				&selectResult, &setResult,
				&nonExistentResult,
				&incrResult, &getResult,
			)

			Expect(cmdErr).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
			Expect(setResult).To(Equal("OK"))
			Expect(nonExistentResult).To(BeNil())
			Expect(incrResult).To(Equal(0))
			Expect(getResult).To(Equal(0))
			Expect(err).To(Not(BeNil()))
		})
	})

})
