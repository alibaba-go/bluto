package bluto_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"git.alibaba.ir/rd/zebel-the-sailor-bluto/bluto"
	. "git.alibaba.ir/rd/zebel-the-sailor-bluto/commander"
	"git.alibaba.ir/rd/zebel-the-sailor-bluto/pooler"
)

var _ = Describe("Bluto", func() {

	// --------------------------------- global vars

	var bl *bluto.Bluto

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
		var err error
		bl, err = bluto.New(getCorrectConfig())
		if err != nil {
			panic(err)
		}
	})

	AfterSuite(func() {
		err := bl.Close()
		if err != nil {
			panic(err)
		}
	})

	BeforeEach(func() {
		var flushResult string
		err := bl.Begin().FLUSHALL(&flushResult, true).Commit()
		if err != nil {
			panic(err)
		}
	})

	// --------------------------------- tests

	Describe("Command and commit", func() {
		It("should return the results of a valid chain of commands", func() {

			key := "someKey"
			pingMsg := "Ping Message"
			var selectResult string
			var setResult string
			var incrResult int
			var getResult int
			var decrResult int
			var pingResult string

			cmdErr := bl.Begin().
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
	})

})
