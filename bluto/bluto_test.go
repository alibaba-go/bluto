package bluto_test

import (
	"errors"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"git.alibaba.ir/rd/zebel-the-sailor-bluto/bluto"
)

var _ = Describe("Bluto", func() {

	// --------------------------------- global vars

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


	// --------------------------------- tests

	Describe("pooler", func() {

		// --------------------------------- tests

		Describe("GetPool", func() {
			It("should connect to the redis server with correct info", func() {
				pool, err := bluto.GetPool(getCorrectConfig())

				Expect(err).To(BeNil())
				Expect(pool).To(Not(BeNil()))

				err = pool.Close()
				Expect(err).To(BeNil())
			})

			It("should not connect to the redis server with incorrect info", func() {
				pool, err := bluto.GetPool(getWrongConfig())

				Expect(err).To(BeNil())
				Expect(pool).To(Not(BeNil()))

				err = pool.Close()
				Expect(err).To(BeNil())
			})
		})
	})

	Describe("New", func() {
		It("should create new bluto instance with correct config", func() {
			bluto, newErr := bluto.New(getCorrectConfig())
			var pingResult string
			cmdErr := bluto.Borrow().Ping(&pingResult,"").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(newErr).To(BeNil())
			Expect(pingResult).To(Equal("PONG"))
		})

		It("should fail to create new bluto instance with wrong config", func() {
			bluto, newErr := bluto.New(getWrongConfig())
			var pingResult string
			cmdErr := bluto.Borrow().Ping(&pingResult,"").Commit()
			Expect(cmdErr).To(Not(BeNil()))
			Expect(newErr).To(BeNil())
			Expect(pingResult).To(Not(Equal("PONG")))
		})
	})

	Describe("Close", func() {
		It("should close bluto instance", func() {
			bluto, newErr := bluto.New(getCorrectConfig())
			clsErr := bluto.Close()
			var pingResult string
			cmdErr := bluto.Borrow().Ping(&pingResult,"").Commit()
			Expect(clsErr).To(BeNil())
			Expect(cmdErr).To(Equal(errors.New("redigo: get on closed pool")))
			Expect(newErr).To(BeNil())
			Expect(pingResult).To(Not(Equal("PONG")))
		})
	})

	Describe("Borrow", func() {
		It("should close bluto instance", func() {
			bluto, newErr := bluto.New(getCorrectConfig())
			var pingResult string
			cmdErr := bluto.Borrow().Ping(&pingResult,"").Commit()
			Expect(cmdErr).To(BeNil())
			Expect(newErr).To(BeNil())
			Expect(pingResult).To(Equal("PONG"))
		})
	})

})
