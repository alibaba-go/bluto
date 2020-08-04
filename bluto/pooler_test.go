package bluto_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"

	"git.alibaba.ir/rd/zebel-the-sailor-bluto/bluto"
)

var _ = Describe("Pooler", func() {

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
