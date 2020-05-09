package pooler_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "git.alibaba.ir/rd/zebel-the-sailor-bluto/pooler"
)

var _ = Describe("Redis", func() {

	// --------------------------------- global funcs

	var getCorrectConfig = func() Config {
		address := os.Getenv("REDIS_ADDRESS")
		return Config{
			Address:               address,
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	var getWrongConfig = func() Config {
		return Config{
			Address:               "blahblah",
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	// --------------------------------- tests

	Describe("GetPool", func() {
		It("should connect to the redis server with correct info", func() {
			pool, err := GetPool(getCorrectConfig())

			Expect(err).To(BeNil())
			Expect(pool).To(Not(BeNil()))

			err = pool.Close()
			Expect(err).To(BeNil())
		})

		It("should not connect to the redis server with incorrect info", func() {
			pool, err := GetPool(getWrongConfig())

			Expect(err).To(BeNil())
			Expect(pool).To(Not(BeNil()))

			err = pool.Close()
			Expect(err).To(BeNil())
		})
	})

})
