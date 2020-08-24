package bluto_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/alibaba-go/bluto/bluto"
)

var _ = Describe("Pooler", func() {

	// --------------------------------- global functions

	var getCorrectConfig = func() bluto.Config {
		address := os.Getenv("REDIS_ADDRESS")
		return bluto.Config{
			Address: address,
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
			pool, _ := bluto.GetPool(getWrongConfig())
			conn := pool.Get()
			errSend := conn.Send("PING")
			_, errDo := conn.Do("")
			errClose := pool.Close()

			Expect(errSend).To(Not(BeNil()))
			Expect(errDo).To(Not(BeNil()))
			Expect(pool).To(Not(BeNil()))
			Expect(errClose).To(BeNil())
		})
	})
})
