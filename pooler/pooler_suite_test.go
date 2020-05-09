package pooler_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPooler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pooler Suite")
}
