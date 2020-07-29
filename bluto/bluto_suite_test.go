package bluto_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBluto(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bluto Suite")
}
