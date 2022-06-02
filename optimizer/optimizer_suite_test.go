package optimizer

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOptimizer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Optimizer Suite")
}
