package optimizer

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Optimizer", func() {
	Context("and", func() {
		It("", func() {
			promql := `vector_a{key1="val1"} and vector_b{key2="val2"}`
			optimized := OptimizeQuery(promql)
			Expect(optimized).To(Equal(`vector_a{key1="val1",key2="val2"} and vector_b{key1="val1",key2="val2"}`))
		})
	})
})
