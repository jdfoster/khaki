package kharki

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Container", func() {
	It("should start ZooKeeper cluster", func() {
		ctx := context.Background()
		zkc := NewZooKeeperCluster(WithZooKeeperCount(3))
		Expect(zkc.Start(ctx)).Should(Succeed())
	})
})
