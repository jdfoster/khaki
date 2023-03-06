package kharki_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKharki(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kharki Suite")
}
