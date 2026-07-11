package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandomTimeoutInRange(t *testing.T) {
	min := 100 * time.Millisecond
	max := 200 * time.Millisecond
	for i := 0; i < 50; i++ {
		d := randomTimeout(min, max)
		assert.GreaterOrEqual(t, d, min)
		assert.Less(t, d, max)
	}
}
