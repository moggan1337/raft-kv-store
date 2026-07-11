package raft

import (
	"math/rand"
	"time"
)

func randomTimeout(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	span := int64(max - min)
	return min + time.Duration(rand.Int63n(span))
}
