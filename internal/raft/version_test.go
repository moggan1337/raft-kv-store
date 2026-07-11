package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionIsNonEmpty(t *testing.T) {
	assert.NotEmpty(t, Version)
}
