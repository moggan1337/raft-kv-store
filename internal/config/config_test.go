package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("/nonexistent")
	require.NoError(t, err)
	assert.Equal(t, 1, cfg.NodeID)
	assert.NotEmpty(t, cfg.DataDir)
	assert.NotEmpty(t, cfg.ListenAddr)
}

func TestLoadMissing(t *testing.T) {
	_, err := Load("/definitely/does/not/exist/cfg.yaml")
	assert.Error(t, err)
}
