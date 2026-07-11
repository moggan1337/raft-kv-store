package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfoEmits(t *testing.T) {
	var buf bytes.Buffer
	l := New(slog.LevelInfo, &buf)
	l.Info("hello", "k", "v")
	assert.Contains(t, buf.String(), "hello")
	assert.Contains(t, buf.String(), "v")
}

func TestDebugSuppressedAtInfo(t *testing.T) {
	var buf bytes.Buffer
	l := New(slog.LevelInfo, &buf)
	l.Debug("nope")
	assert.NotContains(t, buf.String(), strings.ToLower("nope"))
}
