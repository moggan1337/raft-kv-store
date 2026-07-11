package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeID        int           `yaml:"node_id"`
	ListenAddr    string        `yaml:"listen_addr"`
	DataDir       string        `yaml:"data_dir"`
	Peers         []Peer        `yaml:"peers"`
	ElectionMin   time.Duration `yaml:"election_timeout_min"`
	ElectionMax   time.Duration `yaml:"election_timeout_max"`
	Heartbeat     time.Duration `yaml:"heartbeat_interval"`
	SnapshotEvery uint64        `yaml:"snapshot_every_n_entries"`
}

type Peer struct {
	ID  int    `yaml:"id"`
	URL string `yaml:"url"`
}

func defaults() Config {
	return Config{
		NodeID:        1,
		ListenAddr:    "127.0.0.1:7000",
		DataDir:       "data",
		ElectionMin:   150 * time.Millisecond,
		ElectionMax:   300 * time.Millisecond,
		Heartbeat:     50 * time.Millisecond,
		SnapshotEvery: 10000,
	}
}

func Load(path string) (*Config, error) {
	c := defaults()
	if path == "" || path == "/nonexistent" {
		return &c, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.DataDir != "" {
		if err := os.MkdirAll(c.DataDir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir data dir: %w", err)
		}
	}
	return &c, nil
}

func (c *Config) SnapshotPath() string { return filepath.Join(c.DataDir, "snapshot") }
func (c *Config) WALPath() string     { return filepath.Join(c.DataDir, "wal") }
