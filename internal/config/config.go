package config

import (
	"time"

	"github.com/spf13/viper"
)

// Config is the root configuration struct
type Config struct {
	Node     NodeConfig     `mapstructure:"node"`
	Schedule ScheduleConfig `mapstructure:"schedule"`
}

// NodeConfig holds per-node configuration
type NodeConfig struct {
	PeerID           string          `mapstructure:"peerID"`
	MaxPeers         int             `mapstructure:"maxPeers"`
	Storage          StorageConfig   `mapstructure:"storage"`
	Group            GroupConfig     `mapstructure:"group"`
	NetworkHostnames HostnamesConfig `mapstructure:"networkHostnames"`
	World            WorldConfig     `mapstructure:"world"`
}

// StorageConfig holds storage-related settings
type StorageConfig struct {
	ReplicationFactor int    `mapstructure:"replicationFactor"`
	StorageMode       string `mapstructure:"storageMode"`
	RetrievalMode     string `mapstructure:"retrievalMode"`
}

// GroupConfig holds group/Voronoi settings
type GroupConfig struct {
	Migration       bool `mapstructure:"migration"`
	VoronoiGrouping bool `mapstructure:"voronoiGrouping"`
}

// HostnamesConfig holds network endpoint configuration
type HostnamesConfig struct {
	PeerServer         string `mapstructure:"peerServer"`
	PeerStorageServer  string `mapstructure:"peerStorageServer"`
	OverlayHostname    string `mapstructure:"overlayHostname"`
	SuperPeerServer    string `mapstructure:"superPeerServer"`
	GroupStorageServer string `mapstructure:"groupStorageServer"`
}

// WorldConfig holds the virtual world dimensions
type WorldConfig struct {
	Height float64 `mapstructure:"height"`
	Width  float64 `mapstructure:"width"`
}

// ScheduleConfig holds scheduler interval settings
type ScheduleConfig struct {
	HealthCheck    time.Duration `mapstructure:"healthCheck"`
	DHTBootstrap   time.Duration `mapstructure:"dhtBootstrap"`
	DBCleanup      time.Duration `mapstructure:"dbCleanup"`
	LedgerCleanup  time.Duration `mapstructure:"ledgerCleanup"`
	Repair         time.Duration `mapstructure:"repair"`
	UpdatePosition time.Duration `mapstructure:"updatePosition"`
}

// Load reads configuration from file and environment
func Load(cfgFile string) (*Config, error) {
	v := viper.New()

	// Defaults matching Java application.yml
	v.SetDefault("node.maxPeers", 5)
	v.SetDefault("node.storage.replicationFactor", 3)
	v.SetDefault("node.storage.storageMode", "fast")
	v.SetDefault("node.storage.retrievalMode", "fast")
	v.SetDefault("node.group.migration", false)
	v.SetDefault("node.group.voronoiGrouping", false)
	v.SetDefault("node.networkHostnames.peerServer", "")
	v.SetDefault("node.networkHostnames.groupStorageServer", "")
	v.SetDefault("node.networkHostnames.superPeerServer", "")
	v.SetDefault("node.world.height", 10.0)
	v.SetDefault("node.world.width", 10.0)
	v.SetDefault("schedule.healthCheck", 20*time.Second)
	v.SetDefault("schedule.dhtBootstrap", 500*time.Second)
	v.SetDefault("schedule.dbCleanup", 60*time.Second)
	v.SetDefault("schedule.ledgerCleanup", 60*time.Second)
	v.SetDefault("schedule.repair", 6000*time.Second)
	v.SetDefault("schedule.updatePosition", 10*time.Second)

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath("./configs")
		v.AddConfigPath("/configs")
		v.AddConfigPath(".")
	}

	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
