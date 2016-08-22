package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/legionus/kavka/pkg/storage"
)

const (
	AppConfigContextVar = "app.config"
)

type cfgKeyConfig int

type CfgLogLevel struct {
	logrus.Level
}

func (d *CfgLogLevel) UnmarshalText(data []byte) (err error) {
	d.Level, err = logrus.ParseLevel(strings.ToLower(string(data)))
	return
}

type Global struct {
	// Address is the advertised host:port for client connections
	Address string
	// Logfile specifies logfile location
	Logfile string
	// Hostname specifies the node name
	Hostname string
	// Group specifies the name of the group
	Group string
	// FIXME
	Port int
}

type Topic struct {
	// AllowTopicsCreation enables auto creation of topic on the server
	AllowTopicsCreation bool `yaml:"allow-topics-creation"`
	// WriteConcern describes the number of groups, which should confirm write of each block.
	// This value should not be more than the number of nodes in the cluster.
	WriteConcern int64 `yaml:"write-concern"`
	// MessageRetentionPeriod defines the maximum time we will retain a message.
	MessageRetentionPeriod time.Duration `yaml:"message-retention-period"`
	// PartitionSize defines maximum partition size.
	MaxPartitionSize int64 `yaml:"max-partition-size"`
	// MaxMessageSize defines maximum size of incoming message. Set 0 to disable.
	MaxMessageSize int64 `yaml:"max-message-size"`
	// ChunkSize defines maximum size of a block on which is divided the incoming message.
	MaxChunkSize int64 `yaml:"max-chunk-size"`
	// CleanupPeriod sets time period between cleanup iterations.
	CleanupPeriod time.Duration `yaml:"cleanup-period"`
}

type Logging struct {
	Level            CfgLogLevel
	DisableColors    bool
	DisableTimestamp bool
	FullTimestamp    bool
	DisableSorting   bool
}

type StorageDriver map[string]storage.StorageDriverParameters

func (s StorageDriver) Name() string {
	for k := range s {
		return k
	}
	return ""
}

func (s StorageDriver) Parameters() storage.StorageDriverParameters {
	return s[s.Name()]
}

type Storage struct {
	// SyncPool specifies the number of concurrent processes synchronization chunks from other servers.
	SyncPool int
	// Driver
	Driver StorageDriver
	// CleanupPeriod sets time period between cleanup iterations.
	CleanupPeriod time.Duration `yaml:"cleanup-period"`
}

type EtcdURLs struct {
	// URLs are the URLs for etcd
	URLs []string
}

type SecurityConfig struct {
	CertFile      string `yaml:"cert-file"`
	KeyFile       string `yaml:"key-file"`
	CertAuth      bool   `yaml:"client-cert-auth"`
	TrustedCAFile string `yaml:"trusted-ca-file"`
	AutoTLS       bool   `yaml:"auto-tls"`
}

type ServingInfo struct {
	EtcdURLs       `yaml:",inline"`
	SecurityConfig `yaml:",inline"`
}

type EtcdDiscovery struct {
	DNSCluster *string `yaml:"discovery-srv"`
	Dproxy     *string `yaml:"discovery-proxy"`
	Durl       *string `yaml:"discovery"`
}

type Etcd struct {
	// NoServer suppresses startup of etcd server
	NoServer bool `yaml:"no-server"`

	// StorageDir indicates where to save the etcd data
	StorageDir string `yaml:"storage-dir"`

	Name                *string `yaml:"name"`
	InitialCluster      *string `yaml:"initial-cluster"`
	InitialClusterToken *string `yaml:"initial-cluster-token"`
	ClusterState        *string `yaml:"initial-cluster-state"`

	Client ServingInfo `yaml:"client"`
	Peer   ServingInfo `yaml:"peer"`

	AdvertiseClient EtcdURLs `yaml:"advertise-client"`
	AdvertisePeer   EtcdURLs `yaml:"advertise-peer"`

	Discovery EtcdDiscovery `yaml:",inline"`
}

type Config struct {
	Global  Global
	Logging Logging
	Topic   Topic
	Storage Storage
	Etcd    Etcd
}

// SetDefaults applies default values to config structure.
func (c *Config) SetDefaults() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		panic("unable to get hostname")
	}

	c.Global.Hostname = hostname
	c.Global.Group = hostname
	c.Global.Logfile = "/var/log/kavka.log"

	c.Topic.MaxChunkSize = int64(1024)
	c.Topic.WriteConcern = 1
	c.Topic.CleanupPeriod = 1 * time.Minute

	c.Storage.SyncPool = 10
	c.Storage.CleanupPeriod = 1 * time.Minute

	c.Logging.Level.Level = logrus.InfoLevel
	c.Logging.DisableColors = true
	c.Logging.DisableTimestamp = false
	c.Logging.FullTimestamp = true
	c.Logging.DisableSorting = false

	return c
}

func (c *Config) LoadEnv() *Config {
	if v := os.Getenv("KAVKA_HOSTNAME"); v != "" {
		c.Global.Hostname = v
	}
	if v := os.Getenv("KAVKA_GROUP"); v != "" {
		c.Global.Group = v
	}
	if v := os.Getenv("KAVKA_ADDRESS"); v != "" {
		c.Global.Address = v
	}
	if v := os.Getenv("KAVKA_LOGFILE"); v != "" {
		c.Global.Logfile = v
	}
	return c
}

func NewConfig(filename string) (*Config, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	cfg.SetDefaults()

	if err := yaml.Unmarshal(buf, cfg); err != nil {
		return nil, err
	}

	cfg.LoadEnv()

	if len(cfg.Storage.Driver) == 0 {
		return nil, fmt.Errorf("storage driver is not defined")
	}

	if len(cfg.Storage.Driver) > 1 {
		return nil, fmt.Errorf("multiple storage drivers specified in configuration")
	}

	return cfg, err
}
