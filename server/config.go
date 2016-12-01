// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/timeutil"
)

// Config is the pd server configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	Version bool `json:"-"`

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	Name    string `toml:"name" json:"name"`
	DataDir string `toml:"data-dir" json:"data-dir"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`

	// Join to an existing pd cluster, a string of endpoints.
	Join string `toml:"join" json:"join"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// Log level.
	LogLevel string `toml:"log-level" json:"log-level"`
	// Log file.
	LogFile string `toml:"log-file" json:"log-file"`

	// TsoSaveInterval is the interval to save timestamp.
	TsoSaveInterval timeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// MaxPeerCount for a region. default is 3.
	MaxPeerCount uint64 `toml:"max-peer-count" json:"max-peer-count"`

	ScheduleCfg ScheduleConfig `toml:"schedule" json:"schedule"`

	MetricCfg MetricConfig `toml:"metric" json:"metric"`

	ConstraintCfg []ConstraintConfig `toml:"constraints" json:"constraints"`
	constraints   *Constraints

	// Only test can change them.
	nextRetryDelay             time.Duration
	disableStrictReconfigCheck bool

	tickMs     uint64
	electionMs uint64

	configFile string
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.Version, "version", false, "print version information and exit")
	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.StringVar(&cfg.Name, "name", defaultName, "human-readable name for this pd member")

	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to the data directory (default 'default.${name}')")
	fs.StringVar(&cfg.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.AdvertiseClientUrls, "advertise-client-urls", "", "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", "advertise url for peer traffic (default '${peer-urls}')")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	fs.StringVar(&cfg.Join, "join", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")

	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")

	return cfg
}

const (
	defaultLeaderLease    = int64(3)
	defaultMaxPeerCount   = uint64(3)
	defaultNextRetryDelay = time.Second

	defaultName                = "pd"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defualtInitialClusterState = embed.ClusterStateFlagNew

	// etcd use 100ms for heartbeat and 1s for election timeout.
	// We can enlarge both a little to reduce the network aggression.
	// now embed etcd use TickMs for heartbeat, we will update
	// after embed etcd decouples tick and heartbeat.
	defaultTickMs = uint64(500)
	// embed etcd has a check that `5 * tick > election`
	defaultElectionMs = uint64(3000)
)

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v *timeutil.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) validate() error {
	if c.Join != "" && c.InitialCluster != "" {
		return errors.New("-initial-cluster and -join can not be provided at the same time")
	}
	return nil
}

func (c *Config) adjust() error {
	if err := c.validate(); err != nil {
		return errors.Trace(err)
	}

	adjustString(&c.Name, defaultName)
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)

	if c.Join != "" {
		initialCluster, state, err := prepareJoinCluster(c)
		if err != nil {
			return errors.Trace(err)
		}
		c.InitialCluster = initialCluster
		c.InitialClusterState = state
	}

	if len(c.InitialCluster) == 0 {
		// The advertise peer urls may be http://127.0.0.1:2380,http://127.0.0.1:2381
		// so the initial cluster is pd=http://127.0.0.1:2380,pd=http://127.0.0.1:2381
		items := strings.Split(c.AdvertisePeerUrls, ",")

		sep := ""
		for _, item := range items {
			c.InitialCluster += fmt.Sprintf("%s%s=%s", sep, c.Name, item)
			sep = ","
		}
	}

	adjustString(&c.InitialClusterState, defualtInitialClusterState)

	adjustUint64(&c.MaxPeerCount, defaultMaxPeerCount)

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	adjustDuration(&c.TsoSaveInterval, time.Duration(defaultLeaderLease)*time.Second)

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = defaultNextRetryDelay
	}

	adjustUint64(&c.tickMs, defaultTickMs)
	adjustUint64(&c.electionMs, defaultElectionMs)

	c.ScheduleCfg.adjust()
	return c.parseConstraints()
}

func (c *Config) clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

func (c *Config) setScheduleConfig(cfg ScheduleConfig) {
	// TODO: add more check for cfg set.
	cfg.adjust()

	c.ScheduleCfg = cfg
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// ScheduleConfig is the schedule configuration.
type ScheduleConfig struct {
	// If the region count of one store is less than this value,
	// it will never be used as a source store.
	MinRegionCount uint64 `toml:"min-region-count" json:"min-region-count"`

	// If the leader count of one store is less than this value,
	// it will never be used as a source store.
	MinLeaderCount uint64 `toml:"min-leader-count" json:"min-leader-count"`

	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	MaxSnapshotCount uint64 `toml:"max-snapshot-count" json:"max-snapshot-count"`

	// If the source and target store's diff score is less than this value,
	// the schedule will be canceled.
	MinBalanceDiffRatio float64 `toml:"min-balance-diff-ratio" json:"min-balance-diff-ratio"`

	// MaxStoreDownDuration is the max duration at which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownDuration timeutil.Duration `toml:"max-store-down-duration" json:"max-store-down-duration"`

	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit" json:"leader-schedule-limit"`
	// LeaderScheduleInterval is the interval to schedule leader.
	LeaderScheduleInterval timeutil.Duration `toml:"leader-schedule-interval" json:"leader-schedule-interval"`

	// StorageScheduleLimit is the max coexist storage schedules.
	StorageScheduleLimit uint64 `toml:"storage-schedule-limit" json:"storage-schedule-limit"`
	// StorageScheduleInterval is the interval to schedule storage.
	StorageScheduleInterval timeutil.Duration `toml:"storage-schedule-interval" json:"storage-schedule-interval"`
}

const (
	defaultMinRegionCount          = uint64(10)
	defaultMinLeaderCount          = uint64(10)
	defaultMaxSnapshotCount        = uint64(3)
	defaultMinBalanceDiffRatio     = float64(0.01)
	defaultMaxStoreDownDuration    = 30 * time.Minute
	defaultLeaderScheduleLimit     = 4
	defaultLeaderScheduleInterval  = 30 * time.Second
	defaultStorageScheduleLimit    = 4
	defaultStorageScheduleInterval = 30 * time.Second
)

func newScheduleConfig() *ScheduleConfig {
	return &ScheduleConfig{}
}

func (c *ScheduleConfig) adjust() {
	adjustUint64(&c.MinRegionCount, defaultMinRegionCount)
	adjustUint64(&c.MinLeaderCount, defaultMinLeaderCount)
	adjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	adjustFloat64(&c.MinBalanceDiffRatio, defaultMinBalanceDiffRatio)
	adjustDuration(&c.MaxStoreDownDuration, defaultMaxStoreDownDuration)
	adjustUint64(&c.LeaderScheduleLimit, defaultLeaderScheduleLimit)
	adjustDuration(&c.LeaderScheduleInterval, defaultLeaderScheduleInterval)
	adjustUint64(&c.StorageScheduleLimit, defaultStorageScheduleLimit)
	adjustDuration(&c.StorageScheduleInterval, defaultStorageScheduleInterval)
}

// scheduleOption is a wrapper to access the configuration safely.
type scheduleOption struct {
	v           atomic.Value
	constraints *Constraints
}

func newScheduleOption(cfg *Config) *scheduleOption {
	o := &scheduleOption{}
	o.store(&cfg.ScheduleCfg)
	o.constraints = cfg.constraints
	return o
}

func (o *scheduleOption) load() *ScheduleConfig {
	return o.v.Load().(*ScheduleConfig)
}

func (o *scheduleOption) store(cfg *ScheduleConfig) {
	o.v.Store(cfg)
}

func (o *scheduleOption) GetConstraints() *Constraints {
	return o.constraints
}

func (o *scheduleOption) GetMinRegionCount() uint64 {
	return o.load().MinRegionCount
}

func (o *scheduleOption) GetMinLeaderCount() uint64 {
	return o.load().MinLeaderCount
}

func (o *scheduleOption) GetMaxSnapshotCount() uint64 {
	return o.load().MaxSnapshotCount
}

func (o *scheduleOption) GetMinBalanceDiffRatio() float64 {
	return o.load().MinBalanceDiffRatio
}

func (o *scheduleOption) GetMaxStoreDownTime() time.Duration {
	return o.load().MaxStoreDownDuration.Duration
}

func (o *scheduleOption) GetLeaderScheduleLimit() uint64 {
	return o.load().LeaderScheduleLimit
}

func (o *scheduleOption) GetLeaderScheduleInterval() time.Duration {
	return o.load().LeaderScheduleInterval.Duration
}

func (o *scheduleOption) GetStorageScheduleLimit() uint64 {
	return o.load().StorageScheduleLimit
}

func (o *scheduleOption) GetStorageScheduleInterval() time.Duration {
	return o.load().StorageScheduleInterval.Duration
}

// MetricConfig is the metric configuration.
type MetricConfig struct {
	PushAddress  string            `toml:"address" json:"address"`
	PushInterval timeutil.Duration `toml:"interval" json:"interval"`
}

// ConstraintConfig is the replica constraint configuration to place replicas.
type ConstraintConfig struct {
	Labels   []string `toml:"labels" json:"labels"`
	Replicas int      `toml:"replicas" json:"replicas"`
}

var validLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-._]*[a-z0-9])?$`)

func parseConstraint(cfg *ConstraintConfig) (*Constraint, error) {
	if cfg.Replicas == 0 {
		return nil, errors.New("constraint replicas must > 0")
	}
	labels := make(map[string]string)
	for _, label := range cfg.Labels {
		kv := strings.Split(strings.ToLower(label), "=")
		if len(kv) != 2 {
			return nil, errors.Errorf("invalid constraint %q", label)
		}
		k, v := kv[0], kv[1]
		if !validLabel.MatchString(k) || !validLabel.MatchString(v) {
			return nil, errors.Errorf("invalid constraint %q, must match %s", label, validLabel)
		}
		if _, ok := labels[k]; ok {
			return nil, errors.Errorf("duplicated constraint %q", label)
		}
		labels[k] = v
	}
	return &Constraint{
		Labels:   labels,
		Replicas: cfg.Replicas,
	}, nil
}

func (c *Config) parseConstraints() error {
	var constraints []*Constraint
	for _, cfg := range c.ConstraintCfg {
		constraint, err := parseConstraint(&cfg)
		if err != nil {
			return errors.Trace(err)
		}
		constraints = append(constraints, constraint)
	}

	var err error
	c.constraints, err = newConstraints(int(c.MaxPeerCount), constraints)
	return errors.Trace(err)
}

// ParseUrls parse a string into multiple urls.
// Export for api.
func ParseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Trace(err)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

// generates a configuration for embedded etcd.
func (c *Config) genEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.EnablePprof = true
	cfg.StrictReconfigCheck = !c.disableStrictReconfigCheck
	cfg.TickMs = uint(c.tickMs)
	cfg.ElectionMs = uint(c.electionMs)

	var err error

	cfg.LPUrls, err = ParseUrls(c.PeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls, err = ParseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls, err = ParseUrls(c.ClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls, err = ParseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}

var unixURLCount uint64

// unixURL returns a unique unix socket url, used for test only.
func unixURL() string {
	return fmt.Sprintf("unix://localhost:%d.%d.sock", os.Getpid(), atomic.AddUint64(&unixURLCount, 1))
}

// NewTestSingleConfig is only for test to create one pd.
// Because pd-client also needs this, so export here.
func NewTestSingleConfig() *Config {
	cfg := &Config{
		Name:       "pd",
		ClientUrls: unixURL(),
		PeerUrls:   unixURL(),

		InitialClusterState: embed.ClusterStateFlagNew,

		LeaderLease:     1,
		TsoSaveInterval: timeutil.NewDuration(200 * time.Millisecond),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.disableStrictReconfigCheck = true
	cfg.tickMs = 100
	cfg.electionMs = 1000

	return cfg
}

// NewTestMultiConfig is only for test to create multiple pd configurations.
// Because pd-client also needs this, so export here.
func NewTestMultiConfig(count int) []*Config {
	cfgs := make([]*Config, count)

	clusters := []string{}
	for i := 1; i <= count; i++ {
		cfg := NewTestSingleConfig()
		cfg.Name = fmt.Sprintf("pd%d", i)

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.InitialCluster = initialCluster
	}

	return cfgs
}
