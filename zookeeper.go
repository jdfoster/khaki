package kharki

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/go-zookeeper/zk"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type zkOptions struct {
	count            int
	hostnamePrefix   string
	clientPort       int
	leaderPeerPort   int
	electionPeerPort int
	networkNames     []string
	retryCount       int
	retryInterval    time.Duration
}

type ZooKeeperClusterOption = func(*zkOptions)

func WithZooKeeperCount(count int) ZooKeeperClusterOption {
	return func(zko *zkOptions) {
		zko.count = count
	}
}

type ZooKeeperCluster struct {
	options     *zkOptions
	requests    []testcontainers.GenericContainerRequest
	containers  []testcontainers.Container
	clientPorts []string
}

func (c ZooKeeperCluster) probe() ([]*zk.ServerStats, error) {
	ss := make([]string, len(c.clientPorts))
	for i, p := range c.clientPorts {
		ss[i] = fmt.Sprintf("localhost:%s", p)
	}

	vv, ok := zk.FLWSrvr(ss, time.Second*2)
	if !ok {
		if vv != nil && len(vv) > 0 {
			for i, v := range vv {
				if v != nil && v.Error != nil {
					return vv, fmt.Errorf("failed to probe ZooKeeper, server %q raised an error: %w", ss[i], v.Error)
				}
			}
		}

		return vv, fmt.Errorf("failed to probe ZooKeeper")
	}

	return vv, nil
}

func (c ZooKeeperCluster) Leader() (string, error) {
	ss, err := c.probe()
	if err != nil {
		return "", fmt.Errorf("failed to identify ZooKeeper leader: %w", err)
	}

	for _, s := range ss {
		if s.Mode == zk.ModeLeader {
			return s.Server, nil
		}
	}

	return "", nil
}

func (c ZooKeeperCluster) Followers() ([]string, error) {
	var result []string

	ss, err := c.probe()
	if err != nil {
		return result, fmt.Errorf("failed to identify ZooKeeper followers: %w", err)
	}

	for _, s := range ss {
		if s.Mode == zk.ModeFollower {
			result = append(result, s.Server)
		}
	}

	return result, nil
}

func (c *ZooKeeperCluster) Start(ctx context.Context) error {
	req := make(testcontainers.ParallelContainerRequest, len(c.requests))
	for i, r := range c.requests {
		req[i] = r
	}

	for _, n := range c.options.networkNames {
		r := testcontainers.GenericNetworkRequest{
			NetworkRequest: testcontainers.NetworkRequest{Name: n},
		}
		if _, err := testcontainers.GenericNetwork(ctx, r); err != nil {
			return fmt.Errorf("failed to create network %q: %w", n, err)
		}
	}

	var err error
	c.containers, err = testcontainers.ParallelContainers(ctx, req, testcontainers.ParallelContainersOptions{})
	if err != nil {
		return fmt.Errorf("failed to start ZooKeeper container: %w", err)
	}

	c.clientPorts = make([]string, len(c.containers))
	for i, container := range c.containers {
		cp := strconv.Itoa(c.options.clientPort)
		hp, err := container.MappedPort(ctx, nat.Port(cp))
		if err != nil {
			return fmt.Errorf("failed to get port for ZooKeeper container: %w", err)
		}

		c.clientPorts[i] = hp.Port()
	}

	if len(req) > 1 {
		success := false
		for i := c.options.retryCount; i > 0; i-- {
			if _, err := c.Leader(); err != nil {
				time.Sleep(c.options.retryInterval)
				continue
			}

			success = true
			break
		}

		if !success {
			return fmt.Errorf("fauled to start ZooKeeper cluster within timeout")
		}
	}

	return nil
}

func NewZooKeeperCluster(opts ...ZooKeeperClusterOption) *ZooKeeperCluster {
	o := &zkOptions{
		count:            1,
		hostnamePrefix:   "zookeeper",
		clientPort:       2181,
		leaderPeerPort:   2888,
		electionPeerPort: 3888,
		networkNames:     []string{"testcontainers"},
		retryCount:       30,
		retryInterval:    time.Second * 2,
	}

	for _, opt := range opts {
		opt(o)
	}

	c := &ZooKeeperCluster{
		options:  o,
		requests: make([]testcontainers.GenericContainerRequest, o.count),
	}

	pp := make([]string, 3)
	for i, p := range []int{o.clientPort, o.leaderPeerPort, o.electionPeerPort} {
		pp[i] = strconv.Itoa(p) + "/tcp"
	}

	hnpf := strings.ToLower(o.hostnamePrefix) + "-"
	hnsf := ":" + strconv.Itoa(o.leaderPeerPort) + ":" + strconv.Itoa(o.electionPeerPort)
	id := make([]string, o.count)
	hn := make([]string, o.count)
	ss := make([]string, o.count)
	for i := 0; i < o.count; i++ {
		id[i] = strconv.Itoa(i + 1)
		hn[i] = hnpf + id[i]
		ss[i] = hn[i] + hnsf
	}

	zks := strings.Join(ss, ";")

	for i := range c.requests {
		c.requests[i] = testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:    "confluentinc/cp-zookeeper:7.3.2",
				Hostname: hn[i],
				Env: map[string]string{
					"ZOOKEEPER_SERVER_ID":   id[i],
					"ZOOKEEPER_CLIENT_PORT": strconv.Itoa(c.options.clientPort),
					"ZOOKEEPER_SERVERS":     zks,
				},
				ExposedPorts: pp,
				Networks:     o.networkNames,
				WaitingFor:   wait.NewHostPortStrategy(nat.Port(pp[0])),
			},
			Started: true,
		}
	}

	return c
}
