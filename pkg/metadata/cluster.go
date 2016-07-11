package metadata

import (
	"fmt"
	"regexp"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
)

var (
	ClusterObserverContextVar string         = "app.observer.cluster"
	ClusterEtcd               string         = "/cluster"
	ClusterEtcdKeyRegexp      *regexp.Regexp = regexp.MustCompile("^" + ClusterEtcd + "/(?P<group>[^/]+)(/(?P<node>.+))?$")
)

type ClusterEtcdKey struct {
	Group string `json:"group"`
	Node  string `json:"node"`
}

func (k *ClusterEtcdKey) String() (res string) {
	res = ClusterEtcd
	if k.Group != "" {
		res += "/" + k.Group
	}
	if k.Node != "" {
		res += "/" + k.Node
	}
	return
}

func ParseClusterEtcdKey(value string) (*ClusterEtcdKey, error) {
	key := &ClusterEtcdKey{}

	match := ClusterEtcdKeyRegexp.FindStringSubmatch(value)

	if len(match) < 1 || len(match) > 4 {
		return key, fmt.Errorf("bad cluster key: %s", value)
	}

	if len(match) > 1 {
		key.Group = match[1]
	}

	if len(match) > 3 {
		key.Node = match[3]
	}

	return key, nil
}

type NodesCollection struct {
	EtcdCollection
}

func NewNodesCollection(ctx context.Context, cfg *config.Config) (EtcdCollection, error) {
	base, err := newBaseCollection(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &NodesCollection{base}, nil
}
