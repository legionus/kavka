package metadata

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
)

var (
	TopicsObserverContextVar string         = "app.observer.topics"
	TopicsEtcd               string         = "/topics"
	TopicsEtcdKeyRegexp      *regexp.Regexp = regexp.MustCompile("^" + TopicsEtcd + "/(?P<topic>[A-Za-z0-9_-]+)(/(?P<partition>[0-9]+))?$")
)

type TopicEtcdKey struct {
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
}

func (k *TopicEtcdKey) String() (res string) {
	res = TopicsEtcd

	if k.Topic != "" {
		res += "/" + k.Topic
	}

	if k.Partition > NoPartition {
		res += fmt.Sprintf("/%d", k.Partition)
	}

	return
}

func ParseTopicEtcdKey(value string) (*TopicEtcdKey, error) {
	key := &TopicEtcdKey{}

	match := TopicsEtcdKeyRegexp.FindStringSubmatch(value)

	if len(match) < 1 || len(match) > 6 {
		return key, fmt.Errorf("bad topic key: %s: %#v", value, match)
	}

	var err error

	if len(match) > 1 {
		key.Topic = match[1]
	}

	if len(match) > 3 {
		key.Partition, err = strconv.ParseInt(match[3], 10, 64)

		if err != nil {
			return key, err
		}
	} else {
		key.Partition = NoPartition
	}

	return key, nil
}

type TopicsCollection struct {
	EtcdCollection
}

func NewTopicsCollection(ctx context.Context, cfg *config.Config) (EtcdCollection, error) {
	base, err := newBaseCollection(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &TopicsCollection{base}, nil
}
