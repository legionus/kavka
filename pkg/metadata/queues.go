package metadata

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/etcd"
)

var (
	NoPartition int64 = -1
	NoOffset    int64 = -1

	QueuesObserverContextVar string         = "app.observer.queues"
	QueuesEtcd               string         = "/queues"
	QueuesEtcdKeyRegexp      *regexp.Regexp = regexp.MustCompile("^" + QueuesEtcd + "/(?P<topic>[A-Za-z0-9_-]+)(/(?P<partition>[0-9]+)(/(?P<offset>[0-9]+))?)?$")
)

type QueueEtcdKey struct {
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (k *QueueEtcdKey) String() (res string) {
	res = QueuesEtcd

	if k.Topic != "" {
		res += "/" + k.Topic
	}

	if k.Partition > NoPartition {
		res += fmt.Sprintf("/%d", k.Partition)
	}

	if k.Offset > NoOffset {
		res += fmt.Sprintf("/%020d", k.Offset)
	}

	return
}

func ParseQueueEtcdKey(value string) (*QueueEtcdKey, error) {
	key := &QueueEtcdKey{}

	match := QueuesEtcdKeyRegexp.FindStringSubmatch(value)

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

	if len(match) > 5 {
		key.Offset, err = strconv.ParseInt(match[5], 10, 64)

		if err != nil {
			return key, err
		}
	} else {
		key.Offset = NoOffset
	}

	return key, nil
}

type QueuesCollection struct {
	EtcdCollection
}

func NewQueuesCollection(ctx context.Context, cfg *config.Config) (EtcdCollection, error) {
	base, err := newBaseCollection(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &QueuesCollection{base}, nil
}

func (b *QueuesCollection) Create(key EtcdKey, value string) (EtcdKey, error) {
	topicKey, ok := key.(*QueueEtcdKey)
	if !ok {
		return nil, fmt.Errorf("unexpected key")
	}

	keyPrefix := &QueueEtcdKey{
		Topic:     topicKey.Topic,
		Partition: topicKey.Partition,
		Offset:    NoOffset,
	}

	res, err := etcd.NewSequentialKV(b.Client(), keyPrefix.String(), value)
	if err != nil {
		return nil, err
	}

	return ParseQueueEtcdKey(res.Key())
}
