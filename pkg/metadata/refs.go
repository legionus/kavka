package metadata

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
)

const (
	RefsObserverContextVar = "app.observer.refs"
	RefsEtcd               = "/refs"
)

var (
	refsEtcdKeyRegexp *regexp.Regexp = regexp.MustCompile("^" + RefsEtcd + "/(?P<digest>[^/]+)(/(?P<topic>[^/]+)(/(?P<partition>[0-9]+)(/(?P<id>[^/]+)(/(?P<order>[0-9]+))?)?)?)?$")
)

type RefsEtcdKey struct {
	Digest    digest.Digest `json:"digest"`
	Topic     string        `json:"topic"`
	Partition int64         `json:"partition"`
	ID        string        `json:"id"`
	Order     int64         `json:"order"`
}

func (k *RefsEtcdKey) String() (res string) {
	res = RefsEtcd
	if k.Digest != NoString {
		res += "/" + k.Digest.String()
	}
	if k.Topic != NoString {
		res += "/" + k.Topic
	}
	if k.Partition > NoPartition {
		res += fmt.Sprintf("/%d", k.Partition)
	}
	if k.ID != NoString {
		res += "/" + k.ID
	}
	if k.Order > NoOrder {
		res += fmt.Sprintf("/%020d", k.Order)
	}
	return
}

func ParseRefsEtcdKey(value string) (*RefsEtcdKey, error) {
	key := &RefsEtcdKey{}

	match := refsEtcdKeyRegexp.FindStringSubmatch(value)

	if len(match) < 1 || len(match) > 10 {
		return key, fmt.Errorf("bad refs key: %s", value)
	}

	var err error

	if len(match) > 1 {
		key.Digest, err = digest.ParseDigest(match[1])
		if err != nil {
			return key, err
		}
	}

	if len(match) > 3 {
		key.Topic = match[3]
	}

	if len(match) > 5 {
		key.Partition, err = strconv.ParseInt(match[5], 10, 64)

		if err != nil {
			return key, err
		}
	} else {
		key.Partition = NoPartition
	}

	if len(match) > 7 {
		key.ID = match[7]
	}

	if len(match) > 9 {
		key.Order, err = strconv.ParseInt(match[9], 10, 64)

		if err != nil {
			return key, err
		}
	} else {
		key.Order = NoOrder
	}
	return key, nil
}

type RefsCollection struct {
	EtcdCollection
}

func NewRefsCollection(ctx context.Context, cfg *config.Config) (EtcdCollection, error) {
	base, err := newBaseCollection(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &RefsCollection{base}, nil
}
