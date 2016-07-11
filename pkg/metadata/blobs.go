package metadata

import (
	"fmt"
	"regexp"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
)

var (
	BlobsObserverContextVar string         = "app.observer.blobs"
	BlobsEtcd               string         = "/blobs"
	BlobsEtcdKeyRegexp      *regexp.Regexp = regexp.MustCompile("^" + BlobsEtcd + "/(?P<digest>[a-zA-Z0-9-_+.]+:[a-fA-F0-9]+)(/(?P<group>[^/]+)(/(?P<node>.+))?)?$")
)

type BlobEtcdKey struct {
	Digest digest.Digest `json:"digest"`
	Group  string        `json:"group"`
	Host   string        `json:"host"`
}

func (k *BlobEtcdKey) String() (res string) {
	res = BlobsEtcd
	if k.Digest != "" {
		res += "/" + k.Digest.String()
	}
	if k.Group != "" {
		res += "/" + k.Group
	}
	if k.Host != "" {
		res += "/" + k.Host
	}
	return
}

func ParseBlobsEtcdKey(value string) (*BlobEtcdKey, error) {
	key := &BlobEtcdKey{}

	match := BlobsEtcdKeyRegexp.FindStringSubmatch(value)

	if len(match) < 1 || len(match) > 6 {
		return key, fmt.Errorf("bad blob key: %s", value)
	}

	var err error

	if len(match) > 1 {
		key.Digest, err = digest.ParseDigest(match[1])
		if err != nil {
			return key, err
		}
	}

	if len(match) > 3 {
		key.Group = match[3]
	}

	if len(match) > 5 {
		key.Host = match[5]
	}

	return key, nil
}

type BlobsCollection struct {
	EtcdCollection
}

func NewBlobsCollection(ctx context.Context, cfg *config.Config) (EtcdCollection, error) {
	base, err := newBaseCollection(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobsCollection{base}, nil
}
