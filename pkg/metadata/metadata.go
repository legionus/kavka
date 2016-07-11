package metadata

import (
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/etcd"
)

type GetOption int

const (
	PrefixKey GetOption = iota
	FirstKey
	LastKey
	CountKey
)

type EtcdKey interface {
	String() string
}

type EtcdCollection interface {
	Context() context.Context
	Client() *etcd.EtcdClient
	List(prefixKey EtcdKey) ([]EtcdValue, error)
	ListRange(firstKey EtcdKey, lastKey EtcdKey) ([]EtcdValue, error)
	Get(key EtcdKey, opts ...GetOption) (*EtcdValue, error)
	Create(key EtcdKey, value string) (EtcdKey, error)
	Put(key EtcdKey, value string) error
}

type EtcdValue struct {
	RawKey string
	Value  string
}
