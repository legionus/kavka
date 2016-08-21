package metadata

import (
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/etcd"
)

type GetOption int
type ListOption int

const (
	PrefixKey GetOption = iota
	FirstKey
	LastKey
	CountKey

	SortAscend ListOption = iota
	SortDescend
)

type EtcdKey interface {
	String() string
}

type EtcdCollection interface {
	Context() context.Context
	Client() *etcd.EtcdClient
	List(prefixKey EtcdKey, opts ...ListOption) ([]EtcdValue, error)
	ListRange(firstKey EtcdKey, lastKey EtcdKey) ([]EtcdValue, error)
	Get(key EtcdKey, opts ...GetOption) (*EtcdValue, error)
	Create(key EtcdKey, value string) (EtcdKey, error)
	Put(key EtcdKey, value string) error
	Delete(key EtcdKey) error
}

type EtcdValue struct {
	RawKey string
	Value  string
	Count  int64
}
