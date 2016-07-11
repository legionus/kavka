package metadata

import (
	v3 "github.com/coreos/etcd/clientv3"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/etcd"
)

type baseCollection struct {
	ctx    context.Context
	client *etcd.EtcdClient
}

func newBaseCollection(ctx context.Context, cfg *config.Config) (EtcdCollection, error) {
	c, err := etcd.NewEtcdClient(cfg)
	if err != nil {
		return nil, err
	}
	return &baseCollection{
		ctx:    ctx,
		client: c,
	}, nil
}

func (b *baseCollection) Context() context.Context {
	return b.ctx
}

func (b *baseCollection) Client() *etcd.EtcdClient {
	return b.client
}

func (b *baseCollection) List(key EtcdKey) ([]EtcdValue, error) {
	resp, err := b.client.Get(b.ctx, key.String()+"/", v3.WithPrefix())
	if err != nil {
		return nil, err
	}

	res := make([]EtcdValue, len(resp.Kvs))

	for i, v := range resp.Kvs {
		res[i].RawKey = string(v.Key)
		res[i].Value = string(v.Value)
	}

	return res, nil
}

func (b *baseCollection) ListRange(firstKey EtcdKey, lastKey EtcdKey) ([]EtcdValue, error) {
	resp, err := b.client.Get(b.ctx, firstKey.String(),
		v3.WithRange(lastKey.String()),
		v3.WithSort(v3.SortByKey, v3.SortAscend),
	)
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, ErrKeyNotFound
	}

	res := make([]EtcdValue, len(resp.Kvs))

	for i, v := range resp.Kvs {
		res[i].RawKey = string(v.Key)
		res[i].Value = string(v.Value)
	}

	return res, nil
}

func (b *baseCollection) Get(key EtcdKey, opts ...GetOption) (*EtcdValue, error) {
	var ops []v3.OpOption

	for _, opt := range opts {
		switch opt {
		case FirstKey:
			for _, v := range v3.WithFirstKey() {
				ops = append(ops, v)
			}
		case LastKey:
			for _, v := range v3.WithLastKey() {
				ops = append(ops, v)
			}
		case CountKey:
			ops = append(ops, v3.WithCountOnly())
		}
	}

	resp, err := b.client.Get(b.ctx, key.String(), ops...)
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, ErrKeyNotFound
	}

	return &EtcdValue{
		RawKey: string(resp.Kvs[0].Key),
		Value:  string(resp.Kvs[0].Value),
	}, nil
}

func (b *baseCollection) Put(key EtcdKey, value string) (err error) {
	_, err = b.client.Put(b.ctx, key.String(), value)
	return
}

func (b *baseCollection) Create(key EtcdKey, value string) (EtcdKey, error) {
	_, err := b.client.Put(b.ctx, key.String(), value)
	if err != nil {
		return nil, err
	}
	return key, nil
}
