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

func (b *baseCollection) List(key EtcdKey, opts ...ListOption) ([]EtcdValue, error) {
	var ops []v3.OpOption

	ops = append(ops, v3.WithPrefix())

	for _, opt := range opts {
		switch opt {
		case SortAscend:
			ops = append(ops, v3.WithSort(v3.SortByKey, v3.SortAscend))
		case SortDescend:
			ops = append(ops, v3.WithSort(v3.SortByKey, v3.SortDescend))
		}
	}

	resp, err := b.client.Get(b.ctx, key.String()+"/", ops...)
	if err != nil {
		return nil, err
	}

	res := make([]EtcdValue, len(resp.Kvs))

	for i, v := range resp.Kvs {
		res[i].RawKey = string(v.Key)
		res[i].Value = string(v.Value)
		res[i].Count = resp.Count
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
		res[i].Count = resp.Count
	}

	return res, nil
}

func (b *baseCollection) Get(key EtcdKey, opts ...GetOption) (*EtcdValue, error) {
	// FIXME
	countOnly := false

	var ops []v3.OpOption

	for _, opt := range opts {
		switch opt {
		case FirstKey:
			ops = append(ops, v3.WithFirstKey()...)
		case LastKey:
			ops = append(ops, v3.WithLastKey()...)
		case CountKey:
			countOnly = true
			ops = append(ops, v3.WithCountOnly())
		}
	}

	resp, err := b.client.Get(b.ctx, key.String(), ops...)
	if err != nil {
		return nil, err
	}

	if countOnly {
		return &EtcdValue{
			Count: resp.Count,
		}, nil
	}

	if resp.Count == 0 {
		return nil, ErrKeyNotFound
	}

	return &EtcdValue{
		RawKey: string(resp.Kvs[0].Key),
		Value:  string(resp.Kvs[0].Value),
		Count:  resp.Count,
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

func (b *baseCollection) Delete(key EtcdKey) (err error) {
	_, err = b.client.Delete(b.ctx, key.String())
	return
}
