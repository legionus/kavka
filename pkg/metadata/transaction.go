package metadata

import (
	v3 "github.com/coreos/etcd/clientv3"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/etcd"
)

func NewTransaction(ctx context.Context, cfg *config.Config) *Transaction {
	return &Transaction{
		ctx: ctx,
		cfg: cfg,
	}
}

type Transaction struct {
	ctx    context.Context
	cfg    *config.Config
	ops    []v3.Op
}

func (t *Transaction) Put(key EtcdKey, value string) {
	t.ops = append(t.ops, v3.OpPut(key.String(), value))
}

func (t *Transaction) Delete(key EtcdKey) {
	t.ops = append(t.ops, v3.OpDelete(key.String()))
}

func (t *Transaction) Commit() error {
	c, err := etcd.NewEtcdClient(t.cfg)
	if err != nil {
		return err
	}
	_, err = c.Txn(t.ctx).Then(t.ops...).Commit()
	return err
}
