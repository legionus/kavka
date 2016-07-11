package observer

import (
	"sync"

	"github.com/Sirupsen/logrus"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/pborman/uuid"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/etcd"
)

type EtcdEvent struct {
	*v3.Event
}

type EtcdObserveKey string
type EtcdObserveHandler func(ev *EtcdEvent)

type EtcdObserver struct {
	sync.RWMutex

	etcdclient *etcd.EtcdClient
	handlers   map[EtcdObserveKey]EtcdObserveHandler
}

func NewEtcdObserver(cfg *config.Config) (*EtcdObserver, error) {
	c, err := etcd.NewEtcdClient(cfg)
	if err != nil {
		return nil, err
	}

	return &EtcdObserver{
		etcdclient: c,
		handlers:   make(map[EtcdObserveKey]EtcdObserveHandler),
	}, nil
}

func (bs *EtcdObserver) RegisterHandler(handler EtcdObserveHandler) EtcdObserveKey {
	bs.Lock()
	defer bs.Unlock()

	id := EtcdObserveKey(uuid.New())

	bs.handlers[id] = handler
	return id
}

func (bs *EtcdObserver) UnregisterHandler(id EtcdObserveKey) {
	bs.Lock()
	defer bs.Unlock()

	delete(bs.handlers, id)
}

func (bs *EtcdObserver) Observe(ctx context.Context, path string) error {
	for {
		watcher := bs.etcdclient.Watch(ctx, path, v3.WithPrefix())
		if watcher == nil {
			logrus.Fatalf("Unable to make Watch channel")
			return etcd.ErrNoWatcher
		}

		for wresp := range watcher {
			for _, ev := range wresp.Events {
				// Waiting all handlers necessary to comply with sequence of event processing.
				// A side effect of this is that one slow handler can hold them all.
				var wg sync.WaitGroup

				bs.Lock()
				for _, v := range bs.handlers {
					wg.Add(1)
					go func(handler EtcdObserveHandler, ev *v3.Event) {
						handler(&EtcdEvent{ev})
						wg.Done()
					}(v, ev)
				}
				bs.Unlock()

				wg.Wait()
			}

			if wresp.Canceled {
				break
			}
		}
	}
	return nil
}

func (bs *EtcdObserver) RunEtcdObserver(path string) {
	go bs.Observe(context.Background(), path)
}
