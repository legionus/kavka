package syncer

import (
	"fmt"

	"github.com/Sirupsen/logrus"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/etcd/observer"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/storage"
)

func RunSyncer(ctx context.Context) error {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return fmt.Errorf("Unable to obtain config from context")
	}

	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		return fmt.Errorf("Unable to obtain storage driver from context")
	}

	obsrv, ok := ctx.Value(metadata.BlobsObserverContextVar).(*observer.EtcdObserver)
	if !ok {
		return fmt.Errorf("Unable to obtain blob observer from context")
	}

	pool := make(chan int, cfg.Storage.SyncPool)

	bf, err := observer.NewEtcdFilter(obsrv, func(ev *observer.EtcdEvent) {
		if !ev.IsCreate() {
			return
		}

		k, err := metadata.ParseBlobsEtcdKey(string(ev.Kv.Key))
		if err != nil {
			logrus.Error(err)
			return
		}

		if k.Host == cfg.Global.Hostname {
			return
		}

		go func(dgst digest.Digest) {
			pool <- 1

			if has, err := st.Has(dgst); err != nil {
				logrus.Error(err)
				<-pool
				return
			} else if has {
				<-pool
				return
			}

			SyncBlob(ctx, dgst)

			<-pool
		}(k.Digest)
	})

	if err != nil {
		return err
	}

	bf.Start()

	return nil
}
