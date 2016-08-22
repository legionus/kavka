package cleanup

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/storage"
)

func RunCleanupStorage(ctx context.Context) (chan struct{}, error) {
	stopChan := make(chan struct{})

	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return stopChan, fmt.Errorf("Unable to obtain config from context")
	}

	go func() {
		for {
			select {
			case <-time.After(cfg.Storage.CleanupPeriod):
			case <-stopChan:
				return
			}
			if err := CleanupStorage(ctx); err != nil {
				logrus.Errorf("Storage cleanup fails: %s", err)
			}
		}
	}()

	return stopChan, nil
}

func CleanupStorage(ctx context.Context) error {
	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		return fmt.Errorf("Unable to obtain params from context")
	}

	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return fmt.Errorf("Unable to obtain config from context")
	}

	blobsColl, err := metadata.NewBlobsCollection(ctx, cfg)
	if err != nil {
		return err
	}

	refsColl, err := metadata.NewRefsCollection(ctx, cfg)
	if err != nil {
		return err
	}

	st.Iterate(func(k storage.Key, v storage.Blob) (bool, error) {
		dgst, err := digest.ParseDigest(string(k))
		if err != nil {
			logrus.Errorf("Unable to parse key: %s", err)
			return false, nil
		}

		value, err := refsColl.Get(
			&metadata.RefsEtcdKey{
				Digest: dgst,
			},
			metadata.CountKey,
		)
		if err != nil {
			logrus.Errorf("Unable to check key: %s", err)
			return false, nil
		}

		if value.Count > 0 {
			return false, nil
		}

		logrus.Infof("storage key %s is definitely lost", dgst)

		blobKey := &metadata.BlobEtcdKey{
			Digest: dgst,
			Group:  cfg.Global.Group,
			Host:   cfg.Global.Hostname,
		}

		if err = blobsColl.Delete(blobKey); err != nil {
			logrus.Errorf("Unable to delete metadata for %s: %s", blobKey.String(), err)
			return false, nil
		}

		if err = st.Delete(dgst); err != nil {
			logrus.Errorf("Unable to remove %s from storage: %s", dgst, err)
		}

		return false, nil
	})

	return nil
}
