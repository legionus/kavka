package syncer

import (
	"fmt"
	"time"
	"sync"

	"github.com/Sirupsen/logrus"

	"github.com/legionus/kavka/pkg/client"
	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/storage"
)

func SyncBlob(ctx context.Context, dgst digest.Digest) error {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return fmt.Errorf("Unable to obtain config from context")
	}

	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		return fmt.Errorf("unable to obtain storage driver from context")
	}

	blobsColl, err := metadata.NewBlobsCollection(ctx, cfg)
	if err != nil {
		return err
	}

	prefixKey := &metadata.BlobEtcdKey{
		Digest: dgst,
	}

	nodes, err := blobsColl.List(prefixKey)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		return fmt.Errorf("digest not found")
	}

	// FIXME use goroutines
	for _, node := range nodes {
		key, err := metadata.ParseBlobsEtcdKey(node.RawKey)
		if err != nil {
			logrus.Errorf("unable to parse key %s: %v", node.RawKey, err)
			continue
		}

		c, err := client.New(fmt.Sprintf("%s:%d", key.Host, cfg.Global.Port), 3*time.Second)
		if err != nil {
			logrus.Errorf("unable to make client for remote server %s: %v", key.Host, err)
			continue
		}

		data, err := c.GetBlob(dgst.String())
		if err != nil {
			logrus.Errorf("unable to get blob %s from remote server %s: %v", dgst.String(), key.Host, err)
			continue
		}

		res, err := st.Write(data)

		if res != dgst {
			logrus.Errorf("produce different digests when sync blob %s from remote server %s: got %s", dgst.String(), key.Host, res.String())
			continue
		}

		if err != nil && err != storage.ErrBlobExists {
			logrus.Errorf("unable to write blob %s: %v", dgst.String(), err)
			return err
		}

		logrus.Infof("sync %s from remote server %s", dgst.String(), key.Host)
		return nil
	}

	return fmt.Errorf("unable to sync %s", dgst.String())
}

func SyncBlobSeries(ctx context.Context, parts []string) ([]digest.Digest, error) {
	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		return nil, fmt.Errorf("unable to obtain storage driver from context")
	}

	chunks := make([]digest.Digest, len(parts))
	errors := make([]error, len(parts))

	var wg sync.WaitGroup

	for i, chunk := range parts {
		dgst, err := digest.ParseDigest(chunk)
		if err != nil {
			return nil, fmt.Errorf("Bad digest '%s': %v", chunk, err)
		}
		chunks[i] = dgst

		wg.Add(1)

		go func(i int, dgst digest.Digest) {
			defer wg.Done()

			var has bool
			if has, errors[i] = st.Has(dgst); errors[i] != nil {
				return
			} else if has {
				return
			}
			errors[i] = SyncBlob(ctx, dgst)
		}(i, dgst)
	}
	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	return chunks, nil
}
