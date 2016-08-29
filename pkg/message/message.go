package message

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/etcd/observer"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/storage"
	"github.com/legionus/kavka/pkg/syncer"
)

const (
	copyChunk = 128
)

func NewMessageInfo() *MessageInfo {
	return &MessageInfo{
		ID:           uuid.New(),
		CreationTime: time.Now().String(),
	}
}

func ParseMessageInfo(data string) (*MessageInfo, error) {
	res := NewMessageInfo()

	if err := json.Unmarshal([]byte(data), res); err != nil {
		return nil, err
	}
	return res, nil
}

type MessageInfo struct {
	ID           string               `json:"id"`
	CreationTime string               `json:"creation-time"`
	Blobs        []storage.Descriptor `json:"blobs"`
}

func (d MessageInfo) String() string {
	bytes, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func (d *MessageInfo) CopyOut(ctx context.Context, w io.Writer) error {
	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		return fmt.Errorf("Unable to obtain params from context")
	}

	if err := syncer.SyncBlobSeries(ctx, d.Blobs); err != nil {
		return err
	}

	for _, chunk := range d.Blobs {
		blobReader, err := st.Reader(chunk.Digest)
		if err != nil {
			if err == storage.ErrBlobUnknown {
				err = fmt.Errorf("Not found: %s", chunk.Digest.String())
			}
			return err
		}

		for err == nil {
			select {
			case <-ctx.Done():
				blobReader.Close()
				return ctx.Err()
			default:
			}
			_, err = io.CopyN(w, blobReader, copyChunk)
		}

		blobReader.Close()

		if err != io.EOF {
			return err
		}
	}

	return nil
}

func (d *MessageInfo) CopyIn(ctx context.Context, r io.Reader) error {
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

	blobsColl, err := metadata.NewBlobsCollection(ctx, cfg)
	if err != nil {
		return err
	}

	nowValue := time.Now().String()

	// Count number of groups replicated chunk.
	replic := make(map[digest.Digest]int64)

	// Track groups replacated chunk.
	chunks := make(map[digest.Digest]map[string]struct{})

	var wg sync.WaitGroup

	bf, err := observer.NewEtcdFilter(obsrv, func(ev *observer.EtcdEvent) {
		if !ev.IsCreate() {
			return
		}

		resKey, err := metadata.ParseBlobsEtcdKey(string(ev.Kv.Key))
		if err != nil {
			logrus.Error(err)
			return
		}

		if _, ok := chunks[resKey.Digest]; !ok {
			return
		}

		if _, ok := chunks[resKey.Digest][resKey.Group]; !ok {
			chunks[resKey.Digest][resKey.Group] = struct{}{}
			replic[resKey.Digest]++
		}

		if replic[resKey.Digest] == cfg.Topic.WriteConcern {
			wg.Done()
		}
	})

	if err != nil {
		return err
	}

	bf.Start()

	var errIO error

	for errIO != io.EOF {
		chunk := bytes.NewBuffer(nil)

		_, errIO = io.CopyN(chunk, r, cfg.Topic.MaxChunkSize)

		if errIO != nil && errIO != io.EOF {
			return errIO
		}

		dgst := digest.FromBytes(chunk.Bytes())

		if has, err := st.Has(dgst); err != nil {
			return err
		} else if has {
			d.Blobs = append(d.Blobs, storage.Descriptor{
				Digest: dgst,
				Size:   int64(chunk.Len()),
			})
			continue
		}

		_, err = st.Write(chunk.Bytes())
		if err != nil {
			if err == storage.ErrBlobExists {
				d.Blobs = append(d.Blobs, storage.Descriptor{
					Digest: dgst,
					Size:   int64(chunk.Len()),
				})
				continue
			}
			return err
		}

		chunks[dgst] = make(map[string]struct{})
		replic[dgst] = int64(0)
		wg.Add(1)

		_, err = blobsColl.Create(
			&metadata.BlobEtcdKey{
				Digest: dgst,
				Group:  cfg.Global.Group,
				Host:   cfg.Global.Hostname,
			},
			nowValue,
		)
		if err != nil {
			return err
		}

		d.Blobs = append(d.Blobs, storage.Descriptor{
			Digest: dgst,
			Size:   int64(chunk.Len()),
		})
	}

	wg.Wait()
	bf.Stop()

	return nil
}

func (d *MessageInfo) MakeRefs(ctx context.Context, topic string, partition int64) error {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return fmt.Errorf("Unable to obtain config from context")
	}

	refsColl, err := metadata.NewRefsCollection(ctx, cfg)
	if err != nil {
		return err
	}

	ref := &metadata.RefsEtcdKey{
		Topic:     topic,
		Partition: partition,
		ID:        d.ID,
	}

	for i, chunk := range d.Blobs {
		ref.Order = int64(i)
		ref.Digest = chunk.Digest

		_, err = refsColl.Create(ref, time.Now().String())
		if err != nil {
			return err
		}
	}

	return nil
}
