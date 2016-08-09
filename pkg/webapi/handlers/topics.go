package handlers

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/etcd/observer"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/storage"
	"github.com/legionus/kavka/pkg/syncer"
	"github.com/legionus/kavka/pkg/util"
	"github.com/legionus/kavka/pkg/webapi"
)

func topicGetHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	p, ok := ctx.Value(webapi.HTTPRequestQueryParamsContextVar).(*url.Values)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}
	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}

	queuesColl, err := metadata.NewQueuesCollection(ctx, cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	key := &metadata.QueueEtcdKey{
		Topic:     p.Get("topic"),
		Partition: util.ToInt64(p.Get("partition")),
	}

	offsetOldest, offsetNewest, err := getCornerOffsets(queuesColl, key.Topic, key.Partition)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to get offsets: %v", err)
		return
	}

	varsOffset := p.Get("offset")
	varsRelative := p.Get("relative")

	if varsRelative != "" {
		relative := util.ToInt64(varsRelative)

		if relative >= 0 {
			key.Offset = offsetOldest + relative
		} else {
			key.Offset = offsetNewest + relative
		}
	} else if varsOffset != "" {
		key.Offset = util.ToInt64(varsOffset)
	} else {
		// Set default value
		key.Offset = offsetOldest
	}

	if key.Offset < offsetOldest || key.Offset >= offsetNewest {
		errorOutOfRange(ctx, w, r, key.Topic, key.Partition, offsetOldest, offsetNewest)
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	res, err := queuesColl.Get(key)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to get message: %v", err)
		return
	}

	parts := strings.Split(res.Value, ",")

	chunks, err := syncer.SyncBlobSeries(ctx, parts)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	for _, chunk := range chunks {
		blobReader, err := st.Reader(chunk)
		if err != nil {
			if err == storage.ErrBlobUnknown {
				webapi.HTTPResponse(w, http.StatusNotFound, "Not found: %s", chunk.String())
			} else {
				webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
			}
			return
		}

		if _, err := io.Copy(w, blobReader); err != nil {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
			return
		}

		blobReader.Close()
	}
}

func topicPostHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	p, ok := ctx.Value(webapi.HTTPRequestQueryParamsContextVar).(*url.Values)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}

	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}

	topicsColl, err := metadata.NewTopicsCollection(ctx, cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	queuesColl, err := metadata.NewQueuesCollection(ctx, cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	var stream io.Reader = r.Body

	if cfg.Storage.MaxMessageSize > 0 {
		stream = &io.LimitedReader{
			R: r.Body,
			N: cfg.Storage.MaxMessageSize,
		}
	}

	nowValue := fmt.Sprintf("%s", time.Now())
	topicKey := &metadata.TopicEtcdKey{
		Topic:     p.Get("topic"),
		Partition: util.ToInt64(p.Get("partition")),
	}

	if err := hasKey(topicsColl, topicKey, nowValue, cfg.Storage.AllowTopicsCreation); err != nil {
		if err != metadata.ErrKeyNotFound {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		} else {
			webapi.HTTPResponse(w, http.StatusBadRequest, "%s", err)
		}
		return
	}

	topicValue, err := uploadBlobs(ctx, stream)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	res, err := queuesColl.Create(&metadata.QueueEtcdKey{
		Topic:     p.Get("topic"),
		Partition: util.ToInt64(p.Get("partition")),
	},
		strings.Join(topicValue, ","),
	)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}
	rec := res.(*metadata.QueueEtcdKey)
	out := fmt.Sprintf("{topic: %q, partition: %d, offset: %d}", rec.Topic, rec.Partition, rec.Offset)
	w.Write([]byte(out))
}

func hasKey(coll metadata.EtcdCollection, key metadata.EtcdKey, value string, allowCreation bool) error {
	if _, err := coll.Get(key); err != nil {
		if err != metadata.ErrKeyNotFound {
			return err
		}
		if !allowCreation {
			return err
		}
		return coll.Put(key, value)
	}
	return nil
}

func uploadBlobs(ctx context.Context, reader io.Reader) ([]string, error) {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return nil, fmt.Errorf("Unable to obtain config from context")
	}

	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		return nil, fmt.Errorf("Unable to obtain storage driver from context")
	}

	obsrv, ok := ctx.Value(metadata.BlobsObserverContextVar).(*observer.EtcdObserver)
	if !ok {
		return nil, fmt.Errorf("Unable to obtain blob observer from context")
	}

	blobsColl, err := metadata.NewBlobsCollection(ctx, cfg)
	if err != nil {
		return nil, err
	}

	nowValue := fmt.Sprintf("%s", time.Now())

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

		if replic[resKey.Digest] == cfg.Storage.WriteConcern {
			wg.Done()
		}
	})

	if err != nil {
		return nil, err
	}

	bf.Start()

	var (
		errIO      error
		topicValue []string
	)

	for errIO != io.EOF {
		chunk := bytes.NewBuffer(nil)

		_, errIO = io.CopyN(chunk, reader, cfg.Storage.ChunkSize)

		if errIO != nil && errIO != io.EOF {
			return nil, errIO
		}

		dgst := digest.FromBytes(chunk.Bytes())

		if has, err := st.Has(dgst); err != nil {
			return nil, err
		} else if has {
			topicValue = append(topicValue, dgst.String())
			continue
		}

		_, err = st.Write(chunk.Bytes())
		if err != nil {
			if err == storage.ErrBlobExists {
				topicValue = append(topicValue, dgst.String())
				continue
			}
			return nil, err
		}

		chunks[dgst] = make(map[string]struct{})
		replic[dgst] = int64(0)
		wg.Add(1)

		_, err = blobsColl.Create(&metadata.BlobEtcdKey{
			Digest: dgst,
			Group:  cfg.Global.Group,
			Host:   cfg.Global.Hostname,
		},
			nowValue,
		)
		if err != nil {
			return nil, err
		}

		topicValue = append(topicValue, dgst.String())
	}

	wg.Wait()
	bf.Stop()

	return topicValue, nil
}
