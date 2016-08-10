package handlers

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/message"
	"github.com/legionus/kavka/pkg/metadata"
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

	data, err := message.ParseMessageInfo(res.Value)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	if err := data.CopyOut(ctx, w); err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
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

	topicValue := &message.MessageInfo{
		CreationTime: nowValue,
	}

	if err := topicValue.CopyIn(ctx, stream); err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	res, err := queuesColl.Create(
		&metadata.QueueEtcdKey{
			Topic:     p.Get("topic"),
			Partition: util.ToInt64(p.Get("partition")),
		},
		topicValue.String(),
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
