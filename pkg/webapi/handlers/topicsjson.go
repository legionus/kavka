package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/message"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/queue"
	"github.com/legionus/kavka/pkg/util"
	"github.com/legionus/kavka/pkg/webapi"
)

func jsonGetHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	// TODO: use webapi.IsAlive to cancel context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	length := util.ToInt64(p.Get("limit"))
	if length <= 0 {
		length = 1
	}

	lastkey := &metadata.QueueEtcdKey{
		Topic:     key.Topic,
		Partition: key.Partition,
		Offset:    key.Offset + length,
	}

	if lastkey.Offset >= offsetNewest {
		lastkey.Offset = offsetNewest
	}

	queryStr, err := json.Marshal(key)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to marshal json: %v", err)
		return
	}

	mataRange, err := queuesColl.ListRange(key, lastkey)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	successSent := false

	for _, msg := range mataRange {
		if !webapi.IsAlive(w) {
			return
		}

		data, err := message.ParseMessageInfo(msg.Value)
		if err != nil {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
			return
		}

		if !successSent {
			successSent = true

			w.Write([]byte(`{`))
			w.Write([]byte(`"query":`))
			w.Write(queryStr)
			w.Write([]byte(`,"messages":[`))
		} else {
			w.Write([]byte(`,`))
		}

		if err := data.CopyOut(ctx, w); err != nil {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		}
	}

	if !successSent {
		w.Write([]byte(`{`))
		w.Write([]byte(`"query":`))
		w.Write(queryStr)
		w.Write([]byte(`,"messages":[`))
	}

	w.Write([]byte(`]}`))
}

func jsonPostHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
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

	var stream io.Reader = r.Body

	if cfg.Topic.MaxMessageSize > 0 {
		stream = &io.LimitedReader{
			R: r.Body,
			N: cfg.Topic.MaxMessageSize,
		}
	}

	topicKey := &metadata.TopicEtcdKey{
		Topic:     p.Get("topic"),
		Partition: util.ToInt64(p.Get("partition")),
	}

	if err := hasKey(topicsColl, topicKey, time.Now().String(), cfg.Topic.AllowTopicsCreation); err != nil {
		if err != metadata.ErrKeyNotFound {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		} else {
			webapi.HTTPResponse(w, http.StatusBadRequest, "creating partitions is prohibited")
		}
		return
	}

	msg, err := ioutil.ReadAll(stream)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to read body: %s", err)
		return
	}

	var m json.RawMessage
	if err = json.Unmarshal(msg, &m); err != nil {
		webapi.HTTPResponse(w, http.StatusBadRequest, "Message must be JSON")
		return
	}

	topicValue := message.NewMessageInfo()

	if err := topicValue.CopyIn(ctx, bytes.NewReader(msg)); err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	if err := topicValue.MakeRefs(ctx, topicKey.Topic, topicKey.Partition); err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	rec, err := queue.CreateQueue(ctx, topicKey.Topic, topicKey.Partition, topicValue)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	out := fmt.Sprintf("{topic: %q, partition: %d, offset: %d}", rec.Topic, rec.Partition, rec.Offset)
	w.Write([]byte(out))
}
