package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/webapi"
)

// responsePartitionInfo contains information about Kafka partition.
type responsePartitionInfo struct {
	Topic        string `json:"topic"`
	Partition    int64  `json:"partition"`
	OffsetOldest int64  `json:"offsetfrom"`
	OffsetNewest int64  `json:"offsetto"`
}

func getOffset(coll metadata.EtcdCollection, key metadata.EtcdKey, opts ...metadata.GetOption) (int64, error) {
	ans, err := coll.Get(key, opts...)
	if err != nil {
		return int64(0), err
	}

	queueKey, err := metadata.ParseQueueEtcdKey(ans.RawKey)
	if err != nil {
		return int64(0), err
	}

	return queueKey.Offset, nil
}

func getCornerOffsets(coll metadata.EtcdCollection, topic string, partition int64) (int64, int64, error) {
	offsetOldest, err := getOffset(
		coll,
		&metadata.QueueEtcdKey{
			Topic:     topic,
			Partition: partition,
			Offset:    metadata.NoOffset,
		},
		metadata.PrefixKey,
		metadata.FirstKey,
	)
	if err != nil {
		return int64(0), int64(0), err
	}

	offsetNewest, err := getOffset(
		coll,
		&metadata.QueueEtcdKey{
			Topic:     topic,
			Partition: partition,
			Offset:    metadata.NoOffset,
		},
		metadata.PrefixKey,
		metadata.LastKey,
	)
	if err != nil {
		return int64(0), int64(0), err
	}

	return offsetOldest, offsetNewest + 1, nil
}

func infoSinglePartitionHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}

	p, ok := ctx.Value(webapi.HTTPRequestQueryParamsContextVar).(*url.Values)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}

	getPartitionInfo(ctx, cfg, p.Get("topic"), w)
}

func infoAllPartitionHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}

	getPartitionInfo(ctx, cfg, "", w)
}

func getPartitionInfo(ctx context.Context, cfg *config.Config, topic string, w http.ResponseWriter) {
	topicsColl, err := metadata.NewTopicsCollection(ctx, cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to open topics collection: %s", err)
		return
	}

	queuesColl, err := metadata.NewQueuesCollection(ctx, cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "nable to open queue collection: %s", err)
		return
	}

	res, err := topicsColl.List(&metadata.TopicEtcdKey{
		Topic:     topic,
		Partition: metadata.NoPartition,
	})
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to list topics: %v", err)
		return
	}

	var info []*responsePartitionInfo

	for _, v := range res {
		e := &responsePartitionInfo{}

		key, err := metadata.ParseTopicEtcdKey(v.RawKey)
		if err != nil {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "Invalid key '%s': %s", v.RawKey, err)
			return
		}
		e.Topic = key.Topic
		e.Partition = key.Partition

		e.OffsetOldest, e.OffsetNewest, err = getCornerOffsets(queuesColl, key.Topic, key.Partition)
		if err != nil {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
			return
		}

		info = append(info, e)
	}

	b, err := json.Marshal(info)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	w.Write(b)
}

func infoTopicsHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
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

	res, err := topicsColl.List(&metadata.TopicEtcdKey{
		Partition: metadata.NoPartition,
	})
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to list topics: %v", err)
		return
	}

	// FIXME
	info := make(map[string]int64)

	for _, v := range res {
		key, err := metadata.ParseTopicEtcdKey(v.RawKey)
		if err != nil {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
			return
		}
		if _, ok := info[key.Topic]; !ok {
			info[key.Topic] = int64(0)
		}
		info[key.Topic]++
	}

	delim := ""
	w.Write([]byte("["))
	for k, v := range info {
		w.Write([]byte(fmt.Sprintf("%s{\"topic\":%q,\"partitions\":%d}", delim, k, v)))
		delim = ","
	}
	w.Write([]byte("]"))
}
