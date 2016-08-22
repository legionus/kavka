package cleanup

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/message"
	"github.com/legionus/kavka/pkg/metadata"
)

const dateFormat = "2006-01-02 15:04:05 -0700 MST"

func RunCleanupQueues(ctx context.Context) (chan struct{}, error) {
	stopChan := make(chan struct{})

	go func() {
		for {
			select {
			case <-time.After(10 * time.Second):
			case <-stopChan:
				return
			}

			if err := cleanupTopics(ctx); err != nil {
				logrus.Errorf("cleanup fails: %s", err)
			}
		}
	}()

	return stopChan, nil
}

func cleanupTopics(ctx context.Context) error {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return fmt.Errorf("Unable to obtain config from context")
	}

	topicColl, err := metadata.NewTopicsCollection(ctx, cfg)
	if err != nil {
		return err
	}

	records, err := topicColl.List(&metadata.TopicEtcdKey{
		Topic:     metadata.NoString,
		Partition: metadata.NoPartition,
	})
	if err != nil {
		return err
	}

	pool := make(chan int, 10)

	for _, rec := range records {
		go func(key string) {
			pool <- 1
			k, _ := metadata.ParseTopicEtcdKey(key)

			if err := cleanupExpiredMessages(ctx, k); err != nil {
				logrus.Errorf("expired messages cleanup fails for %s: %s", key, err)
			}

			if err := cleanupOversizedMessages(ctx, k); err != nil {
				logrus.Errorf("oversized messages cleanup fails for %s: %s", key, err)
			}

			<-pool
		}(rec.RawKey)
	}

	return nil
}

func cleanupExpiredMessages(ctx context.Context, topicKey *metadata.TopicEtcdKey) error {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return fmt.Errorf("Unable to obtain config from context")
	}

	if cfg.Topic.MessageRetentionPeriod == 0 {
		return nil
	}

	queueColl, err := metadata.NewQueuesCollection(ctx, cfg)
	if err != nil {
		return err
	}

	records, err := queueColl.List(&metadata.QueueEtcdKey{
		Topic:     topicKey.Topic,
		Partition: topicKey.Partition,
		Offset:    metadata.NoOffset,
	}, metadata.SortAscend)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(-1 * cfg.Topic.MessageRetentionPeriod)

	for _, rec := range records {
		key, err := metadata.ParseQueueEtcdKey(rec.RawKey)
		if err != nil {
			logrus.Errorf("Invalid key %s: %s", rec.RawKey, err)
			return err
		}

		msg, err := message.ParseMessageInfo(rec.Value)
		if err != nil {
			logrus.Errorf("Bad message: %s", err)
			return err
		}

		creationTime, err := time.Parse(dateFormat, msg.CreationTime)
		if err != nil {
			logrus.Errorf("Unable to parse date: %s", err)
			return err
		}

		if creationTime.After(deadline) {
			continue
		}

		if err := msg.RemoveRefs(ctx, topicKey.Topic, topicKey.Partition); err != nil {
			logrus.Errorf("Unable to remove references: %s", err)
			continue
		}

		err = queueColl.Delete(key)
		if err != nil {
			logrus.Errorf("Unable to remove message from queue %s: %s", key.String(), err)
			continue
		}
		logrus.Infof("Message expired: %s", key.String())

		if err := msg.Delete(ctx); err != nil {
			logrus.Errorf("Unable to remove message: %s", err)
		}
	}

	return nil
}

func cleanupOversizedMessages(ctx context.Context, topicKey *metadata.TopicEtcdKey) error {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return fmt.Errorf("Unable to obtain config from context")
	}

	if cfg.Topic.MaxPartitionSize == 0 {
		return nil
	}

	queueColl, err := metadata.NewQueuesCollection(ctx, cfg)
	if err != nil {
		return err
	}

	records, err := queueColl.List(&metadata.QueueEtcdKey{
		Topic:     topicKey.Topic,
		Partition: topicKey.Partition,
		Offset:    metadata.NoOffset,
	}, metadata.SortDescend)
	if err != nil {
		return err
	}

	queueSize := uint64(0)

	for _, rec := range records {
		key, err := metadata.ParseQueueEtcdKey(rec.RawKey)
		if err != nil {
			logrus.Errorf("Invalid key %s: %s", rec.RawKey, err)
			continue
		}

		msg, err := message.ParseMessageInfo(rec.Value)
		if err != nil {
			logrus.Errorf("Unable to parse message: %s", err)
			return err
		}

		for _, v := range msg.Blobs {
			queueSize += uint64(v.Size)
		}

		if queueSize < uint64(cfg.Topic.MaxPartitionSize) {
			continue
		}

		if err := msg.RemoveRefs(ctx, topicKey.Topic, topicKey.Partition); err != nil {
			logrus.Errorf("Unable to remove references: %s", err)
			continue
		}

		err = queueColl.Delete(key)
		if err != nil {
			logrus.Errorf("Unable to remove message from queue %s: %s", key.String(), err)
			continue
		}

		if err := msg.Delete(ctx); err != nil {
			logrus.Errorf("Unable to remove message: %s", err)
		}
	}

	return nil
}
