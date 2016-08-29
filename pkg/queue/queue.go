package queue

import (
	"fmt"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/message"
	"github.com/legionus/kavka/pkg/metadata"
)

func CreateQueue(ctx context.Context, topic string, partition int64, msg *message.MessageInfo) (*metadata.QueueEtcdKey, error) {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		return nil, fmt.Errorf("Unable to obtain config from context")
	}

	queuesColl, err := metadata.NewQueuesCollection(ctx, cfg)
	if err != nil {
		return nil, err
	}

	res, err := queuesColl.Create(
		&metadata.QueueEtcdKey{
			Topic:     topic,
			Partition: partition,
		},
		msg.String(),
	)
	if err != nil {
		return nil, err
	}

	return res.(*metadata.QueueEtcdKey), nil
}
