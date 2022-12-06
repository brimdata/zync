package fifo

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CreateMissingTopics creates topics, ignoring any that already exist.
func CreateMissingTopics(ctx context.Context, opts []kgo.Opt, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) error {
	client, err := kadm.NewOptClient(opts...)
	if err != nil {
		return err
	}
	// Create topics one by one to avoid a timeout due to a slow broker.
	for _, topic := range topics {
		resps, err := client.CreateTopics(ctx, partitions, replicationFactor, configs, topic)
		if err != nil {
			return err
		}
		for _, r := range resps {
			if r.Err != nil && r.Err != kerr.TopicAlreadyExists {
				return fmt.Errorf("creating topic %s: %w", r.Topic, r.Err)
			}
		}
	}
	return nil
}
