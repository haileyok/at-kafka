// This is copied from https://github.com/bluesky-social/go-util/blob/main/pkg/bus/producer/producer.go, except it
// removes the proto.message type and removes serialization. Should pass raw JSON bytes instead.

package atkafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	DefaultClientID = "go-bus-producer"
	ModeAsync       = "async"
	ModeSync        = "sync"
)

// Producer is an example producer to a given topic using given Protobuf message type.
//
// A Producer takes a Kafka client and a topic, and sends one of two types of data:
//
//   - A Protobuf message of the given type.
//   - Invalid data that could not be parsed as any Protobuf message.
type Producer struct {
	clientID             string
	client               *kgo.Client
	topic                string
	topicConfig          []string
	topicPartitions      int
	replicationFactor    int
	ensureTopic          bool
	clientConfig         kafka.Config
	logger               *slog.Logger
	defaultAsyncCallback func(r *kgo.Record, err error)

	saslUsername string
	saslPassword string

	// Close the producer and flush any async writes.
	// We can safely call this concurrently and multiple times, as it uses a sync.Once to ensure the client is only flushed and closed once.
	Close func()
}

// New returns a new Producer.
//
// Always use this constructor to construct Producers.
func NewProducer(
	ctx context.Context,
	logger *slog.Logger,
	bootstrapServers []string,
	topic string,
	options ...ProducerOption,
) (*Producer, error) {
	if len(bootstrapServers) == 0 {
		return nil, errors.New("at least one bootstrap server must be provided")
	}

	// By default, append the first bootstrap server's host to the client ID.
	// This should allow us to connect to the Kafka broker using a different hostname
	// than what the broker advertises, which is useful when connecting to a Kafka cluster
	// from outside of the k8s cluster
	parts := strings.Split(bootstrapServers[0], ":")
	firstBootstrapHost := parts[0]
	var clientID string

	clientHostname, err := os.Hostname()
	if err != nil {
		clientID = fmt.Sprintf("%s;host_override=%s", DefaultClientID, firstBootstrapHost)
	} else {
		clientID = fmt.Sprintf("%s;host_override=%s", clientHostname, firstBootstrapHost)
	}

	producer := &Producer{
		topic:             topic,
		clientID:          clientID,
		topicPartitions:   1,
		replicationFactor: 1,
	}
	if logger != nil {
		producer.logger = logger.With("component", "bus-producer", "topic", topic)
	}

	for _, option := range options {
		option(producer)
	}

	producer.clientConfig = kafka.Config{
		BootstrapServers:  bootstrapServers,
		ClientID:          producer.clientID,
		Topic:             topic,
		TopicConfig:       producer.topicConfig,
		TopicPartitions:   producer.topicPartitions,
		ReplicationFactor: producer.replicationFactor,
		SASLUsername:      producer.saslUsername,
		SASLPassword:      producer.saslPassword,
	}

	// Initialize the Kafka client if not already set.
	if producer.client == nil {
		// Default Kafka client configuration.
		client, err := kafka.NewKafkaClient(producer.clientConfig, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka client: %w", err)
		}
		producer.client = client
	}

	// Ensure the topic exists if requested.
	if producer.ensureTopic {
		if err := kafka.EnsureTopic(ctx, producer.client, producer.clientConfig); err != nil {
			return nil, fmt.Errorf("failed to ensure Kafka topic: %w", err)
		}
	}

	producer.defaultAsyncCallback = func(r *kgo.Record, err error) {
		if err != nil {
			producer.log(ctx, slog.LevelError, "failed to async produce record", "key", string(r.Key), "err", err)
		}
	}

	producer.Close = sync.OnceFunc(func() {
		producer.close()
	})

	return producer, nil
}

// ProducerOption is an option when constructing a new Producer.
//
// All parameters except options are required. ProducerOptions allow
// for optional parameters.
type ProducerOption func(*Producer)

// WithTopicConfig allows setting topic configuration options for the producer.
func WithTopicConfig(config ...string) ProducerOption {
	return func(p *Producer) {
		p.topicConfig = config
	}
}

// WithTopicPartitions allows setting the number of partitions for the topic.
func WithTopicPartitions(partitions int) ProducerOption {
	return func(p *Producer) {
		p.topicPartitions = partitions
	}
}

// WithReplicationFactor allows setting the replication factor for the topic.
func WithReplicationFactor(replicationFactor int) ProducerOption {
	return func(p *Producer) {
		p.replicationFactor = replicationFactor
	}
}

// WithRetentionBytes allows setting the retention bytes for the topic.
func WithRetentionBytes(bytes int) ProducerOption {
	return func(p *Producer) {
		p.topicConfig = append(p.topicConfig, fmt.Sprintf("retention.bytes=%d", bytes))
	}
}

// WithRetentionTime allows setting the retention time for the topic (to millisecond precision).
func WithRetentionTime(duration time.Duration) ProducerOption {
	return func(p *Producer) {
		p.topicConfig = append(p.topicConfig, fmt.Sprintf("retention.ms=%d", int64(duration/time.Millisecond)))
	}
}

// WithMaxMessageBytes allows setting the maximum message size for the topic.
// Default is 1MB (1048576 bytes).
func WithMaxMessageBytes(bytes int) ProducerOption {
	return func(p *Producer) {
		p.topicConfig = append(p.topicConfig, fmt.Sprintf("max.message.bytes=%d", bytes))
	}
}

// WithInfiniteRetention allows setting the topic to have infinite retention time.
// By default Topics have a retention time of 7 days and infinite retention bytes.
func WithInfiniteRetention() ProducerOption {
	return func(p *Producer) {
		p.topicConfig = append(p.topicConfig, "retention.ms=-1")
	}
}

// WithClient allows initializing the producer with the specified Kafka client.
// This overrides the provided topic configuration and partitions.
func WithClient(client *kgo.Client) ProducerOption {
	return func(p *Producer) {
		p.client = client
	}
}

// WithHostOverride returns a new Producer Option that overrides the default broker host
// This allows us to connect to the Kafka broker using a different hostname than what the broker advertises.
func WithHostOverride(host string) ProducerOption {
	return func(producer *Producer) {
		if host != "" {
			producer.clientID += ";host_override=" + host
		}
	}
}

// WithEnsureTopic allows setting whether to ensure the topic exists before producing messages.
// If true, the producer will attempt to create the topic if it does not exist.
func WithEnsureTopic(ensure bool) ProducerOption {
	return func(p *Producer) {
		p.ensureTopic = ensure
	}
}

// WithCredentials sets the SASL username and password for the producer.
func WithCredentials(username, password string) ProducerOption {
	return func(p *Producer) {
		p.saslUsername = username
		p.saslPassword = password
	}
}

// ProduceAsync asynchronously sends the given message to
// the Producer's topic with the given partition key.
// This should return immediately, and the message will be sent in the background.
// Errors returned from this method are only related to serialization issues.
func (p *Producer) ProduceAsync(ctx context.Context, key string, message []byte, cb func(r *kgo.Record, err error)) error {
	if cb == nil {
		cb = p.defaultAsyncCallback
	}

	rec := &kgo.Record{
		Key:   []byte(key),
		Value: message,
		Topic: p.topic,
	}

	p.client.Produce(ctx, rec, cb)

	return nil
}

// ProduceSync serializes the given Protobuf messages, and synchronously
// sends it to the Producer's topic with the given partition key.
// This will block until the message is sent or an error occurs.
func (p *Producer) ProduceSync(ctx context.Context, key string, message []byte) error {
	if err := p.client.ProduceSync(ctx, &kgo.Record{
		Key:   []byte(key),
		Value: message,
		Topic: p.topic,
	}).FirstErr(); err != nil {
		return fmt.Errorf("failed to synchronously produce record with key %q: %w", key, err)
	}
	return nil
}

func (p *Producer) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p.log(ctx, slog.LevelInfo, "closing producer, waiting up to 5 seconds to flush async writes")

	err := p.client.Flush(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			p.log(context.Background(), slog.LevelWarn, "producer flush timed out, closing client without waiting for all messages to be sent")
		} else {
			p.log(context.Background(), slog.LevelError, "failed to flush producer", "err", err)
		}
	} else {
		p.logger.Info("producer flushed successfully on close")
	}
	p.client.Close()
}

func (p *Producer) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if p.logger != nil {
		p.logger.Log(ctx, level, msg, args...)
	}
}
