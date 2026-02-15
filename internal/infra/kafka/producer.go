package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	w *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // key 决定 partition
			BatchTimeout: 50 * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *Producer) Close() error { return p.w.Close() }

func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	err := p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	})
	if err != nil {
		log.Printf("[kafka-producer] publish failed topic=%s key=%s size=%d err=%v", p.w.Topic, key, len(value), err)
		return err
	}
	log.Printf("[kafka-producer] publish success topic=%s key=%s size=%d", p.w.Topic, key, len(value))
	return nil
}
