package kafkaproducer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaProducerInterface interface {
	Write(str []byte) (int, error)
}

type kafkaProducer struct {
	producer *kafka.Writer
	topic    string
}

func NewKafkaProducer(brokers []string, topic string, isAsync bool) *kafkaProducer {
	producer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    isAsync,
	}

	return &kafkaProducer{
		producer: producer,
		topic:    topic,
	}
}

func (kp *kafkaProducer) produce(value []byte) error {
	return kp.producer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("logs"),
			Value: value,
		},
	)
}

func (kp *kafkaProducer) Write(val []byte) (int, error) {
	return len(val), kp.produce(val)
}
