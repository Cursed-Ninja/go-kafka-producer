package kafka_producer

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducerInterface interface {
	Write(str []byte) (int, error)
}

type kafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer(brokers []string, topic string) (*kafkaProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"client.id":         "main-server",
		"acks":              "all",
	})

	if err != nil {
		return nil, err
	}

	return &kafkaProducer{
		producer: producer,
		topic:    topic,
	}, nil
}

func (kp *kafkaProducer) produce(value string) error {
	return kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: int32(kafka.PartitionAny)},
		Value:          []byte(value),
	}, nil)
}

func (kp *kafkaProducer) Write(val []byte) (int, error) {
	return len(val), kp.produce(string(val))
}
