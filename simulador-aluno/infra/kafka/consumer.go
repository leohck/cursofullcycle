package kafka

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

type ConsumerKafka struct {
	MsgChannel chan *ckafka.Message
}

func NewKafkaConsumer(msgChan chan *ckafka.Message) *ConsumerKafka {
	return &ConsumerKafka{
		MsgChannel: msgChan,
	}
}

func (k *ConsumerKafka) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KafkaBootstrapServers"),
		"group.id":          os.Getenv("KafkaConsumerGroupId"),
	}

	c, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatalf("error consuming kafka message: " + err.Error())
	}

	topics := []string{os.Getenv("KafkaReadTopic")}
	c.SubscribeTopics(topics, nil)

	fmt.Println("Kafka Consumer has been started")

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			k.MsgChannel <- msg
		}
	}
}
