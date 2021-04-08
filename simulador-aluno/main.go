package main

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	kafka2 "github.com/leohck/imersaofsfc2-simulator/application/kafka"
	"github.com/leohck/imersaofsfc2-simulator/infra/kafka"
	"log"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("error loading .env file")
	}
}

func main() {
	msgChan := make(chan *ckafka.Message)
	consumer := kafka.NewKafkaConsumer(msgChan)

	go consumer.Consume()

	for msg := range msgChan {
		fmt.Println(string(msg.Value))
		go kafka2.Produce(msg)
	}
}

// {"clientId":"1", "routeId":"1"}
// {"clientId":"2", "routeId":"2"}
// {"clientId":"3", "routeId":"3"}
