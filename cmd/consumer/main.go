package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go-kafka_kafka_1:9092",
		"client.id":         "gonsumer-app",
		"group.id":          "gonsumer-group",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println("Erro ao criar Consumer: ", err.Error())
	}

	topics := []string{"gopics"}
	c.SubscribeTopics(topics, nil)
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}
