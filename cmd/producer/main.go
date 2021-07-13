package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()

	Publish("Manda ai", "gopics", producer, nil, deliveryChan)

	go DeliveryReport(deliveryChan) // async

}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "go-kafka_kafka_1:9092",
		"delivery.timeout.ms": "0",     // infinite timing
		"acks":                "all",   // [0 (fastest), 1 (medium), all(slowest)]
		"enable.idempotence":  "false", // true => acks:all || dont repeat a message
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println()
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition) // topic[partition]@offset
				/**
				 * Aqui se faria alguma acao para alertar algum sistema que a acao
				 * foi realizada com sucesso
				 */
			}
		}
	}
}
