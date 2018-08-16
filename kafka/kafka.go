package kafka

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	"github.com/Shopify/sarama"
)

type (
	// WebSocketEvent creates a struct for sending websocket events.
	WebSocketEvent struct {
		Type string
		Body struct {
			Kind      string
			ConcertID string
			Reason    string
			Details   interface{}
		}
	}

	// NotificationEvent creates a struct for consuming notification events.
	NotificationEvent struct {
		Type string
		Body NotificationEventBody
	}

	// NotificationEventBody body for NotificationEvent.
	NotificationEventBody struct {
		Kind            string
		ConcertID       string
		Reason          string
		EnvironmentID   string
		EnvironmentName string
		NoteID          string
		SymphonyID      string
		NoteSource      string
		NoteParams      string
		Details         interface{}
	}

	// IntegrationEvent indicates some integration activity may be needed.
	IntegrationEvent struct {
		Type string
		Body struct {
			Kind      string
			ConcertID string
			Reason    string
			Details   string
		}
	}

	// KafkaEventHandler will handle the events that are consumed from Kafka.
	KafkaEventHandler func(event map[string]interface{}, offset int64) error
)

var (
	brokers            = []string{"kafka-headless:9092"}
	err                error
	simultaneousEvents = 1
)

// Begin by creating kafka consumer information/configuration
func newKafkaConfiguration() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.ChannelBufferSize = 1
	conf.Version = sarama.V0_10_1_0
	return conf
}

// Create a new kafka producer profile
func NewKafkaSyncProducer() sarama.SyncProducer {

	if runtime.GOOS == "darwin" {
		brokers = []string{"localhost:9092"}
	}

	kafka, err := sarama.NewSyncProducer(brokers, newKafkaConfiguration())

	if err != nil {
		fmt.Printf("Kafka error: %s\n", err)
		os.Exit(-1)
	}

	return kafka
}

// SendMsg Send messages to kafka
func SendMsg(kafka sarama.SyncProducer, sendTopic string, event interface{}) error {
	thisJson, err := json.Marshal(event)
	thisTopic := sendTopic

	if err != nil {
		return err
	}

	msgLog := &sarama.ProducerMessage{
		Topic: thisTopic,
		Value: sarama.StringEncoder(string(thisJson)),
	}

	partition, offset, err := kafka.SendMessage(msgLog)
	if err != nil {
		fmt.Printf("Kafka error: %s\n", err)
		return err
	}

	fmt.Printf("Message is stored in topic %v, partition %d, offset %d\n",
		thisTopic, partition, offset)

	return nil
}

// newKafkaConsumer creates a new kafka consumer profile
func newKafkaConsumer() sarama.Consumer {

	kafkaBroker := os.Getenv("KAFKA_BROKER")

	if runtime.GOOS == "darwin" {
		brokers = []string{"localhost:9092"}
	} else {
		if kafkaBroker == "" {
			fmt.Printf("$KAFKA_BROKER must be set")
			os.Exit(-1)
		}
		brokers = []string{kafkaBroker}
	}

	consumer, err := sarama.NewConsumer(brokers, newKafkaConfiguration())

	fmt.Print("Creating new Kafka Consumer \n")

	if err != nil {
		fmt.Printf("Kafka error: %s\n", err)
		os.Exit(-1)
	}

	return consumer
}

// mainConsumer creates the consumer, checks partitions and consumes each one
func MainConsumer(handler KafkaEventHandler, consumeTopic string, offset int64, updateOffset func(int64)) {
	fmt.Print("Initializing main Consumer \n")
	kafka := newKafkaConsumer()

	partitionList, err := kafka.Partitions(consumeTopic)
	if err != nil {
		fmt.Printf("Error retrieving partitionList %s\n", err)
	}

	events := make(chan *sarama.ConsumerMessage, simultaneousEvents)

	for _, partition := range partitionList {
		consumer, err := kafka.ConsumePartition(consumeTopic, partition, offset)
		if err != nil {
			fmt.Print("Unable to consume topic! \n")
			fmt.Printf("Kafka error: %s\n", err)
			os.Exit(-1)
		}

		go func(consumer sarama.PartitionConsumer) {
			for thisEvent := range consumer.Messages() {
				events <- thisEvent
			}
		}(consumer)
	}

	// Eternal consuming loop
	consumeEvents(events, handler, updateOffset)
}

// consumeEvents consumes events received
func consumeEvents(events chan *sarama.ConsumerMessage, handler KafkaEventHandler, updateOffset func(int64)) {
	var msgVal []byte
	var log interface{}
	var logMap map[string]interface{}

	fmt.Print("Service started and ready to consume Events\n")

	for thisEvent := range events {
		fmt.Printf("Reading messages from offset %#v\n", thisEvent.Offset)
		msgVal = thisEvent.Value

		if err = json.Unmarshal(msgVal, &log); err != nil {
			fmt.Printf("Failed parsing: %s\n", err)
		} else {
			logMap = log.(map[string]interface{})
			logType := logMap["Type"]
			fmt.Printf("Processing %s:\n%s\n", logType, string(msgVal))
			handlerErr := handler(logMap, thisEvent.Offset)
			fmt.Printf("Event %s executed \n", logType)
			if handlerErr == nil {
				updateOffset(thisEvent.Offset)
			}
		}
	}
}

// NewIntegrationEvent creates a new IntegrationEvent
func NewIntegrationEvent(kind string, concertID string, reason string, details string) *IntegrationEvent {
	return &IntegrationEvent{
		Type: "IntegrationEvent",
		Body: struct {
			Kind      string
			ConcertID string
			Reason    string
			Details   string
		}{Kind: kind, ConcertID: concertID, Reason: reason, Details: details},
	}
}

// NewWebSocketEvent creates a new WebSocketEvent.
func NewWebSocketEvent(kind string, concertID string, reason string, details interface{}) *WebSocketEvent {
	return &WebSocketEvent{
		Type: "WebSocketEvent",
		Body: struct {
			Kind      string
			ConcertID string
			Reason    string
			Details   interface{}
		}{Kind: kind, ConcertID: concertID, Reason: reason, Details: details},
	}
}
