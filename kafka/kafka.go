package kafka

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"

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
	}

	// IntegrationEvent indicates some integration activity may be needed.
	IntegrationEvent struct {
		Type string
		Body struct {
			Kind      string
			ConcertID string
			Reason    string
		}
	}

	// KafkaEventHandler will handle the events that are consumed from Kafka.
	KafkaEventHandler func(event map[string]interface{}, offset int64)
)

var (
	brokers = []string{"kafka-headless:9092"}
	err     error
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
	thisOldJson, err := json.Marshal(event)
	// workaround for double marshal
	thisJson := strings.Replace(string(thisOldJson), `\"`, `"`, -1)
	thisJson = strings.Replace(string(thisJson), `"[`, `[`, -1)
	thisJson = strings.Replace(string(thisJson), `]"`, `]`, -1)
	thisJson = strings.Replace(string(thisJson), `"{`, `{`, -1)
	thisJson = strings.Replace(string(thisJson), `}"`, `}`, -1)
	// end workaround for double marshal
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

	for _, partition := range partitionList {
		consumer, err := kafka.ConsumePartition(consumeTopic, partition, offset)
		if err != nil {
			fmt.Print("Unable to consume topic! \n")
			fmt.Printf("Kafka error: %s\n", err)
			os.Exit(-1)
		}

		// Eternal consuming loop
		consumeEvents(consumer, handler, updateOffset)
	}
}

// consumeEvents consumes events received
func consumeEvents(consumer sarama.PartitionConsumer, handler KafkaEventHandler, updateOffset func(int64)) {
	var msgVal []byte
	var log interface{}
	var logMap map[string]interface{}

	fmt.Print("Solve Notifier started and ready to consume Events\n")

	// Startup completed.

	if err != nil {
		for err := range consumer.Errors() {
			fmt.Print("Failed to consumeEvents - stack trace/error message below\n")
			fmt.Printf("Kafka error: %s\n", err)
			consumer.AsyncClose()
		}
	}

	for thisEvent := range consumer.Messages() {
		fmt.Printf("Reading messages from offset %#v\n", thisEvent.Offset)
		msgVal = thisEvent.Value

		if err = json.Unmarshal(msgVal, &log); err != nil {
			fmt.Printf("Failed parsing: %s\n", err)
		} else {
			logMap = log.(map[string]interface{})
			logType := logMap["Type"]
			fmt.Printf("Processing %s:\n%s\n", logType, string(msgVal))
			handler(logMap, thisEvent.Offset)
			fmt.Printf("Event %s executed \n", logType)
			updateOffset(thisEvent.Offset)
		}
	}
}
