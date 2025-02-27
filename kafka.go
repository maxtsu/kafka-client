package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gologme/log"
	"gopkg.in/yaml.v2"
)

// Version sarama
const config_file = "kafka-config.yaml"

func main() {
	fmt.Println("kafka sarama application v0.1")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Read the config file
	byteResult := ReadFile(config_file)
	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	//If not a producer, then a consumer in the config yaml
	if !configYaml.Producer {

		//keepRunning := true
		fmt.Println("Starting a new Sarama consumer")

		// sarama config
		config := sarama.NewConfig()
		config.Consumer.Offsets.AutoCommit.Enable = false          // disable auto-commit
		brokers := strings.Split(configYaml.BootstrapServers, ",") // convert string to slice/list
		topics := strings.Split(configYaml.Topics, ",")            // convert string to slice/list

		// // Create new consumer
		// consumer, err := sarama.NewConsumerGroup(brokers, configYaml.GroupID, config)
		// if err != nil {
		// 	log.Panicf("Error creating consumer group client: %v", err)
		// }

		/**
		 * Setup a new Sarama consumer group
		 */
		consumer := Consumer{
			ready: make(chan bool),
			//clientID:    clientID,
			//processFunc: processFunc,
			processChan: make(chan *sarama.ConsumerMessage, 1000),
		}

		client, err := sarama.NewConsumerGroup(brokers, configYaml.GroupID, config)
		if err != nil {
			fmt.Printf("InitConsumer: Error creating consumer group client: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		consumptionIsPaused := false
		wg := &sync.WaitGroup{}
		go func() {
			defer wg.Done()
			for {
				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				if err := client.Consume(ctx, topics, &consumer); err != nil {
					fmt.Printf("InitConsumer: Error from consumer: %v", err)
					fmt.Printf("Retrying to Connect Kafka in 30s...")
					time.Sleep(connectionRetryInterval)
				}
				// check if context was cancelled, signaling that the consumer should stop
				if ctx.Err() != nil {
					log.Errorln("InitConsumer: Stopping Consumer")
					return
				}
				consumer.ready = make(chan bool)
			}
		}()
	}
}

// configuration file kafka-config.yaml
type Config struct {
	Timestamp         bool   `yaml:"timestamp"`
	Producer          bool   `yaml:"producer"`
	BootstrapServers  string `yaml:"bootstrap.servers"`
	SaslMechanisms    string `yaml:"sasl.mechanisms"`
	SecurityProtocol  string `yaml:"security.protocol"`
	SaslUsername      string `yaml:"sasl.username"`
	SaslPassword      string `yaml:"sasl.password"`
	SslCaLocation     string `yaml:"ssl.ca.location"`
	GroupID           string `yaml:"group.id"`
	Topics            string `yaml:"topics"`
	AutoOffset        string `yaml:"auto.offset.reset"`
	PartitionStrategy string `yaml:"partition.assignment.strategy"`
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready    chan bool
	clientID string
	session  sarama.ConsumerGroupSession
	// buffered channel to hold messages pulled from kafka
	processChan chan *sarama.ConsumerMessage
	// Function to process consumed messages
	//  ===> processFunc func(*config.AntWorkItemT, bool)
}

// Function to read text file return byteResult
func ReadFile(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	byteResult, _ := io.ReadAll(file)
	file.Close()
	return byteResult
}
