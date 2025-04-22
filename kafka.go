package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"main/consumer"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"gopkg.in/yaml.v2"
)

// Version sarama v1.2
const config_file = "kafka-config.yaml"

func main() {
	fmt.Println("kafka sarama application v0.1")

	// Read the config file
	byteResult := ReadFile(config_file)
	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	if err != nil {
		fmt.Printf("Failed to create TLS configuration: %v", err)
	}
	//If not a producer, then a consumer in the config yaml
	if !configYaml.Producer {

		fmt.Println("Starting a new Sarama consumer")
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
		// brokers := strings.Split(configYaml.BootstrapServers, ",") // convert string to slice/list
		// topics := strings.Split(configYaml.Topics, ",")            // convert string to slice/list

		// sarama config
		config := sarama.NewConfig()
		config.Consumer.Offsets.AutoCommit.Enable = false // disable auto-commit
		config.Net.SASL.Enable = false
		switch configYaml.SaslMechanisms {
		case "PLAIN": // SASLTypePlaintext represents the SASL/PLAIN mechanism
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "OAUTHBEARER":
			config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
		config.Net.SASL.User = configYaml.SaslUsername
		config.Net.SASL.Password = configYaml.SaslPassword

		cgroup_wg := &sync.WaitGroup{}
		// point new consumer is created
		// Start multiple consumer workers
		ctx, cancel := context.WithCancel(context.Background())

		for id := range 5 {
			cWorker := consumeWork{
				id:         id,
				ctx:        ctx,
				cancel:     cancel,
				c_wg:       cgroup_wg,
				config:     config,
				configYaml: configYaml,
			}

			cgroup_wg.Add(1)
			go cWorker.consumerWorker()
			//go consumerWorker(id, cgroup_wg, config, configYaml)
		}

		//point consumer is finished
		fmt.Println("Just before cg_group wait")
		cgroup_wg.Wait()
		fmt.Println("completely at the End")
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

type consumeWork struct {
	id         int
	ctx        context.Context
	cancel     context.CancelFunc
	c_wg       *sync.WaitGroup
	config     *sarama.Config
	configYaml Config
}

func (c *consumeWork) consumerWorker() {
	//func consumerWorker(id int, c_wg *sync.WaitGroup, config *sarama.Config, configYaml Config) {
	defer c.c_wg.Done()
	brokers := strings.Split(c.configYaml.BootstrapServers, ",") // convert string to slice/list
	topics := strings.Split(c.configYaml.Topics, ",")            // convert string to slice/list
	keepRunning := true

	consumer := consumer.CreateConsumer(c.id)

	client, err := sarama.NewConsumerGroup(brokers, c.configYaml.GroupID, c.config)
	if err != nil {
		fmt.Printf("InitConsumer %d: Error creating consumer group client: %v\n", c.id, err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(c.ctx, topics, &consumer); err != nil {
				fmt.Printf("Consumer: %d Error from consumer: %v", c.id, err)
				fmt.Printf("Consumer: %d Retrying to Connect Kafka in 30s...", c.id)

				connectionRetryInterval := time.Duration(30) * time.Second
				time.Sleep(connectionRetryInterval)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if c.ctx.Err() != nil {
				fmt.Printf("Consumer: %d Stopping Consumer\n", c.id)
				return
			}
			consumer.Ready = make(chan bool)
			fmt.Printf("Consumer: %d Post-consumer.Ready\n", c.id)
		}
	}()

	<-consumer.Ready // Await till the consumer has been set up
	fmt.Printf("Consumer: %d Sarama consumer ready\n", c.id)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Run msg process in gorouting
	go consumer.ProcessIngestMessages()

	for keepRunning {
		select {
		case <-c.ctx.Done():
			fmt.Printf("Consumer: %d terminating: context cancelled\n", c.id)
			keepRunning = false
		case <-sigterm:
			fmt.Printf("Consumer: %d terminating: via signal\n", c.id)
			keepRunning = false
		}
	}
	c.cancel() // close all consumers
	fmt.Printf("Consumer: %d ctx cancelled\n", c.id)
	wg.Wait()
	if err = client.Close(); err != nil {
		fmt.Printf("Consumer: %d InitConsumer: Error closing client: %v\n", c.id, err)
	}
	fmt.Printf("Consumer: %d completed terminated function\n", c.id)
}
