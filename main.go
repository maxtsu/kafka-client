package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
	"gopkg.in/yaml.v2"
)

// Version 0.9
const config_file = "kafka-config.yaml"

func main() {
	fmt.Println("kafka application sarama v0.1")
	// sigchan := make(chan os.Signal, 1)
	// signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Rad the config file
	byteResult := ReadFile(config_file)

	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	brokers := strings.Split(configYaml.BootstrapServers, ",")
	groupID := configYaml.GroupID
	topics := strings.Split(configYaml.Topics, ",")

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // adjust to match your Kafka cluster version

	// SASL/SSL (if your cluster is secured)
	// sasl.mechanism PLAIN OAUTHBEARER SCRAM
	if configYaml.SaslMechanisms == "PLAIN" { // GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER.
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext // or OAUTHBEARER, SCRAM
		config.Net.SASL.User = "username"
		config.Net.SASL.Password = "password"

		config.Net.TLS.Enable = true
		// Optionally set config.Net.TLS.Config = &tls.Config{...} for custom CA/cert
	}

	if configYaml.SecurityProtocol == "PLAINTEXT" { // PLAINTEXT = no TLS, no SASL
		config.Net.TLS.Enable = false
		config.Net.SASL.Enable = false
	} else if configYaml.SecurityProtocol == "SASL_SSL" { // security.protocol = SASL_SSL
		config.Net.TLS.Enable = true
		// Load CA certificate
		tlsCfg, err := tlsConfigFromCA(configYaml.SslCaLocation)
		if err != nil {
			panic(err)
		}
		config.Net.TLS.Config = tlsCfg
		// config.Net.TLS.Config = &tls.Config{
		// 	InsecureSkipVerify: false, // true only if testing with self‑signed certs
		// }
	}

	if !configYaml.Producer {
		fmt.Println("kafka consumer")
		// Set partition strategy
		if configYaml.BalanceStrategy == "range" {
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
		} else if configYaml.BalanceStrategy == "roundrobin" {
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
		} else if configYaml.BalanceStrategy == "sticky" {
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		}
		// Set offest postition
		if configYaml.ConsumerOffsets == "latest" {
			config.Consumer.Offsets.Initial = sarama.OffsetNewest
		} else if configYaml.ConsumerOffsets == "earliest" {
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
		}

		config.Consumer.Return.Errors = true

		// // For stable consumer offsets commits manually set
		// config.Consumer.Group.Session.Timeout = 10 * 1000 // ms
		// config.Consumer.Group.Heartbeat.Interval = 3 * 1000 // ms

		// Create consumer group
		cg, err := sarama.NewConsumerGroup(brokers, groupID, config)
		if err != nil {
			fmt.Printf("Error creating consumer group: %v", err)
		}
		defer cg.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Handle signals for graceful shutdown
		go func() {
			sigchan := make(chan os.Signal, 1)
			signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
			<-sigchan
			log.Println("Shutdown signal received")
			cancel()
		}()

		handler := consumerGroupHandler{}

		// Consume in a loop to handle rebalances and errors
		for {
			if err := cg.Consume(ctx, topics, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			// If context was cancelled, exit loop
			if ctx.Err() != nil {
				break
			}
		}
		fmt.Println("Consumer group closed")

	} else { //This is a producer
		fmt.Println("kafka producer")
		prod, err := sarama.NewAsyncProducer(brokers, config)
		if err != nil {
			log.Fatalf("create async producer: %v", err)
		}
		defer prod.Close()

		// Goroutine to log successes
		go func() {
			for m := range prod.Successes() {
				fmt.Printf("ok topic=%s partition=%d offset=%d\n", m.Topic, m.Partition, m.Offset)
			}
		}()

		// Goroutine to log errors
		go func() {
			for e := range prod.Errors() {
				log.Printf("err topic=%s: %v", e.Msg.Topic, e.Err)
				// Optional: implement a retry/backoff queue if needed
			}
		}()

		// Keyboard reader
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Kafka Producer")
		fmt.Println("Insert/Paste JSON message and press enter")
		fmt.Println("CTRL-C or CTRL-Z to cancel")
		for {
			fmt.Print("-> ")
			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			fmt.Println("Message to send: ", text)
			msg := &sarama.ProducerMessage{
				Topic: configYaml.Topics,
				Key:   sarama.StringEncoder(configYaml.MessageKey),
				Value: sarama.StringEncoder(text),
			}
			prod.Input() <- msg

			if err != nil {
				fmt.Printf("Failed to produce message: %s\n", err)
				return
			}
			fmt.Println("Message produced successfully!")
		}
	}
}

// configuration file kafka-config.yaml
type Config struct {
	Timestamp        bool   `yaml:"timestamp"`
	Producer         bool   `yaml:"producer"`
	BootstrapServers string `yaml:"bootstrap.servers"`
	SaslMechanisms   string `yaml:"sasl.mechanisms"`
	SecurityProtocol string `yaml:"security.protocol"`
	SaslUsername     string `yaml:"sasl.username"`
	SaslPassword     string `yaml:"sasl.password"`
	SslCaLocation    string `yaml:"ssl.ca.location"`
	GroupID          string `yaml:"group.id"`
	MessageKey       string `yaml:"message.key"`
	Topics           string `yaml:"topics"`
	ConsumerOffsets  string `yaml:"Consumer.Offsets"`
	BalanceStrategy  string `yaml:"BalanceStrategy"`
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

type consumerGroupHandler struct{}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error { return nil }

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim starts a consumer loop of the given claim (partition)
// Must run the loop and return only when claim.Messages() channel is closed
func (consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message: topic=%s partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// Mark message consumed for commit
		sess.MarkMessage(msg, "")
	}
	return nil
}

func tlsConfigFromCA(path string) (*tls.Config, error) {
	caCert, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA cert")
	}

	return &tls.Config{
		RootCAs: caPool,
	}, nil
}
