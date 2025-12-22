package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"gopkg.in/yaml.v2"
)

// Version 0.9
const config_file = "kafka-config.yaml"

func main() {
	fmt.Println("kafka application sarama v0.1")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Rad the config file
	byteResult := ReadFile(config_file)

	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	brokers := configYaml.BootstrapServers
	groupID := configYaml.GroupID
	topics := configYaml.Topics

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // adjust to match your Kafka cluster version


// Set partition strategy
if 
    config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange
    config.Consumer.Offsets.Initial = sarama.OffsetNewest // or sarama.OffsetOldest
    config.Consumer.Return.Errors = true



	// Create kafka consumer configuration for kafkaCfg
	// consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 	"bootstrap.servers":  configYaml.BootstrapServers,
	// 	"sasl.mechanisms":    configYaml.SaslMechanisms,
	// 	"security.protocol":  configYaml.SecurityProtocol,
	// 	"sasl.username":      configYaml.SaslUsername,
	// 	"sasl.password":      configYaml.SaslPassword,
	// 	"ssl.ca.location":    configYaml.SslCaLocation,
	// 	"group.id":           configYaml.GroupID,
	// 	"session.timeout.ms": 6000,
	// 	// Start reading from the first message of each assigned
	// 	// partition if there are no previously committed offsets
	// 	// for this group.
	// 	"auto.offset.reset": configYaml.AutoOffset,
	// 	// Whether or not we store offsets automatically.
	// 	"enable.auto.offset.store":      false,
	// 	"partition.assignment.strategy": configYaml.PartitionStrategy,
	// })
	// if err != nil {
	// 	fmt.Println("Failed to create consumer. ", err)
	// 	os.Exit(1)
	// }
	// fmt.Println("Created Consumer. ", consumer)

	// topics := []string{configYaml.Topics}

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
	MessageKey        string `yaml:"message.key"`
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
