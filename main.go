package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/yaml.v2"
)

// Version 0.9
const config_file = "kafka-config.yaml"

func main() {
	fmt.Println("kafka application v0.9")
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

	//If not a producer, then a consumer in the config yaml
	if !configYaml.Producer {
		// Create kafka consumer configuration for kafkaCfg
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  configYaml.BootstrapServers,
			"sasl.mechanisms":    configYaml.SaslMechanisms,
			"security.protocol":  configYaml.SecurityProtocol,
			"sasl.username":      configYaml.SaslUsername,
			"sasl.password":      configYaml.SaslPassword,
			"ssl.ca.location":    configYaml.SslCaLocation,
			"group.id":           configYaml.GroupID,
			"session.timeout.ms": 6000,
			// Start reading from the first message of each assigned
			// partition if there are no previously committed offsets
			// for this group.
			"auto.offset.reset": configYaml.AutoOffset,
			// Whether or not we store offsets automatically.
			"enable.auto.offset.store":      false,
			"partition.assignment.strategy": configYaml.PartitionStrategy,
		})
		if err != nil {
			fmt.Println("Failed to create consumer. ", err)
			os.Exit(1)
		}
		fmt.Println("Created Consumer. ", consumer)

		topics := []string{configYaml.Topics}

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
