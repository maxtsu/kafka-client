package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"main/configuration"
	"main/consumerGroup"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/gologme/log"
	"gopkg.in/yaml.v2"
)

var (
	IngestConsumerUp bool
	ConfigConsumerUp bool
	ProducerUp       bool
)

// SaslAuthentication is structure to hold authentication credentials
type SaslAuthentication struct {
	Username    string `validate:"required_with=Password gt=0" json:"username,omitempty"`
	Password    string `validate:"required_with=Username gt=0" json:"password,omitempty"`
	Certificate string `json:"certificate,omitempty"`
}

// KafkaConfig is the destination structure of KafkaConfig
type KafkaConfig struct {
	BootstrapServers   []string            `validate:"required,gt=0" json:"bootstrap-servers"`
	IngestTopic        string              `json:"ingest-consumer-topic,omitempty"`
	ProducerTopic      string              `json:"producer-topic,omitempty"`
	Sasl               *SaslAuthentication `json:"sasl,omitempty"`
	UseHashPartitioner bool                `json:"use-hash-partitioner,omitempty"`
	IngestConsumer     sarama.ConsumerGroup
	ConfigConsumer     sarama.Consumer
	Producer           sarama.AsyncProducer
	SecurityMechanism  string // "PLAIN" "OAUTHBEARER" "SCRAM-SHA-256"
	SecurityProtocol   string
	//processFunc        func(*work.AntWorkItemT, bool)
}

var Kafka = &KafkaConfig{}

// Version sarama v0.1
const config_file = "kafka-config.yaml"
const num_consumers = 1

func main() {

	run := true                            // Global run status of consumers
	sigchan := make(chan os.Signal, 1)     // signal channel to notify on SIGNALs
	signal.Notify(sigchan, syscall.SIGINT) // signal CTRL-C
	go func() {                            // goroutine to catch os signals
		for run {
			<-sigchan   // Receive signal
			run = false // Kill running consumers/producers and terminate application
		}
	}()

	fmt.Println("kafka sarama application v0.1")
	fmt.Println("Press Ctrl+C to exit")
	SetLoggingLevel("DEBUG")
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	// Read the config file
	byteResult := configuration.ReadFile(config_file)
	var configYaml configuration.Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	Kafka.BootstrapServers = strings.Split(configYaml.BootstrapServers, ",")
	Kafka.IngestTopic = configYaml.Topics
	Kafka.ProducerTopic = configYaml.Topics
	Kafka.Sasl = &SaslAuthentication{
		Username:    configYaml.SaslUsername,
		Password:    configYaml.SaslPassword,
		Certificate: configYaml.SslCaLocation,
	}
	Kafka.SecurityMechanism = configYaml.SaslMechanisms
	Kafka.SecurityProtocol = configYaml.SecurityProtocol
	//
	//
	//
	//If not a producer, then a consumer in the config yaml
	if !configYaml.Producer {

		fmt.Println("Starting a new Sarama consumer")

		// sarama config
		config := sarama.NewConfig()
		//config.Consumer.Offsets.AutoCommit.Enable = false // disable auto-commit
		config.Consumer.Offsets.AutoCommit.Enable = true // set autocommit
		config.Net.SASL.Enable = false

		switch configYaml.SecurityProtocol {
		case "SASL_SSL":

			config.Net.SASL.Enable = true
			config.Net.SASL.Handshake = true

			tlsConfig := configuration.NewTLSConfig(configYaml.SslCaLocation)
			// if err != nil {
			// 	log.Fatal(err)
			// }
			fmt.Printf("TLS %+v\n", tlsConfig)
			config.Net.TLS.Enable = true
			config.Net.TLS.Config = tlsConfig
		}
		switch configYaml.SaslMechanisms {
		case "PLAIN": // SASLTypePlaintext represents the SASL/PLAIN mechanism
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			config.Net.SASL.Enable = true
			fmt.Printf("SASL PLAIN\n")
		case "OAUTHBEARER":
			config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
		config.Net.SASL.User = configYaml.SaslUsername
		config.Net.SASL.Password = configYaml.SaslPassword

		fmt.Printf("CONFIG: %+v", config.Net)

		cgroup_wg := &sync.WaitGroup{}
		// point new consumer is created
		// Start multiple consumer workers
		ctx, cancel := context.WithCancel(context.Background())

		for id := range num_consumers {
			stamp := fmt.Sprintf("[sarama ID %d]", id)
			sarama.Logger = log.New(os.Stdout, stamp, log.LstdFlags)
			cGroup := consumerGroup.ConsumerGroup{
				Id:         id,
				Ctx:        ctx,
				Cancel:     cancel,
				C_wg:       cgroup_wg,
				Config:     config,
				ConfigYaml: configYaml,
			}
			cgroup_wg.Add(1)
			go cGroup.StartConsumerGroup()
		}
		cgroup_wg.Wait()
		fmt.Println("Application terminated")
	}
	//
	//
	// Producer
	//
	if configYaml.Producer { // Kafka Producer
		fmt.Println("Starting a new Sarama Producer")

		fmt.Printf("kafkaconfig: %+v\n", Kafka)
		Kafka.InitProducer(true)

		str := "Hello, Kafka!"
		msg := []byte(str)

		Kafka.PublishToKafka(msg, "message key")

		defer Kafka.Stop()
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Kafka Producer sarama")
		fmt.Println("Insert/Paste JSON message and press enter")
		fmt.Println("CTRL-C or CTRL-Z to cancel")
		for run {
			fmt.Print("-> ")
			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			fmt.Println("Message to send: ", text)
			// Convert string to serial byte format for transmission
			bytes := []byte(text)
			// Produce the message to the Kafka topic
			m1 := map[string][]byte{"message_key": bytes}
			BufKafkaProducerChan <- m1 // Send to producer channel
		}
	}
}

// Function to set logging level from the config.json
func SetLoggingLevel(lv string) {
	loggingLevel := lv // Set logging level From config.json
	var level int = 4
	switch loggingLevel {
	case "debug":
		level = 5
	case "info":
		level = 4
	case "warn":
		level = 3
	case "error":
		level = 2
	default:
		level = 4
	}
	// Initialize Logger
	// Level 10 = panic, fatal, error, warn, info, debug, & trace
	// Level 5 = panic, fatal, error, warn, info, & debug
	// Level 4 = panic, fatal, error, warn, & info
	// Level 3 = panic, fatal, error, & warn
	// Level 2 = panic, fatal & error
	// Level 1 = panic, fatal
	log.DisableAllLevels() // Reset all logging levels
	log.EnableLevelsByNumber(level)
	log.EnableFormattedPrefix()
	log.Infoln("Logging configured as ", strings.ToUpper(loggingLevel), ". Set at level ", level)
}

func createTLSConfiguration(cert string) (t *tls.Config) {
	caCert, err := os.ReadFile(cert)
	if err != nil {
		log.Fatal(err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	t = &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
	}
	return t
}
