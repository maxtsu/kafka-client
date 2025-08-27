package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"main/configuration"
	"main/consumerGroup"
	"os"
	"strings"
	"sync"

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
	ConfigTopic        string              `json:"config-consumer-topic,omitempty"`
	ProducerTopic      string              `json:"producer-topic,omitempty"`
	Sasl               *SaslAuthentication `json:"sasl,omitempty"`
	UseHashPartitioner bool                `json:"use-hash-partitioner,omitempty"`
	IngestConsumer     sarama.ConsumerGroup
	ConfigConsumer     sarama.Consumer
	Producer           sarama.AsyncProducer
	//processFunc        func(*work.AntWorkItemT, bool)
	monitorConsumer int
}

var Kafka = &KafkaConfig{
	BootstrapServers: strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
	IngestTopic:      "insights-ingest-data-topic",
	ConfigTopic:      "insights-ingest-config-topic",
	ProducerTopic:    "insights-topic-rule-data-producer-topic",
	Sasl: &SaslAuthentication{
		Username:    os.Getenv("KAFKA_BROKER_USERNAME"),
		Password:    os.Getenv("KAFKA_BROKER_PASSWORD"),
		Certificate: os.Getenv("KAFKA_BROKER_CERTIFICATE"),
	},
	//monitorConsumer: getMonitorConsumerEnv(),
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

// Version sarama v0.1
const config_file = "kafka-config.yaml"
const num_consumers = 1

func main() {
	fmt.Println("kafka sarama application v0.1")

	// Read the config file
	byteResult := configuration.ReadFile(config_file)
	var configYaml configuration.Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	if err != nil {
		fmt.Printf("Failed to create TLS configuration: %v\n", err)
	}
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
}
