package kafkaData

import (
	"context"
	"fmt"
	"kafkaData/configuration"
	"kafkaData/consumerGroup"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"gopkg.in/yaml.v2"
)

// Version sarama v0.1
const config_file = "kafka-config.yaml"
const num_consumers = 5

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
		//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

		// sarama config
		config := sarama.NewConfig()
		//config.Consumer.Offsets.AutoCommit.Enable = false // disable auto-commit
		config.Consumer.Offsets.AutoCommit.Enable = true // seet autocommit
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
		case "OAUTHBEARER":
			config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
		config.Net.SASL.User = configYaml.SaslUsername
		config.Net.SASL.Password = configYaml.SaslPassword

		fmt.Printf("CONFIH: %+v", config.Net)

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

func (k *KafkaConfig) InitProducer(retry bool) {
	if len(k.BootstrapServers) == 0 || k.BootstrapServers[0] == "" {
		return
	}
	if log.GetLevel().String() == "debug" {
		sarama.Logger = logger.New(os.Stdout, "[sarama] ", logger.LstdFlags)
	}

	sarama.MaxRequestSize = 1024 * 1024 * 100 // Max Batch size 100 Mb

	// producer config
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Producer.Retry.Max = 2
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.ClientID = util.GetEnv("POD_NAME", "paragon-insights-tand")
	var username, password, cert string
	if k.Sasl != nil {
		username = k.Sasl.Username
		password = k.Sasl.Password
		cert = k.Sasl.Certificate
	}
	if username == "" || password == "" {
		config.Net.SASL.Enable = false
	} else {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
		config.Net.SASL.Handshake = true
	}
	if cert == "" {
		config.Net.TLS.Enable = false
	} else {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = createTLSConfiguration(cert)
	}

	// async producer
	prd, err := sarama.NewAsyncProducer(k.BootstrapServers, config)

	if err != nil {
		log.Errorln("Could not initialize kafka producer: ", err)
		if retry {
			k.RetryProducerConnection()
		}
		return
	} else if !retry && stopKafkaProducerRetry != nil {
		// Retry is Successful
		stopKafkaProducerRetry <- struct{}{}
		stopKafkaProducerRetry = nil
	}
	k.Producer = prd
	for i := 0; i < int(NoOfProducerGoRoutines); i++ {
		go func() {
			stopChannel := make(chan struct{}, 1)
			stopKafkaProducerChannels = append(stopKafkaProducerChannels, stopChannel)
			ProducerUp = true

			for {
				select {
				case message := <-BufKafkaProducerChan:
					for k, v := range message {
						go Kafka.PublishToKafka(v, k)
					}
				case <-stopChannel:
					ProducerUp = false
					return
				}
			}
		}()
	}
}
