package main

import (
	"context"
	"fmt"
	"log"
	"main/configuration"
	"main/consumerGroup"
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
