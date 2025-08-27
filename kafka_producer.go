package main

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/gologme/log"
)

var (
	NoOfProducerGoRoutines = 1
	// buffered channel to hold map of partitionKey -> message that needs to be published to kafka
	BufKafkaProducerChan      = make(chan map[string][]byte, 1000)
	stopKafkaProducerChannels []chan struct{}
	stopKafkaProducerRetry    chan struct{}
	connectionRetryInterval   = time.Duration(30) * time.Second
)

func (k *KafkaConfig) InitProducer(retry bool) {
	if len(k.BootstrapServers) == 0 || k.BootstrapServers[0] == "" {
		return
	}
	// if log.GetLevel().String() == "debug" {
	// 	sarama.Logger = logger.New(os.Stdout, "[sarama] ", logger.LstdFlags)
	// }

	sarama.MaxRequestSize = 1024 * 1024 * 100 // Max Batch size 100 Mb

	// producer config
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Producer.Retry.Max = 2
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.ClientID = "GROUP-IP"
	var username, password, cert string
	if k.Sasl != nil {
		username = k.Sasl.Username
		password = k.Sasl.Password
		cert = k.Sasl.
			Certificate
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
	fmt.Printf("async producer\n")

	if err != nil {
		fmt.Printf("err\n")
		log.Errorln("Could not initialize kafka producer: ", err)
		if retry {
			fmt.Printf("Retry\n")
			k.RetryProducerConnection()
		}
		return
	} else if !retry && stopKafkaProducerRetry != nil {
		fmt.Printf("elsi if retry\n")
		// Retry is Successful
		stopKafkaProducerRetry <- struct{}{}
		stopKafkaProducerRetry = nil
	}
	k.Producer = prd
	fmt.Printf("Start for loop \n")
	for i := 0; i < int(NoOfProducerGoRoutines); i++ {
		go func() {
			fmt.Printf("Start go func \n")
			stopChannel := make(chan struct{}, 1)
			stopKafkaProducerChannels = append(stopKafkaProducerChannels, stopChannel)
			ProducerUp = true
			fmt.Printf("ProducerUp \n")

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

// PublishToKafka publishes messages to kafka
func (k *KafkaConfig) PublishToKafka(message []byte, kafkaPartionKey string) {
	// skip publishing to destination if not connected
	if k.Producer == nil {
		return
	}

	// publish sync
	msg := &sarama.ProducerMessage{
		Topic: k.ProducerTopic,
		Value: sarama.ByteEncoder(message),
	}

	// If message key is present then key hash is used to map messages
	// to same partitions. If not present, all messages will be
	// distributed randomly over the different partitions.
	msg.Key = sarama.StringEncoder(kafkaPartionKey)
	k.Producer.Input() <- msg

	select {
	case <-k.Producer.Successes():
		log.Debugln("Successfully published to Kafka partition: ", kafkaPartionKey)
		break
	case err := <-k.Producer.Errors():
		log.Errorln("Error while publishing to Kafka partition:", kafkaPartionKey, "err: ", err)
		break
	}
}

// RetryConnection retries connection to Kafka every 30s until connection is made
func (k *KafkaConfig) RetryProducerConnection() {
	timer := time.NewTicker(connectionRetryInterval)
	stopConnectionRetryChannel := make(chan struct{}, 1)
	stopKafkaProducerRetry = stopConnectionRetryChannel
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			k.InitProducer(false)
		case <-stopConnectionRetryChannel:
			return
		}
	}
}

// Stop sends signal for all running go routines to stop
// and closes all connections which were instantiated
func (k *KafkaConfig) Stop() {
	if stopKafkaProducerRetry != nil {
		stopKafkaProducerRetry <- struct{}{}
	}
	for _, channel := range stopKafkaProducerChannels {
		channel <- struct{}{}
	}
	stopKafkaProducerRetry = nil
	stopKafkaProducerChannels = nil
}
