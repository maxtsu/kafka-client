package consumerGroup

import (
	"context"
	"fmt"
	"main/configuration"
	"main/consumer"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type ConsumerGroup struct {
	Id         int
	Ctx        context.Context
	Cancel     context.CancelFunc
	C_wg       *sync.WaitGroup
	Config     *sarama.Config
	ConfigYaml configuration.Config
}

func (groupConsumer *ConsumerGroup) StartConsumerGroup() {
	defer groupConsumer.C_wg.Done()
	brokers := strings.Split(groupConsumer.ConfigYaml.BootstrapServers, ",") // convert string to slice/list
	topics := strings.Split(groupConsumer.ConfigYaml.Topics, ",")            // convert string to slice/list
	keepRunning := true

	consumer := consumer.CreateConsumer(groupConsumer.Id)

	client, err := sarama.NewConsumerGroup(brokers, groupConsumer.ConfigYaml.GroupID, groupConsumer.Config)
	if err != nil {
		fmt.Printf("InitConsumer %d: Error creating consumer group client: %v\n", groupConsumer.Id, err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(groupConsumer.Ctx, topics, &consumer); err != nil {
				fmt.Printf("Consumer: %d Error from consumer: %v", groupConsumer.Id, err)
				fmt.Printf("Consumer: %d Retrying to Connect Kafka in 30s...", groupConsumer.Id)

				connectionRetryInterval := time.Duration(30) * time.Second
				time.Sleep(connectionRetryInterval)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if groupConsumer.Ctx.Err() != nil {
				fmt.Printf("Consumer: %d Stopping Consumer\n", groupConsumer.Id)
				return
			}
			consumer.Ready = make(chan bool)
			fmt.Printf("Consumer: %d Post-consumer.Ready\n", groupConsumer.Id)
		}
	}()

	<-consumer.Ready // Await till the consumer has been set up
	fmt.Printf("Consumer: %d Sarama consumer ready\n", groupConsumer.Id)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Run msg process in gorouting
	go consumer.ProcessIngestMessages()

	for keepRunning {
		select {
		case <-groupConsumer.Ctx.Done():
			fmt.Printf("Consumer: %d terminating: context cancelled\n", groupConsumer.Id)
			keepRunning = false
		case <-sigterm:
			fmt.Printf("Consumer: %d terminating: via signal\n", groupConsumer.Id)
			keepRunning = false
		}
	}
	groupConsumer.Cancel() // close all consumers
	fmt.Printf("Consumer: %d ctx cancelled\n", groupConsumer.Id)
	wg.Wait()
	if err = client.Close(); err != nil {
		fmt.Printf("Consumer: %d InitConsumer: Error closing client: %v\n", groupConsumer.Id, err)
	}
	fmt.Printf("Consumer: %d completed terminated function\n", groupConsumer.Id)
}
