package consumer

import (
	"fmt"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/gologme/log"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ID       int
	Ready    chan bool
	clientID string
	session  sarama.ConsumerGroupSession
	// buffered channel to hold messages pulled from kafka
	processChan chan *sarama.ConsumerMessage
	// Function to process consumed messages
	//  ===> processFunc func(*config.AntWorkItemT, bool)
}

// Create the ready channel
func CreateConsumer(id int) Consumer {
	consumer := Consumer{ // Setup a new Sarama consumer group
		ID:    id,
		Ready: make(chan bool),
	}
	consumer.processChan = make(chan *sarama.ConsumerMessage, 1024)
	return consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
// Setup implements sarama.ConsumerGroupHandler.
func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.Ready)
	fmt.Printf("Consumer: %d Setup memberid %+v sessionid %+v claims %+v\n", consumer.ID, session.MemberID(), session.GenerationID(), session.Claims())
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler.
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer cleanup.")
	fmt.Println("Cleanup ", "memberid ", session.MemberID(), "sessionid ", session.GenerationID(), "claims ", session.Claims())
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	consumer.session = session
	fmt.Println("ConsumeClaim Started")
	for {
		select {
		case message := <-claim.Messages():
			consumer.processChan <- message
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// processIngestMessages process messages on [consumer.processChan].
func (consumer *Consumer) ProcessIngestMessages() {
	for message := range consumer.processChan {
		if message == nil {
			log.Debugln("Received nil message, continuing.")
			continue
		}
		// Update LastIngestMessageTime here since it is used to check if
		// the consumer is properly consuming data, even if the data is old.
		// LastIngestMessageTime = time.Now()
		// partitionStr := strconv.FormatInt(int64(message.Partition), 10)
		// if time.Since(message.Timestamp) > 5*time.Minute {
		// 	log.Errorf(
		// 		"processKafkaMessages: Skipping message. partition=%s, offset=%d, key=%s",
		// 		partitionStr, message.Offset, string(message.Key),
		// 	)
		// 	continue
		// }
		fmt.Printf("PART: %s MSG: %s\n", strconv.Itoa(int(message.Partition)), string(message.Value))
	}
	fmt.Println("Completed IngestMessaging")
}
