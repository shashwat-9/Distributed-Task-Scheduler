package SQS

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"scheduler-engine/internal/util"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var logger *zap.Logger

type Message struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	Timestamp time.Time              `json:"timestamp"`
}

type SQSConsumer struct {
	client            *sqs.Client
	queueURL          string
	maxMessages       int32
	waitTimeSeconds   int32
	visibilityTimeout int32
	workers           int
	shutdown          chan struct{}
	wg                sync.WaitGroup
}

func NewSQSConsumer(queueURL string, workers int) (*SQSConsumer, error) {
	logger = util.GetLogger("logs/sqs.log", 10, 5, 28)
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("ap-south-1"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &SQSConsumer{
		client:            sqs.NewFromConfig(awsConfig),
		queueURL:          queueURL,
		maxMessages:       10,
		waitTimeSeconds:   20,
		visibilityTimeout: 30,
		workers:           workers,
		shutdown:          make(chan struct{}),
	}, nil
}

// Start begins consuming messages
func (c *SQSConsumer) Start() {
	logger.Info(fmt.Sprintf("Starting SQS consumer with %d workers for queue: %s", c.workers, c.queueURL))

	// Start worker goroutines
	for i := 0; i < c.workers; i++ {
		logger.Info(fmt.Sprintf("Starting worker %d", i))
		c.wg.Add(1)
		go c.worker(i)
	}

	logger.Info("SQS consumer started successfully")
}

// Stop gracefully stops the consumer
func (c *SQSConsumer) Stop() {
	log.Println("Stopping SQS consumer...")
	close(c.shutdown)
	c.wg.Wait()
	log.Println("SQS consumer stopped")
}

func (c *SQSConsumer) worker(workerID int) {
	defer c.wg.Done()
	logger.Info(fmt.Sprintf("Worker %d started", workerID))

	for {
		select {
		case <-c.shutdown:
			logger.Info(fmt.Sprintf("Worker %d shutting down", workerID))
			return
		default:
			c.pollAndProcessMessages(workerID)
		}
	}
}

func (c *SQSConsumer) pollAndProcessMessages(workerID int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &c.queueURL,
		MaxNumberOfMessages:   c.maxMessages,
		WaitTimeSeconds:       c.waitTimeSeconds,
		VisibilityTimeout:     c.visibilityTimeout,
		MessageAttributeNames: []string{"All"},
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameAll,
		},
	})

	if err != nil {
		log.Printf("Worker %d: Error receiving messages: %v", workerID, err)
		time.Sleep(5 * time.Second) // Back off on error
		return
	}

	if len(result.Messages) == 0 {
		return
	}

	logger.Info(fmt.Sprintf("Worker %d: Received %d messages", workerID, len(result.Messages)))

	// Process each message
	for _, msg := range result.Messages {
		if err := c.processMessage(workerID, msg); err != nil {
			log.Printf("Worker %d: Error processing message %s: %v",
				workerID, aws.ToString(msg.MessageId), err)
			// In production, you might want to send to DLQ or retry
			continue
		}

		// Delete message after successful processing
		if err := c.deleteMessage(ctx, msg); err != nil {
			log.Printf("Worker %d: Error deleting message %s: %v",
				workerID, aws.ToString(msg.MessageId), err)
		}
	}
}

func (c *SQSConsumer) processMessage(workerID int, sqsMsg types.Message) error {
	messageID := aws.ToString(sqsMsg.MessageId)
	body := aws.ToString(sqsMsg.Body)

	log.Printf("Worker %d: Processing message %s with body ", workerID, messageID, body)

	var msg Message
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Process based on message type
	switch msg.Type {
	default:
		//add function
		log.Printf("Unknown message type: %s", msg.Type)
		return nil // Don't fail for unknown types
	}
}

func (c *SQSConsumer) deleteMessage(ctx context.Context, msg types.Message) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &c.queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})

	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	log.Printf("Deleted message %s", aws.ToString(msg.MessageId))
	return nil
}

func (c *SQSConsumer) GetQueueAttributes() error {
	result, err := c.client.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: &c.queueURL,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})

	if err != nil {
		return fmt.Errorf("failed to get queue attributes: %w", err)
	}

	log.Printf("Queue stats - Visible: %s, In-flight: %s",
		result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)],
		result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)],
	)

	return nil
}

func main() {
	// Configuration
	queueURL := "https://sqs.us-east-1.amazonaws.com/123456789012/your-queue-name"
	workers := 3

	// Create consumer
	consumer, err := NewSQSConsumer(queueURL, workers)
	if err != nil {
		log.Fatal("Failed to create SQS consumer:", err)
	}

	// Get queue info
	if err := consumer.GetQueueAttributes(); err != nil {
		log.Printf("Warning: Could not get queue attributes: %v", err)
	}

	// Start consuming
	consumer.Start()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("SQS Consumer is running. Press Ctrl+C to stop...")
	<-sigChan

	// Graceful shutdown
	consumer.Stop()
}
