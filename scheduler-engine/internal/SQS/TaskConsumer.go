package SQS

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"scheduler-engine/internal/util"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

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
	logger            *zap.Logger
}

func NewSQSConsumer(queueURL string, workers int) (*SQSConsumer, error) {
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
		logger:            util.GetLogger("logs/sqs.log", 10, 5, 28),
	}, nil
}

// Start begins consuming messages
func (c *SQSConsumer) Start() {
	c.logger.Info(fmt.Sprintf("Starting SQS consumer with %d workers for queue: %s", c.workers, c.queueURL))

	// Start worker goroutines
	for i := 0; i < c.workers; i++ {
		c.logger.Info(fmt.Sprintf("Starting worker %d", i))
		c.wg.Add(1)
		go c.worker(i)
	}

	c.logger.Info("SQS consumer started successfully")
}

func (c *SQSConsumer) Stop() {
	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			c.logger.Error(err.Error())
		}
	}(c.logger)

	c.logger.Info("Stopping SQS consumer...")
	close(c.shutdown)
	c.wg.Wait()
	c.logger.Info("SQS consumer stopped")
}

func (c *SQSConsumer) worker(workerID int) {
	defer c.wg.Done()
	c.logger.Info(fmt.Sprintf("Worker %d started", workerID))

	for {
		select {
		case <-c.shutdown:
			c.logger.Info(fmt.Sprintf("Worker %d shutting down", workerID))
			return
		default:
			c.pollAndProcessMessages(workerID)
		}
	}
}

func (c *SQSConsumer) pollAndProcessMessages(workerID int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(int(c.visibilityTimeout))*time.Second)
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
		if ctx.Err() == context.DeadlineExceeded {
			c.logger.Error(fmt.Sprintf("Worker %d: Context deadline exceeded", workerID))
		}
		c.logger.Error(fmt.Sprintf("Worker %d: Error receiving messages: %v", workerID, err))
		//time.Sleep(5 * time.Second) // Back off on error
		return
	}

	if len(result.Messages) == 0 {
		return
	}

	c.logger.Info(fmt.Sprintf("Worker %d: Received %d messages", workerID, len(result.Messages)))

	for _, msg := range result.Messages {

		if err := c.processMessage(workerID, msg); err != nil {
			c.logger.Error(fmt.Sprintf("Worker %d: Error processing message %s: %v",
				workerID, aws.ToString(msg.MessageId), err))
			//sendToDLQ
		}

		if err := c.deleteMessage(ctx, msg); err != nil {
			c.logger.Error(fmt.Sprintf("Worker %d: Error deleting message %s: %v",
				workerID, aws.ToString(msg.MessageId), err))
		}
	}
}

func (c *SQSConsumer) processMessage(workerID int, sqsMsg types.Message) error {
	messageID := aws.ToString(sqsMsg.MessageId)
	body := aws.ToString(sqsMsg.Body)

	c.logger.Info(fmt.Sprintf("Worker %d: Processing message %s with body ", workerID, messageID, body))

	var msg Message
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	c.logger.Info(fmt.Sprintf("Worker %d: Processed message %s with body %s", workerID, messageID, msg.Payload["message"]))
	return nil
}

func (c *SQSConsumer) deleteMessage(ctx context.Context, msg types.Message) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &c.queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})

	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	c.logger.Info(fmt.Sprintf("Deleted message %s", aws.ToString(msg.MessageId)))
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

	c.logger.Info(fmt.Sprintf("Queue stats - Visible: %s, In-flight: %s",
		result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)],
		result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)],
	))

	return nil
}
