package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	consumerConfig "scheduler-engine/internal/config"
	"scheduler-engine/internal/k8s"
	"scheduler-engine/internal/models"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSClient struct {
	client         *sqs.Client
	ConsumerConfig consumerConfig.TaskConsumerConfig
	DLQConfig      consumerConfig.DLQProducerConfig
	workerCount    int
	shutdown       chan struct{}
	wg             sync.WaitGroup
	logger         *zap.Logger
	kubeClient     *k8s.KubernetesManager
}

func NewSQSClient(taskConsumerConfig consumerConfig.TaskConsumerConfig, workerCount int, namedLogger *zap.Logger) (*SQSClient, error) {
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(taskConsumerConfig.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	kubernetesClient, err := k8s.GetKubernetesManager()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", err)
	}
	return &SQSClient{
		client:         sqs.NewFromConfig(awsConfig),
		ConsumerConfig: taskConsumerConfig,
		workerCount:    workerCount,
		shutdown:       make(chan struct{}),
		kubeClient:     kubernetesClient,
		logger:         namedLogger,
	}, nil
}

func (c *SQSClient) Start() {
	c.logger.Info(fmt.Sprintf("Starting Task consumer with %d workers for queue: %s", c.workerCount, c.ConsumerConfig.QueueURL))

	for i := 0; i < c.workerCount; i++ {
		//c.logger.Info(fmt.Sprintf("Starting worker %d", i))
		c.wg.Add(1)
		go c.worker(i)
	}

	c.logger.Info("Task consumer started successfully")
}

func (c *SQSClient) Stop() {
	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			c.logger.Error(err.Error())
		}
	}(c.logger)

	c.logger.Info("Stopping consumer...")
	close(c.shutdown)
	c.wg.Wait()
	c.logger.Info("consumer stopped")
}

func (c *SQSClient) worker(workerID int) {
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

func (c *SQSClient) pollAndProcessMessages(workerID int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(int(c.ConsumerConfig.VisibilityTimeout))*time.Second)
	defer cancel()

	result, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &c.ConsumerConfig.QueueURL,
		MaxNumberOfMessages:   c.ConsumerConfig.MaxMessages,
		WaitTimeSeconds:       c.ConsumerConfig.WaitTimeSeconds,
		VisibilityTimeout:     c.ConsumerConfig.VisibilityTimeout,
		MessageAttributeNames: []string{"All"},
	})

	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.logger.Error(fmt.Sprintf("Worker %d: Context deadline exceeded", workerID))
			return
		}
		c.logger.Error(fmt.Sprintf("Worker %d: Error receiving messages: %v", workerID, err))
		return
	}

	if len(result.Messages) == 0 {
		return
	}

	c.logger.Info(fmt.Sprintf("Worker %d: Received %d messages", workerID, len(result.Messages)))

	for _, msg := range result.Messages {

		select {

		case <-ctx.Done():
			c.logger.Error(fmt.Sprintf("Worker %d: Context deadline exceeded", workerID))
			return
		default:
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error(fmt.Sprintf("Worker %d: Error processing message %s: %v",
					workerID, aws.ToString(msg.MessageId), err))
				err := c.sendToDLQ(msg, err.Error())
				if err != nil {
					continue
				}
			}

			if err := c.deleteMessage(ctx, msg); err != nil {
				c.logger.Error(fmt.Sprintf("Worker %d: Error deleting message %s: %v",
					workerID, aws.ToString(msg.MessageId), err))
			}
		}
	}
}

func (c *SQSClient) processMessage(ctx context.Context, sqsMsg types.Message) error {
	messageID := aws.ToString(sqsMsg.MessageId)
	body := aws.ToString(sqsMsg.Body)

	c.logger.Info(fmt.Sprintf("Worker: Processing message %s with body %s", messageID, body))

	var msg models.Task
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	exists, err := c.kubeClient.JobExistsByLabel("execution_id", msg.JobId, msg.Namespace)
	if err != nil {
		//Check for cases of job creation issues to push to DLQ
		//Check for cluster issues and log them
		return err
	} else if exists == true {
		err := c.deleteMessage(ctx, sqsMsg)
		if err != nil {
			return err
		}
		c.logger.Info(fmt.Sprintf("Worker: Job %s already exists, deleting message %s", msg.Name, messageID))
		return nil
	}
	err = c.kubeClient.CreateJob(msg)
	if err != nil {
		return err
	}
	return nil
}

func (c *SQSClient) deleteMessage(ctx context.Context, msg types.Message) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &c.ConsumerConfig.QueueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})

	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	c.logger.Info(fmt.Sprintf("Deleted message %s", aws.ToString(msg.MessageId)))
	return nil
}

func (c *SQSClient) sendToDLQ(originalMessage types.Message, errorReason string) error {
	dlqMessage := map[string]interface{}{
		"originalMessageId":  aws.ToString(originalMessage.MessageId),
		"originalBody":       aws.ToString(originalMessage.Body),
		"errorReason":        errorReason,
		"timestamp":          time.Now().UTC().Format(time.RFC3339),
		"originalAttributes": originalMessage.MessageAttributes,
	}

	messageBody, err := json.Marshal(dlqMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", err)
	}

	_, err = c.client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:    &c.DLQConfig.QueueURL,
		MessageBody: aws.String(string(messageBody)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"ErrorReason": {
				DataType:    aws.String("String"),
				StringValue: aws.String(errorReason),
			},
			"OriginalMessageId": {
				DataType:    aws.String("String"),
				StringValue: originalMessage.MessageId,
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to send message to DLQ: %w", err)
	}

	c.logger.Info(fmt.Sprintf("Successfully sent message to DLQ: %s", aws.ToString(originalMessage.MessageId)))
	return nil
}

func (c *SQSClient) GetQueueAttributes() error {
	result, err := c.client.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: &c.ConsumerConfig.QueueURL,
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
