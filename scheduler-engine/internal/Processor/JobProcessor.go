package Processor

type JobProcessor interface {
	Process(messageValue []byte) error
	ExecuteTask() error
	StopProcessor()
	Close()
}
