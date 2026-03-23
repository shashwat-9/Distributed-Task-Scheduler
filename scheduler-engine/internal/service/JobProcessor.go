package service

type JobProcessor interface {
	Process(messageValue []byte) error
	ExecuteTask() error
	StopProcessor()
}
