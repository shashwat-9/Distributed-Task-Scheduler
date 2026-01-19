package executor

type TaskEventExecutor interface {
	Execute()
}

type TaskExecutorImpl struct {
}

func (t TaskExecutorImpl) Execute() {
}
