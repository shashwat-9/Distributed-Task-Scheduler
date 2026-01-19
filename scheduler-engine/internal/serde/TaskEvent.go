package serde

type TaskEvent struct {
}

func MarshalTask(task TaskEvent) []byte {
	return nil
}

func UnMarshalTask(data []byte) TaskEvent {
	return TaskEvent{}
}
