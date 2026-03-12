package models

import (
	"encoding/json"
	"time"
)

type Job struct {
	Image       string
	Cmd         []string
	Name        string
	ScheduledAt time.Time
}

func UnmarshalTask(messageValue []byte) (Job, error) {
	var task Job
	err := json.Unmarshal(messageValue, &task)
	if err != nil {
		return Job{}, err
	}

	return task, nil
}
