package service

import (
	"container/list"
	"scheduler-engine/internal/models"
	"sync"
	"time"
)

type JobProcessor struct {
	jobStore  *list.List
	mutex     sync.Mutex
	live      bool
	WatchList *list.List
}

func (jobProcessor JobProcessor) AddTask(messageValue []byte) error {
	task, err := models.UnmarshalTask(messageValue)
	if err != nil {
		return err
	}

	jobProcessor.mutex.Lock()
	defer jobProcessor.mutex.Unlock()

	jobProcessor.jobStore.PushBack(task)

	return nil
}

func (jobProcessor JobProcessor) ExecuteScheduledTask() {
	for jobProcessor.live {
		frontElement := jobProcessor.jobStore.Front()
		if frontElement != nil {
			job := frontElement.Value.(*models.Job)

			if time.Now().Before(job.ScheduledAt) {
				time.Sleep(time.Until(job.ScheduledAt))
			}

			//check if rebalancing is triggered.

			jobProcessor.mutex.Lock()
			defer jobProcessor.mutex.Unlock()

			jobProcessor.jobStore.Remove(frontElement)

			//call kubernetes API and submit the job
			//add the job to the watch list, for the watcher goroutine to pick up.
		}
	}
}

func (jobProcessor JobProcessor) StopProcessor() {
	jobProcessor.live = false
}
