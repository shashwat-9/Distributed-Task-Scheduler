package Processor

import (
	"container/list"
	"scheduler-engine/internal/k8s"
	"scheduler-engine/internal/models"
	"sync"
	"time"
)

type ScheduledJobProcessor struct {
	jobStore          *list.List
	mutex             sync.Mutex
	live              bool
	WatchList         *list.List
	kubernetesManager *k8s.KubernetesManager
}

func NewScheduledJobProcessor() JobProcessor {
	kubeManager, err := k8s.GetKubernetesManager()
	if err != nil {
		panic(err)
	}
	return &ScheduledJobProcessor{
		jobStore:          list.New(),
		WatchList:         list.New(),
		mutex:             sync.Mutex{},
		live:              true,
		kubernetesManager: kubeManager,
	}
}

func (jobProcessor ScheduledJobProcessor) Process(messageValue []byte) error {
	task, err := models.UnmarshalTask(messageValue)
	if err != nil {
		return err
	}

	jobProcessor.mutex.Lock()
	defer jobProcessor.mutex.Unlock()

	jobProcessor.jobStore.PushBack(task)

	return nil
}

func (jobProcessor ScheduledJobProcessor) ExecuteTask() error {
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

			_, err := jobProcessor.kubernetesManager.CreatePod("", nil)
			if err != nil {
				return err
			}
			//add the job to the watch list, for the watcher goroutine to pick up.
		}
	}

	return nil
}

func (jobProcessor ScheduledJobProcessor) StopProcessor() {
	jobProcessor.live = false
}
