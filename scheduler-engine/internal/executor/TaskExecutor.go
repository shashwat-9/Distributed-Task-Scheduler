package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Task func()

type DynamicWorkerPool struct {
	// Configuration
	minWorkers    int
	maxWorkers    int
	queueCapacity int

	// State
	taskQueue chan Task
	quit      chan bool
	wg        sync.WaitGroup

	// Worker tracking
	activeWorkers int64
	workerMutex   sync.Mutex

	// Scaling parameters
	scaleUpThreshold   float64
	scaleDownThreshold float64

	// Monitoring
	scalingTicker *time.Ticker
	scalingDone   chan bool
	scaleDown     chan bool
}

func NewDynamicWorkerPool(minWorkers, maxWorkers, queueCapacity int, scaleUpThreshold, scaleDownThreshold float64) *DynamicWorkerPool {
	if minWorkers <= 0 || maxWorkers < minWorkers || queueCapacity <= 0 {
		panic("Invalid worker pool configuration")
	}

	return &DynamicWorkerPool{
		minWorkers:         minWorkers,
		maxWorkers:         maxWorkers,
		queueCapacity:      queueCapacity,
		taskQueue:          make(chan Task, queueCapacity),
		quit:               make(chan bool),
		activeWorkers:      0,
		scaleUpThreshold:   scaleUpThreshold,
		scaleDownThreshold: scaleDownThreshold,
		scalingTicker:      time.NewTicker(2 * time.Second),
		scalingDone:        make(chan bool),
		scaleDown:          make(chan bool),
	}
}

func (dwp *DynamicWorkerPool) Start() {
	// Start minimum workers
	for i := 0; i < dwp.minWorkers; i++ {
		dwp.startWorker()
		fmt.Printf("Started worker %d (Total active: %d)\n", (i + 1), dwp.activeWorkers)
	}

	fmt.Printf("Started dynamic worker pool with %d workers (min: %d, max: %d)\n",
		dwp.minWorkers, dwp.minWorkers, dwp.maxWorkers)

	go dwp.scalingMonitor()

}

func (dwp *DynamicWorkerPool) startWorker() {
	workerID := atomic.AddInt64(&dwp.activeWorkers, 1)
	dwp.wg.Add(1)

	go dwp.worker(int(workerID))
}

func (dwp *DynamicWorkerPool) worker(id int) {
	defer dwp.wg.Done()
	defer atomic.AddInt64(&dwp.activeWorkers, -1)
	defer fmt.Printf("Worker %d stopped (Active workers: %d)\n", id, atomic.LoadInt64(&dwp.activeWorkers)-1)

	for {
		select {
		case task, ok := <-dwp.taskQueue:
			if !ok {
				return
			}
			fmt.Printf("Worker %d executing task\n", id)
			task()
		case <-dwp.quit:
			return
		case <-dwp.scaleDown:
			if atomic.LoadInt64(&dwp.activeWorkers) > int64(dwp.minWorkers) {
				fmt.Printf("Worker %d exiting due to scale-down\n", id)
				return
			}
		}
	}
}

func (dwp *DynamicWorkerPool) scalingMonitor() {
	defer dwp.scalingTicker.Stop()

	for {
		select {
		case <-dwp.scalingTicker.C:
			dwp.checkAndScale()
		case <-dwp.scalingDone:
			return
		}
	}
}

func (dwp *DynamicWorkerPool) checkAndScale() {
	queueSize := len(dwp.taskQueue)
	queueCapacity := cap(dwp.taskQueue)
	currentWorkers := atomic.LoadInt64(&dwp.activeWorkers)

	utilization := float64(queueSize) / float64(queueCapacity)

	fmt.Printf("Queue Status: %d/%d (%.1f%% full), Active Workers: %d\n",
		queueSize, queueCapacity, utilization*100, currentWorkers)

	for utilization >= dwp.scaleUpThreshold && currentWorkers < int64(dwp.maxWorkers) {
		fmt.Printf("Scaling UP: Queue utilization %.1f%% >= %.1f%% threshold\n",
			utilization*100, dwp.scaleUpThreshold*100)
		dwp.startWorker()
	}

	for utilization <= dwp.scaleDownThreshold && currentWorkers > int64(dwp.minWorkers) {
		fmt.Printf("Scaling Down: Queue utilization %.1f%% >= %.1f%% threshold\n",
			utilization*100, dwp.scaleDownThreshold*100)
		dwp.scaleDown <- true
	}
}

func (dwp *DynamicWorkerPool) SubmitBlocking(task Task) {
	dwp.taskQueue <- task
}

func (dwp *DynamicWorkerPool) GetStats() map[string]interface{} {
	queueSize := len(dwp.taskQueue)
	queueCapacity := cap(dwp.taskQueue)
	activeWorkers := atomic.LoadInt64(&dwp.activeWorkers)

	return map[string]interface{}{
		"queue_size":        queueSize,
		"queue_capacity":    queueCapacity,
		"queue_utilization": float64(queueSize) / float64(queueCapacity),
		"active_workers":    activeWorkers,
		"min_workers":       dwp.minWorkers,
		"max_workers":       dwp.maxWorkers,
		"available_space":   queueCapacity - queueSize,
	}
}

func (dwp *DynamicWorkerPool) Shutdown() {
	fmt.Println("Initiating shutdown...")

	// Stop scaling monitor
	close(dwp.scalingDone)

	// Close task queue to prevent new submissions
	close(dwp.taskQueue)

	// Signal all workers to quit
	close(dwp.quit)

	// Wait for all workers to finish
	dwp.wg.Wait()

	fmt.Println("All workers stopped")
}

func (dwp *DynamicWorkerPool) GracefulShutdown(timeout time.Duration) {
	fmt.Println("Initiating graceful shutdown...")

	close(dwp.scalingDone)

	done := make(chan bool)
	go func() {
		// Wait for queue to drain
		for len(dwp.taskQueue) > 0 {
			fmt.Printf("Draining queue... %d tasks remaining\n", len(dwp.taskQueue))
			time.Sleep(100 * time.Millisecond)
		}

		// Now close the task queue and signal workers
		close(dwp.taskQueue)
		close(dwp.quit)
		dwp.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		fmt.Println("Graceful shutdown completed")
	case <-time.After(timeout):
		fmt.Println("Graceful shutdown timed out, forcing shutdown")
		close(dwp.taskQueue)
		close(dwp.quit)
		dwp.wg.Wait()
	}
}
