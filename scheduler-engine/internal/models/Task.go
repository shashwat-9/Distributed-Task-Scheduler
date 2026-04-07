package models

import "time"

type Payload struct {
	Schedule       int      `json:"scheduled_at"`
	Cmds           []string `json:"cmds"`
	TaskId         int      `json:"task_id"`
	Image          string   `json:"image"`
	ScriptLocation []string `json:"script_location"`
	OutputLocation []string `json:"output_location"`
}

type Task struct {
	JobID     string    `json:"job_id"`
	Type      string    `json:"type"`
	Payload   Payload   `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}
