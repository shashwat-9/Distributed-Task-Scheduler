package models

type Task struct {
	JobId         string              `json:"id"`
	Name          string              `json:"name"`
	Image         string              `json:"image"`
	Namespace     string              `json:"namespace"`
	InputLocation string              `json:"file_location"`
	Schedule      string              `json:"Schedule"`
	Compute       string              `json:"compute"`
	Memory        string              `json:"memory"`
	Retries       string              `json:"retries"`
	Command       []string            `json:"command"`
	EnvVariables  []map[string]string `json:"env"`
	Timeout       string              `json:"timeout"`
}
