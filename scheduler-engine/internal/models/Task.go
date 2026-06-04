package models

type Task struct {
	JobId         string            `json:"id"`
	Name          string            `json:"name"`
	Image         string            `json:"image"`
	Namespace     string            `json:"namespace"`
	InputLocation string            `json:"file_location"`
	Schedule      string            `json:"Schedule"`
	Retries       int32             `json:"retries"`
	Command       []string          `json:"command"`
	EnvVariables  map[string]string `json:"env"`
	Timeout       int64             `json:"timeout"`
	MinCompute    string            `json:"min_compute"`
	MinMemory     string            `json:"min_memory"`
	MaxMemory     string            `json:"max_memory"`
	MaxCompute    string            `json:"max_compute"`
	SecretRefs    []SecretReference `json:"secret_refs,omitempty"`
}

type SecretReference struct {
	SecretName string            `json:"secret_name"`
	EnvVars    map[string]string `json:"env_vars"` // key in secret -> env var name
}
