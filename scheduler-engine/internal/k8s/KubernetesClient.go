package k8s

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/utils/pointer"
	"os"
	"path/filepath"
	"scheduler-engine/internal/constants"
	"scheduler-engine/internal/models"
	"scheduler-engine/internal/util"
	"strings"
)

var kubernetesManager KubernetesManager

type KubernetesManager struct {
	client kubernetes.Interface
	logger *zap.Logger
}

func GetKubernetesManager() (*KubernetesManager, error) {
	if kubernetesManager.client == nil {
		client, err := createKubernetesClient()
		if err != nil {
			return nil, err
		}
		kubernetesManager.client = client
		kubernetesManager.logger = util.GetLogger()
		kubernetesManager.logger.Info("Kubernetes Manager Initialized")
	}
	return &kubernetesManager, nil
}

func createKubernetesClient() (kubernetes.Interface, error) {
	var config *rest.Config
	var err error

	config, err = rest.InClusterConfig()
	if err != nil {
		var kubeconfig string

		if home := homedir.HomeDir(); home != "" {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}

		// Allow override via environment variable
		if kubeconfigPath := os.Getenv("KUBECONFIG"); kubeconfigPath != "" {
			kubeconfig = kubeconfigPath
		}

		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return client, nil

}

func (km *KubernetesManager) CreateJob(task models.Task) error {
	name := fmt.Sprintf(task.Name+"-%s", task.JobId)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: task.Namespace,
			Labels: map[string]string{
				"app":          name,
				"execution_id": task.JobId,
			},
			Annotations: map[string]string{
				"description": "",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          pointer.Int32(task.Retries),
			ActiveDeadlineSeconds: pointer.Int64(getTimeout(task.Timeout)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":          name,
						"execution_id": task.JobId,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:      name,
							Image:     task.Image,
							Command:   []string{"bash", "-c"},
							Args:      createArgs(task.Command, task.Schedule, task.InputLocation),
							Env:       mapToEnvVars(task),
							Resources: createResourceRequirements(task.MinCompute, task.MinMemory, task.MaxCompute, task.MaxMemory),
						},
					},
				},
			},
		},
	}

	result, err := km.client.BatchV1().Jobs(task.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		kubernetesManager.logger.Error("Error creating job : " + err.Error())
		return err
	}
	kubernetesManager.logger.Info("Job created successfully : " + result.String())

	return nil
}

func mapToEnvVars(task models.Task) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	for key, value := range task.EnvVariables {
		envVars = append(envVars, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	for _, secretRef := range task.SecretRefs {
		for secretKey, envVarName := range secretRef.EnvVars {
			envVars = append(envVars, corev1.EnvVar{
				Name: envVarName,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: secretRef.SecretName,
						},
						Key: secretKey,
					},
				},
			})
		}
	}

	return envVars
}

func (km *KubernetesManager) JobExistsByLabel(key string, value string, namespace string) (bool, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", key, value),
		Limit:         1,
	}

	jobs, err := km.client.BatchV1().Jobs(namespace).List(context.TODO(), listOptions)
	if err != nil {
		return false, err
	}

	if jobs != nil && len(jobs.Items) > 0 {
		return true, nil
	}

	return false, nil
}

func createArgs(cmds []string, schedule string, inputLocation string) []string {
	var args []string
	args = append(args, "./execute.sh")
	for _, cmd := range cmds {
		args = append(args, "-c", "'"+cmd+"'")
	}
	args = append(args, "-s", "'"+schedule+"'")
	args = append(args, "-f", "'"+inputLocation+"'")

	cmd := strings.Join(args, " ")
	return []string{cmd}
}

func getTimeout(timeout int64) int64 {
	if timeout == 0 {
		return constants.DefaultTimeout
	}
	return timeout
}

func createResourceRequirements(cpuRequest, memRequest, cpuLimit, memLimit string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuRequest),
			corev1.ResourceMemory: resource.MustParse(memRequest),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuLimit),
			corev1.ResourceMemory: resource.MustParse(memLimit),
		},
	}
}
