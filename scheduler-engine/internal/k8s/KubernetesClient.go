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
	"scheduler-engine/internal/models"
	"scheduler-engine/internal/util"
)

var kubernetesManager KubernetesManager
var logger *zap.Logger

type KubernetesManager struct {
	client kubernetes.Interface
}

func GetKubernetesManager() (*KubernetesManager, error) {
	if kubernetesManager.client == nil {
		client, err := createKubernetesClient()
		if err != nil {
			return nil, err
		}
		kubernetesManager.client = client
		logger = util.GetLogger("logs/k8s.log", 10, 5, 28)
		logger.Info("Kubernetes Manager Initialized")
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
			ActiveDeadlineSeconds: pointer.Int64(task.Timeout),
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
							Args:      createCmds(task.Command),
							Env:       mapToEnvVars(task.EnvVariables),
							Resources: createResourceRequirements(task.MinCompute, task.MinMemory, task.MaxCompute, task.MaxMemory),
						},
					},
				},
			},
		},
	}

	result, err := km.client.BatchV1().Jobs(task.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		logger.Error("Error creating job : " + err.Error())
		return err
	}
	logger.Info("Job created successfully : " + result.String())

	return nil
}

func mapToEnvVars(envMap map[string]string) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	for key, value := range envMap {
		envVars = append(envVars, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	return envVars
}

func (km *KubernetesManager) JobExistsByLabel(originalName string, namespace string) (bool, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: fmt.Sprintf("original-name=%s", originalName),
		Limit:         1,
	}

	jobs, err := km.client.BatchV1().Jobs(namespace).List(context.TODO(), listOptions)
	if err != nil {
		return false, err
	}

	for _, job := range jobs.Items {
		if job.Status.Active > 0 {
			return true, nil
		}
		if job.Status.Succeeded > 0 {
			return true, nil
		}
		if job.Status.Failed > 0 {
			return true, nil
		}
	}

	return false, nil
}

func createCmds(cmds []string) (cmdsArray []string) {
	//for _, cmd := range cmds {
	//	cmdsArray = append(cmdsArray, cmd)
	//}
	//return cmdsArray

	return []string{"./execute.sh -c 'pip install pandas openpyxl' -c 'python3 excel_generator.py' -s '1778583996' -f 's3://amzn-dts-input/SampleScript.py'"}
}

func getTimeout(timeout int) int {
	//if timeout == 0 {
	//	return nil
	//}
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
