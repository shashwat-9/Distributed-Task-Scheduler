package k8s

import (
	"context"
	"go.uber.org/zap"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

func (km *KubernetesManager) CreateJob(task models.Task) {

	image := task.Image
	name := task.Name
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default", // change to target namespace
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: pointer.Int32(3), // retry up to 3 times
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    name,
							Image:   image,
							Command: []string{"bash", "-c"},
							Args:    []string{"./execute.sh -c 'pip install pandas openpyxl' -c 'python3 excel_generator.py' -s '1778583996' -f 's3://amzn-dts-input/SampleScript.py'"},
						},
					},
				},
			},
		},
	}

	result, _ := km.client.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
	logger.Info("Job created successfully : " + result.String())

}
