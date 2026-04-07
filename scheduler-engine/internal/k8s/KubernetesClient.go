package k8s

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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

func (km *KubernetesManager) CreatePodObject(task models.Task) (*v1.Pod, error) {
	podName := fmt.Sprintf("task-%d", task.Payload.TaskId)
	namespace := "default" //will be changed later
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  podName,
					Image: task.Payload.Image,
					Command: []string{
						"/bin/sh",
						"-c",
						fmt.Sprintf("echo %s > /tmp/output.txt", task.Payload.OutputLocation),
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "output-volume",
							MountPath: "/tmp",
						},
					},
				},
			},
		},
	}

	return pod, nil

}

func (km *KubernetesManager) CreatePod(namespace string, pod *v1.Pod) (*v1.Pod, error) {
	createdPod, err := km.client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("error creating pod: %v", err)
	}
	return createdPod, nil
}

func (km *KubernetesManager) DeletePod(namespace, name string) error {
	err := km.client.CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("error deleting pod %s/%s: %v", namespace, name, err)
	}
	return nil
}
