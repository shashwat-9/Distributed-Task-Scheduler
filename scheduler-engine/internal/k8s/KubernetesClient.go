package k8s

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
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
		logger = getLogger()
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

func (km *KubernetesManager) ListPods(namespace string) (*v1.PodList, error) {
	pods, err := km.client.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error listing pods: %v", err)
	}
	return pods, nil
}

func (km *KubernetesManager) GetPod(namespace, name string) (*v1.Pod, error) {
	pod, err := km.client.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting pod %s/%s: %v", namespace, name, err)
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

func getLogger() *zap.Logger {
	lumberjackLogger := &lumberjack.Logger{
		Filename:   "logs/k8s.log",
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     28,
		Compress:   false,
	}

	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "time"
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder

	return zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		zapcore.AddSync(lumberjackLogger),
		zap.InfoLevel), zap.AddCaller())

}
