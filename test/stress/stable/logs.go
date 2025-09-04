package stable

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type streamKey struct {
	Pod       string
	Container string
}

type logCollector struct {
	cs        *kubernetes.Clientset
	namespace string
	outDir    string

	mu      sync.Mutex
	cancel  context.CancelFunc
	streams map[streamKey]context.CancelFunc
}

func startNamespaceLogCollector(ctx context.Context, cs *kubernetes.Clientset, namespace, outDir string) (*logCollector, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	cctx, cancel := context.WithCancel(ctx)

	c := &logCollector{
		cs:        cs,
		namespace: namespace,
		outDir:    outDir,
		cancel:    cancel,
		streams:   make(map[streamKey]context.CancelFunc),
	}

	f := informers.NewSharedInformerFactoryWithOptions(cs, 0, informers.WithNamespace(namespace))
	pi := f.Core().V1().Pods().Informer()

	pi.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.ensureStreams(cctx, obj.(*corev1.Pod)) },
		UpdateFunc: func(_, newObj interface{}) { c.ensureStreams(cctx, newObj.(*corev1.Pod)) },
		DeleteFunc: func(obj interface{}) { c.stopStreamsForPod(obj.(*corev1.Pod).Name) },
	})

	go f.Start(cctx.Done())
	if !cache.WaitForCacheSync(cctx.Done(), pi.HasSynced) {
		cancel()
		return nil, fmt.Errorf("pod informer not synced")
	}

	podList, err := cs.CoreV1().Pods(namespace).List(cctx, metav1.ListOptions{})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("list pods: %w", err)
	}
	for i := range podList.Items {
		c.ensureStreams(cctx, &podList.Items[i])
	}

	return c, nil
}

func (c *logCollector) stop() { c.cancel(); c.stopAll() }

func (c *logCollector) ensureStreams(ctx context.Context, pod *corev1.Pod) {
	containers := make([]string, 0, len(pod.Spec.Containers))
	for _, cc := range pod.Spec.Containers {
		containers = append(containers, cc.Name)
	}
	for _, name := range containers {
		key := streamKey{Pod: pod.Name, Container: name}

		if c.hasStream(key) {
			continue
		}
		if !isContainerRunnable(pod, name) {
			continue
		}
		c.startStream(ctx, pod.Name, name)
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		c.stopStreamsForPod(pod.Name)
	}
}

func isContainerRunnable(pod *corev1.Pod, name string) bool {
	for _, st := range pod.Status.ContainerStatuses {
		if st.Name == name {
			return st.Ready || st.State.Running != nil || st.RestartCount > 0 || st.State.Terminated != nil
		}
	}
	return true
}

func (c *logCollector) hasStream(k streamKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.streams[k]
	return ok
}

func (c *logCollector) startStream(parent context.Context, podName, container string) {
	k := streamKey{Pod: podName, Container: container}

	c.mu.Lock()
	if _, exist := c.streams[k]; exist {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	c.streams[k] = cancel
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			delete(c.streams, k)
			c.mu.Unlock()
		}()

		fp := filepath.Join(c.outDir, fmt.Sprintf("%s_%s.log", podName, container))
		f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Printf("[log-collector] open %s error: %v\n", fp, err)
			return
		}
		defer f.Close()

		_ = c.dumpOnce(ctx, f, podName, container, true)

		backoff := time.Second
		for {
			if err := c.follow(ctx, f, podName, container); err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Printf("[log-collector] follow %s/%s error: %v (retry in %s)\n", podName, container, err, backoff)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				}
				if backoff < 10*time.Second {
					backoff *= 2
				}
				continue
			}
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *logCollector) dumpOnce(ctx context.Context, w io.Writer, podName, container string, previous bool) error {
	req := c.cs.CoreV1().Pods(c.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  container,
		Timestamps: true,
		Previous:   previous,
	})
	rc, err := req.Stream(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	fmt.Fprintf(w, "\n===== [%s] %s/%s previous=%v =====\n", time.Now().Format(time.RFC3339), podName, container, previous)
	_, err = io.Copy(w, bufio.NewReader(rc))
	return err
}

func (c *logCollector) follow(ctx context.Context, w io.Writer, podName, container string) error {
	req := c.cs.CoreV1().Pods(c.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  container,
		Timestamps: true,
		Follow:     true,
	})
	rc, err := req.Stream(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	fmt.Fprintf(w, "\n===== [%s] %s/%s follow begin =====\n", time.Now().Format(time.RFC3339), podName, container)
	_, err = io.Copy(w, bufio.NewReader(rc))
	fmt.Fprintf(w, "\n===== [%s] %s/%s follow end (err=%v) =====\n", time.Now().Format(time.RFC3339), podName, container, err)
	return err
}

func (c *logCollector) stopStreamsForPod(pod string) {
	c.mu.Lock()
	var cancels []context.CancelFunc
	for k, cancel := range c.streams {
		if k.Pod == pod {
			cancels = append(cancels, cancel)
			delete(c.streams, k)
		}
	}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (c *logCollector) stopAll() {
	c.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(c.streams))
	for _, cancel := range c.streams {
		cancels = append(cancels, cancel)
	}
	c.streams = map[streamKey]context.CancelFunc{}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
