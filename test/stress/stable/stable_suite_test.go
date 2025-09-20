package stable

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	kind "sigs.k8s.io/kind/cmd/kind/app"
	kindcmd "sigs.k8s.io/kind/pkg/cmd"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	kubeInCluster     = envBool("BANYANDB_KUBE_IN_CLUSTER", false)
	kubeConfigPath    = envString("BANYANDB_KUBE_CONFIG", filepath.Join(os.TempDir(), "banyandb-stable-suite1.config"))
	deployKindCluster = envBool("BANYANDB_KIND_CREATE_CLUSTER", true)
	// kubeConfigPath       = clientcmd.RecommendedHomeFile.
	banyanDBNS                 = envString("BANYANDB_DEPLOY_NS", "banyandb")
	metadataDir                = envString("BANYANDB_TEST_METADATA_DIR", "testdata/metadata")
	queryDir                   = envString("BANYANDB_TEST_QUERY_DIR", "testdata/query")
	banyanDBLogs               = envString("BANYANDB_TEST_BANYANDB_LOGS_DIR", "logs")
	measureAccessLogPath       = envString("BANYANDB_TEST_MESURE_ACCESSLOG_DIR", "filter/target/measure")
	streamAccessLogPath        = envString("BANYANDB_TEST_STREAM_ACCESSLOG_FILE", "filter/target/stream/accesslog")
	banyanDBImageRepo          = envString("BANYANDB_DEPLOY_IMAGE_REPO", "ghcr.io/apache/skywalking-banyandb")
	banyanDBImageTag           = envString("BANYANDB_DEPLOY_IMAGE_TAG", "46083529398b73504e9ca929ef367cd1776aef82")
	deleteKindClusterAfterTest = envBool("BANYAHNDB_KIND_DELETE_AFTER_TEST", false)
	clientCount                = envInt("BANYANDB_TEST_CLIENT_COUNT", 1)         // total number of client count for sending the stream and measure data to the liaison node
	measureBulkSize            = envInt("BANYANDB_TEST_MEASURE_BULK_SUZE", 2000) // number of measures in each write request
	scaleClusterCount          = envInt("BANYANDB_TEST_CLUSTER_COUNT", 1)
	scaleServiceCount          = envInt("BANYANDB_TEST_SCALE_SERVICE_COUNT", 1)  // each service will scale to how many services count
	scaleInstanceCount         = envInt("BANYANDB_TEST_SCALE_INSTANCE_COUNT", 1) // each service will scale to how many instances count
	scaleEndpointCount         = envInt("BANYANDB_TEST_SCALE_ENDPOINT_COUNT", 1) // each instance will scale to how many endpoints count
	totalBenchmarkTime         = envDuration("BANYANDB_TEST_DURATION", time.Hour)
	queryParallelsCount        = envInt("BANYANDB_TEST_QUERY_PARALLELS_COUNT", 20) // total number of parallels for query execution
	queryTimes                 = envInt("BANYANDB_TEST_QUERY_TIMES", 50)           // each query will execute how many times
	queryStatsFilePath         = envString("BANYANDB_TEST_QUERY_STATS_FILE_PATH", filepath.Join(os.TempDir(), "query-stats.txt"))
	mode                       = envString("BANYANDB_TEST_MODE", "write")
	writeDataType              = envString("BANYANDB_TEST_WRITE_DATA_TYPE", "measure")
	zipkinScaleSize            = envInt("BANYANDB_TEST_ZIPKIN_SCALE_SIZE", 1)
	logScaleSize               = envInt("BANYANDB_TEST_LOG_SCALE_SIZE", 1)

	k8sClient              *kubernetes.Clientset
	k8sRestConfig          *rest.Config
	forwardPortStopChannel = make(chan struct{}, 1)
	logsCollector          *logCollector
	liaisonGRPCPort        int32
	allLiaisonPodName      []string
	allConnection          []*grpc.ClientConn
)

func TestStable(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Stable Suite", g.Label("integration", "slow"))
}

var _ = g.BeforeSuite(func() {
	fmt.Println("Starting Cluster Stable Integration Test Suite...")

	kindClusterFile, err := filepath.Abs("kind.yaml")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var config *rest.Config
	if kubeInCluster {
		config, err = rest.InClusterConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sRestConfig = config
	} else {
		if deployKindCluster {
			fmt.Println("Creating kind cluster, config:", kubeConfigPath)
			args := []string{
				"create", "cluster",
				"--name", "banyandb-stable-suite1",
				"--config", kindClusterFile,
				"--kubeconfig", kubeConfigPath,
			}
			err = kind.Run(kindcmd.NewLogger(), kindcmd.StandardIOStreams(), args)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err = os.Setenv("KUBECONFIG", kubeConfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		config, err = clientcmd.BuildConfigFromFlags("", kubeConfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kubeConfigYaml, err := os.ReadFile(kubeConfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sRestConfig, err = clientcmd.RESTConfigFromKubeConfig(kubeConfigYaml)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	k8sClient, err = kubernetes.NewForConfig(config)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

})

var _ = g.AfterSuite(func() {
	close(forwardPortStopChannel)
	if deleteKindClusterAfterTest {
		err := kind.Run(kindcmd.NewLogger(), kindcmd.StandardIOStreams(), []string{
			"delete", "cluster",
			"--name", "banyandb-stable-suite",
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	if logsCollector != nil {
		logsCollector.stop()
	}
	for _, c := range allConnection {
		_ = c.Close()
	}
})

var _ = g.Describe("Stable", func() {
	g.It("should pass", func() {
		// install the banyanDB to the cluster
		fmt.Println("Installing BanyanDB to the kind cluster")
		connections := installCluster()

		// using the first one for the schema initialization
		_, measureSchemas, err := InitializeAllMetadata(connections[0], metadataDir)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Starting the data generators
		scales := &scalerCounts{
			cluster:  scaleClusterCount,
			service:  scaleServiceCount,
			instance: scaleInstanceCount,
			endpoint: scaleEndpointCount,
		}
		measureGenerator, err := newMeasureDataGenerator(measureAccessLogPath, mode, scales, measureBulkSize*clientCount*5, measureSchemas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		queryExecutor := newMeasureQueryExecutor(queryParallelsCount, queryTimes, queryStatsFilePath, measureGenerator,
			newDashboardServiceQuery(filepath.Join(queryDir, "measure", "dashboardServices.yaml")),
			newDashboardTopologyQuery(filepath.Join(queryDir, "measure", "dashboardTopology.yaml")),
			newDashboardTopologyServiceQuery(filepath.Join(queryDir, "measure", "dashboardTopologyServices.yaml")),
			newDashboardTopologyEndpointQuery(filepath.Join(queryDir, "measure", "dashboardTopologyEndpoints.yaml")),
			newDashboardGatewayServiceQuery(filepath.Join(queryDir, "measure", "dashboardGatewayServices.yaml")),
			newOrgServiceQuery(filepath.Join(queryDir, "measure", "orgServices.yaml")),
			newOrgServiceDetailQuery(filepath.Join(queryDir, "measure", "orgServiceDetail.yaml")),
			newOrgServiceMetricsQuery(filepath.Join(queryDir, "measure", "orgServiceMetrics.yaml")),
			newWorkspacePerformanceQuery(filepath.Join(queryDir, "measure", "workspacePerformance.yaml")),
		)
		if mode == "read" {
			measureGenerator.setNormalWriteRoundStart(func() {
				// only execute the query when mode is read
				go queryExecutor.execute(measurev1.NewMeasureServiceClient(connections[0]))
			})
		}

		streamGenerator, err := newStreamDataGenerator(streamAccessLogPath, mode, scales, measureBulkSize*clientCount*5, streamSchemas,
			newZipkinSpanTransformer(zipkinScaleSize),
			newLogTransformer(logScaleSize))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// starting the batch write
		startBatch(connections, measureGenerator, streamGenerator, totalBenchmarkTime)
	})
})

func startBatch(
	connections []*grpc.ClientConn,
	measureGenerator *measureDataGenerator,
	streamGenerator *streamDataGenerator,
	duration time.Duration,
) {
	gomega.Expect(len(connections)).To(gomega.BeNumerically(">", 0))
	gomega.Expect(measureGenerator).NotTo(gomega.BeNil())
	gomega.Expect(duration).NotTo(gomega.BeZero())

	ctx, cancelFunc := context.WithTimeout(context.Background(), duration)
	defer cancelFunc()

	var measureClientDone sync.WaitGroup
	var streamClientDone sync.WaitGroup
	var allClientStarted sync.WaitGroup
	allClientStarted.Add(len(connections))
	statics := make([]*clientStatics, len(connections))
	for i := range connections {
		statics[i] = newClientStatics(i)
		if writeDataType == "measure" {
			measureClientDone.Add(1)
			go func(inx int, conn *grpc.ClientConn, s *clientStatics) {
				allClientStarted.Done()
				defer func() {
					g.GinkgoRecover()
					measureClientDone.Done()
				}()
				startMeasureWrite(ctx, inx, conn, measureGenerator, s)
			}(i, connections[i], statics[i])
		}

		if writeDataType == "stream" {
			measureClientDone.Add(1)
			go func(inx int, conn *grpc.ClientConn, s *clientStatics) {
				allClientStarted.Done()
				defer func() {
					g.GinkgoRecover()
					streamClientDone.Done()
				}()
				startStreamWrite(ctx, inx, conn, streamGenerator, s)
			}(i, connections[i], statics[i])
		}
	}

	allClientStarted.Wait()
	fmt.Println("all", writeDataType, "write clients started")
	if writeDataType == "measure" {
		measureGenerator.start()
	} else if writeDataType == "stream" {
		streamGenerator.start()
	}

	if mode == "write" {
		go func() {
			ticker := time.NewTicker(time.Second * 5)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					clientStats := ""
					for i, s := range statics {
						if i > 0 {
							clientStats += ", "
						}
						measureWrite, streamWrite := s.cleanAllCounter()
						clientStats += fmt.Sprintf("client %d wrote %d measures, %d streams", s.index, measureWrite, streamWrite)
					}
					fmt.Println("Benchmark write statistics in the last 5 seconds: ", time.Now().Format(time.RFC3339), clientStats)
				}
			}
		}()
	}

	measureClientDone.Wait()
	streamClientDone.Wait()
}

func startMeasureWrite(ctx context.Context, inx int, connection *grpc.ClientConn, generator *measureDataGenerator, s *clientStatics) {
	fmt.Println("Starting measure write goroutine:", inx)
	l := logger.GetLogger(fmt.Sprintf("measure-client-%d", inx))
	c := measurev1.NewMeasureServiceClient(connection)
	newConnectionClient := func() {
		_ = connection.Close()
		connection = popNewConnection()
		c = measurev1.NewMeasureServiceClient(connection)
	}
	var (
		mu                  sync.Mutex
		olderClientFinished <-chan struct{}
		client              grpc.BidiStreamingClient[measurev1.WriteRequest, measurev1.WriteResponse]
	)

	createClient := func() error {
		mu.Lock()
		defer mu.Unlock()
		if olderClientFinished != nil {
			<-olderClientFinished
		}
		var err error
		client, err = c.Write(ctx)
		if err != nil {
			return fmt.Errorf("failed to create write client: %w", err)
		}
		done := make(chan struct{})
		olderClientFinished = done
		go func(c grpc.BidiStreamingClient[measurev1.WriteRequest, measurev1.WriteResponse]) {
			defer func() {
				close(done)
			}()
			for {
				v, err := c.Recv()
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					l.Err(err).Msg("failed to receive response from measureService")
					return
				}
				if v.Status != "STATUS_SUCCEED" {
					l.Err(fmt.Errorf("measure service got an unexpected response: %s", v)).
						Msg("failed to receive response from measureService")
					return
				}
			}
		}(client)
		return nil
	}
	err := createClient()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	lock := sync.Mutex{}
	flush := func(newClient bool) error {
		lock.Lock()
		defer lock.Unlock()
		if client != nil {
			if errClose := client.CloseSend(); errClose != nil {
				return fmt.Errorf("failed to close send: %w", errClose)
			}
		}
		if !newClient {
			return nil
		}
		return createClient()
	}

	generator.notifyNormalWriteFinishRound(func() {
		if errFlush := flush(true); errFlush != nil {
			l.Err(errFlush).Msg("failed to force flush measure")
			waitFlushSuccess(newConnectionClient, flush)
		}
	})
	for {
		select {
		case <-ctx.Done():
			if errFlush := flush(false); errFlush != nil {
				gomega.Expect(errFlush).To(gomega.BeNil())
			}
			return
		default:
			take := generator.take()
			if errSend := client.Send(take); errSend != nil {
				l.Err(errSend).Msg("failed to send measure")
				waitFlushSuccess(newConnectionClient, flush)
				continue
			}
			if s.incMeasureWriteCount()%int64(measureBulkSize) == 0 {
				if errFlush := flush(true); errFlush != nil {
					l.Err(errFlush).Msg("failed to flush measure")
					waitFlushSuccess(newConnectionClient, flush)
					continue
				}
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func waitFlushSuccess(newConnectionClient func(), flush func(newClient bool) error) {
	for {
		time.Sleep(time.Second * 3)
		newConnectionClient()
		errFlush := flush(true)
		if errFlush == nil {
			return
		}
	}
}

func startStreamWrite(ctx context.Context, inx int, connection *grpc.ClientConn, generator *streamDataGenerator, s *clientStatics) {
	fmt.Println("Starting stream write goroutine:", inx)
	l := logger.GetLogger(fmt.Sprintf("stream-client-%d", inx))
	c := streamv1.NewStreamServiceClient(connection)
	var client grpc.BidiStreamingClient[streamv1.WriteRequest, streamv1.WriteResponse]
	createClient := func() error {
		var err error
		client, err = c.Write(ctx)
		if err != nil {
			return fmt.Errorf("failed to create write client: %w", err)
		}
		go func(c grpc.BidiStreamingClient[streamv1.WriteRequest, streamv1.WriteResponse]) {
			for {
				v, err := c.Recv()
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					l.Err(err).Msg("failed to receive response from streamService")
					return
				}
				if v.Status != "STATUS_SUCCEED" {
					l.Err(fmt.Errorf("stream service got an unexpected response: %s", v))
				}
			}
		}(client)
		return nil
	}
	err := createClient()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	flush := func(newClient bool) error {
		if errClose := client.CloseSend(); errClose != nil {
			return fmt.Errorf("failed to close send: %w", errClose)
		}
		if !newClient {
			return nil
		}
		return createClient()
	}
	for {
		select {
		case <-ctx.Done():
			if errFlush := flush(false); errFlush != nil {
				gomega.Expect(errFlush).To(gomega.BeNil())
			}
			return
		default:
			if errSend := client.Send(generator.take()); errSend != nil {
				gomega.Expect(errSend).To(gomega.BeNil())
			}
			if s.incStreamWriteCount()%int64(measureBulkSize) == 0 {
				if errFlush := flush(true); errFlush != nil {
					gomega.Expect(errFlush).To(gomega.BeNil())
				}
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func installCluster() []*grpc.ClientConn {
	fmt.Println("Installing Banyan and prometheus/grafana to the cluster")
	runningInstallClusterScript()

	// wait for all the liaison nodes to be ready
	fmt.Println("Waiting for all the liaison nodes to be ready")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	liaisonListOpt := metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=liaison",
	}
	list, err := k8sClient.CoreV1().Pods(banyanDBNS).List(ctx, liaisonListOpt)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	allLiaisonNodeReady := make(map[string]bool)
	for _, p := range list.Items {
		allLiaisonNodeReady[p.Name] = false
	}
	watcher, err := k8sClient.CoreV1().Pods(banyanDBNS).Watch(ctx, liaisonListOpt)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer watcher.Stop()

	waitSuccess := false
	for {
		select {
		case <-ctx.Done():
			break
		case event := <-watcher.ResultChan():
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					allLiaisonNodeReady[pod.Name] = true
					fmt.Printf("✅ Pod %s/%s is Ready\n", pod.Namespace, pod.Name)
				}
			}
			waitSuccess = checkAllPodReady(allLiaisonNodeReady)
		}
		if waitSuccess {
			break
		}
	}
	gomega.Expect(waitSuccess).To(gomega.BeTrue())

	// watch all the banyanDB pods logs
	abs, err := filepath.Abs(banyanDBLogs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	startNamespaceLogCollector(context.Background(), k8sClient, banyanDBNS, abs)

	// getting the service address
	fmt.Println("Getting the liaison gRPC port and forwarding to localhost")
	liaisonServices, err := k8sClient.CoreV1().Services(banyanDBNS).List(context.Background(), liaisonListOpt)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(len(liaisonServices.Items)).To(gomega.BeNumerically(">", 0))
	var grpcPort int32
	for _, s := range liaisonServices.Items {
		for _, p := range s.Spec.Ports {
			if p.Name == "grpc" {
				grpcPort = p.Port
				break
			}
		}
	}
	gomega.Expect(grpcPort).NotTo(gomega.BeZero())
	liaisonGRPCPort = grpcPort
	for podName := range allLiaisonNodeReady {
		allLiaisonPodName = append(allLiaisonPodName, podName)
	}

	result := make([]*grpc.ClientConn, 0, clientCount)
	targetWithCount := make(map[string]int)
	for _ = range clientCount {
		conn := popNewConnection()
		result = append(result, conn)
		targetWithCount[conn.Target()]++
	}
	fmt.Println("Total Initialized gRPC connections for parallel write:", len(result), "with target address and connection count:", targetWithCount)
	return result
}

func popNewConnection() *grpc.ClientConn {
	if kubeInCluster {
		conn, err := grpc.NewClient(fmt.Sprintf("%s.%s.svc.cluster.local:%d", "banyandb-grpc", banyanDBNS, liaisonGRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		allConnection = append(allConnection, conn)
		return conn
	}

	nextInx := len(allConnection) % len(allLiaisonPodName)
	podName := allLiaisonPodName[nextInx]
	roundTripper, upgrader, err := spdy.RoundTripperFor(k8sRestConfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", banyanDBNS, podName)
	hostIP := k8sRestConfig.Host
	serverURL := hostIP + path

	req, err := http.NewRequest(http.MethodPost, serverURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	readyChannel := make(chan struct{}, 1)
	forwardErrorChannel := make(chan error, 1)

	fmt.Println("Ready to port-forward liaison", podName, "grpc port", liaisonGRPCPort, "to localhost")
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, "POST", req.URL)
	forwarder, err := portforward.New(dialer, []string{fmt.Sprintf(":%d", liaisonGRPCPort)}, forwardPortStopChannel, readyChannel,
		bufio.NewWriter(&stdout), bufio.NewWriter(&stderr))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	go func() {
		defer g.GinkgoRecover()
		err = forwarder.ForwardPorts()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	var grpcAddress string
	select {
	case <-readyChannel:
		ports, err := forwarder.GetPorts()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(ports)).To(gomega.BeNumerically("==", 1))
		localPort := ports[0].Local
		fmt.Printf("Liaison Pod %s/%s grpc port %d is forwarded to localhost on port %d\n",
			banyanDBNS, podName, liaisonGRPCPort, localPort)
		grpcAddress = fmt.Sprintf("localhost:%d", localPort)
	case forwardErr := <-forwardErrorChannel:
		gomega.Expect(forwardErr).NotTo(gomega.HaveOccurred())
	}

	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	allConnection = append(allConnection, conn)
	return conn
}

func runningInstallClusterScript() {
	shellRunningBaseDir := filepath.Join(os.TempDir(), "banyandb-stable-suite")
	err := os.MkdirAll(shellRunningBaseDir, os.ModePerm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// running the install script
	scriptPath, err := filepath.Abs("cluster.sh")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	banyanDBValuesPath, err := filepath.Abs("helm-values-banyandb.yaml")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	prometheusValuesPath, err := filepath.Abs("helm-values-prometheus.yaml")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	grafanaValuesPath, err := filepath.Abs("helm-values-grafana.yaml")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	fmt.Println("Running the install cluster script", scriptPath)
	cmd := exec.Command(scriptPath, shellRunningBaseDir, banyanDBImageRepo, banyanDBImageTag,
		banyanDBValuesPath, banyanDBNS, prometheusValuesPath, grafanaValuesPath)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeConfigPath /*, "HTTP_PROXY=http://127.0.0.1:7890", "HTTPS_PROXY=http://127.0.0.1:7890"*/)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func checkAllPodReady(d map[string]bool) bool {
	for _, ready := range d {
		if !ready {
			return false
		}
	}
	return true
}

type clientStatics struct {
	measureWriteCount *int64
	streamWriteCount  *int64
	index             int
}

func newClientStatics(index int) *clientStatics {
	return &clientStatics{
		index:             index,
		measureWriteCount: new(int64),
		streamWriteCount:  new(int64),
	}
}

func (c *clientStatics) incMeasureWriteCount() int64 {
	return atomic.AddInt64(c.measureWriteCount, 1)
}

func (c *clientStatics) incStreamWriteCount() int64 {
	return atomic.AddInt64(c.streamWriteCount, 1)
}

func (c *clientStatics) cleanAllCounter() (measureWrite, streamWrite int64) {
	return atomic.SwapInt64(c.measureWriteCount, 0), atomic.SwapInt64(c.streamWriteCount, 0)
}

func envString(key, def string) (r string) {
	defer func() {
		fmt.Println("Environment variable", key, "=", r)
	}()
	env, b := os.LookupEnv(key)
	if !b {
		return def
	}
	return env
}

func envBool(key string, def bool) (r bool) {
	defer func() {
		fmt.Println("Environment variable", key, "=", r)
	}()
	env, b := os.LookupEnv(key)
	if !b {
		return def
	}
	lower := strings.ToLower(env)
	if lower == "true" || lower == "1" || lower == "yes" {
		return true
	}
	return false
}

func envInt(key string, def int) (r int) {
	defer func() {
		fmt.Println("Environment variable", key, "=", r)
	}()
	env, b := os.LookupEnv(key)
	if !b {
		return def
	}
	var err error
	r, err = strconv.Atoi(env)
	if err != nil {
		return def
	}
	return r
}

func envDuration(key string, def time.Duration) (r time.Duration) {
	defer func() {
		fmt.Println("Environment variable", key, "=", r)
	}()
	env, b := os.LookupEnv(key)
	if !b {
		return def
	}
	var err error
	r, err = time.ParseDuration(env)
	if err != nil {
		return def
	}
	return r
}
