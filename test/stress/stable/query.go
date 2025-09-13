package stable

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/montanaflynn/stats"
	g "github.com/onsi/ginkgo/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
	rand2 "math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type measureQueryExecutor struct {
	queries   []measureQuery
	parallels int
	eachTimes int

	ctx              *executeContext
	measureGenerator *measureDataGenerator

	stats *queryStats
	lock  sync.Mutex
}

func newMeasureQueryExecutor(parallels, times int, statsFile string, measureGenerator *measureDataGenerator, queries ...measureQuery) *measureQueryExecutor {
	s, err := newQueryStats(statsFile)
	if err != nil {
		panic(err)
	}
	return &measureQueryExecutor{
		queries:          queries,
		parallels:        parallels,
		eachTimes:        times,
		measureGenerator: measureGenerator,
		stats:            s,
	}
}

type executeContext struct {
	latestServiceCount int
	services           []*serviceNameCombine
}

type serviceNameCombine struct {
	namespace string
	service   string
	names     []*serviceName
}

type measureQueryExecuteJob struct {
	idx    int
	req    *measurev1.QueryRequest
	entity string
}

func (m *measureQueryExecutor) execute(c measurev1.MeasureServiceClient) {
	m.lock.Lock()
	defer m.lock.Unlock()
	invoke := func(v []*measurev1.QueryRequest, e []string) []*measurev1.QueryResponse {
		result := make([]*measurev1.QueryResponse, len(v))
		queue := make(chan *measureQueryExecuteJob, len(v))
		wg := sync.WaitGroup{}
		for i := 0; i < m.parallels; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer g.GinkgoRecover()
				partDurations := make([]float64, 0)
				defer func() {
					m.stats.recordPartLatencies(partDurations)
				}()
				for j := range queue {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					t := time.Now()
					resp, err := c.Query(ctx, j.req)
					cancel()
					result[j.idx] = resp

					d := time.Now().Sub(t)
					partDurations = append(partDurations, float64(d.Nanoseconds()))
					m.stats.recordJob(j, resp, err)
				}
			}()
		}

		for i, req := range v {
			queue <- &measureQueryExecuteJob{idx: i, req: req, entity: e[i]}
		}

		close(queue)
		wg.Wait()
		return result
	}

	// build the execute context
	ctx := m.generateContext()

	start := time.Now().Add(-time.Minute * 15)
	end := time.Now()

	fmt.Println("Starting to execute queries...")
	m.stats.newQueryRound()
	now := time.Now()
	for i := range m.eachTimes {
		partNow := time.Now()
		for _, q := range m.queries {
			q.generate(ctx, start, end, invoke)
		}
		fmt.Printf("Queries executed(%d/%d), time cost: %s\n", i, m.eachTimes, time.Since(partNow).String())
	}

	fmt.Printf("Total queries executed, time cost: %s\n", time.Since(now).String())
	m.stats.writeQueryResult()
}

func (m *measureQueryExecutor) generateContext() *executeContext {
	serviceTraffics := m.measureGenerator.trafficRequests[EntityScopeTypeService]
	if m.ctx == nil || m.ctx.latestServiceCount != len(serviceTraffics) {
		ctx := &executeContext{
			services:           make([]*serviceNameCombine, 0),
			latestServiceCount: len(serviceTraffics),
		}
		tmp := make(map[string]*serviceNameCombine)
		for _, t := range serviceTraffics {
			name := parseServiceName(t.DataPoint.TagFamilies[0].Tags[0].Value.(*modelv1.TagValue_Str).Str.Value)
			key := fmt.Sprintf("%s/%s", name.namespace, name.service)
			combiner, ok := tmp[key]
			if !ok {
				combiner = &serviceNameCombine{
					namespace: name.namespace,
					service:   name.service,
				}
				tmp[key] = combiner
			}
			combiner.names = append(combiner.names, name)
		}
		for _, v := range tmp {
			ctx.services = append(ctx.services, v)
		}
		m.ctx = ctx
	}
	return m.ctx
}

func (m *executeContext) allService(filter func(name *serviceName) bool) []*serviceName {
	result := make([]*serviceName, 0)
	for _, s := range m.services {
		for _, name := range s.names {
			if filter(name) {
				continue
			}
			result = append(result, name)
		}
	}
	return result
}

func (m *executeContext) randomService(count int, filter func(name *serviceName) bool) []*serviceName {
	result := make([]*serviceName, 0, count)
	set := make(map[*serviceName]bool)
	for tmp := 0; tmp < count*10; tmp++ {
		randInx := rand2.Int() % len(m.services)
		s := m.services[randInx]
		var latestNotFiltered *serviceName
		for _, n := range s.names {
			if filter(n) {
				continue
			}
			latestNotFiltered = n
		}
		if latestNotFiltered == nil {
			continue
		}

		set[latestNotFiltered] = true
		if len(set) >= count {
			break
		}
	}
	for s := range set {
		result = append(result, s)
	}
	return result
}

func (m *executeContext) randomServiceWithGroup(count int, filter func(names *serviceNameCombine) bool) []*serviceNameCombine {
	result := make([]*serviceNameCombine, 0, count)
	set := make(map[*serviceNameCombine]bool)
	for {
		randInx := rand2.Int() % len(m.services)
		s := m.services[randInx]
		if filter != nil {
			if filter(s) {
				continue
			}
		}

		set[s] = true
		if len(set) >= count {
			break
		}
	}
	for s := range set {
		result = append(result, s)
	}
	return result
}

type measureQuery interface {
	generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest, []string) []*measurev1.QueryResponse)
}

func queryUnwrapFromAccessLog(d string) []*measurev1.QueryRequest {
	lines := strings.Split(d, "\n")
	result := make([]*measurev1.QueryRequest, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			break
		}
		entry := &metricsQueryLogEntry{}
		if err := json.Unmarshal([]byte(line), entry); err != nil {
			panic(err)
		}
		var requestJSON string
		if err := json.Unmarshal(entry.Request, &requestJSON); err != nil {
			panic(err)
		}
		req := &measurev1.QueryRequest{}
		if e := protojson.Unmarshal([]byte(requestJSON), req); e != nil {
			panic(e)
		}
		result = append(result, req)
	}
	return result
}

func queryGenerate[M any](start, end *timestamppb.Timestamp, original []*measurev1.QueryRequest, data []M, setting func(q *measurev1.QueryRequest, data M) string) ([]*measurev1.QueryRequest, []string) {
	requests := make([]*measurev1.QueryRequest, 0, len(original)*len(data))
	entities := make([]string, 0, len(original)*len(data))
	for _, r := range original {
		for _, d := range data {
			request := proto.Clone(r).(*measurev1.QueryRequest)
			if start != nil {
				request.TimeRange.Begin = start
			}
			if end != nil {
				request.TimeRange.End = end
			}
			entity := setting(request, d)
			requests = append(requests, request)
			entities = append(entities, entity)
		}
	}
	return requests, entities
}

type dashboardServicesQuery struct {
	instance []*measurev1.QueryRequest
	services []*measurev1.QueryRequest
	tags     []*measurev1.QueryRequest
}

func newDashboardServiceQuery(path string) measureQuery {
	data := readYamlAsMap(path)
	return &dashboardServicesQuery{
		services: queryUnwrapFromAccessLog(data["services"]),
		instance: queryUnwrapFromAccessLog(data["instance"]),
		tags:     queryUnwrapFromAccessLog(data["tags"]),
	}
}

func (d *dashboardServicesQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest, []string) []*measurev1.QueryResponse) {
	services := ctx.randomService(20, func(name *serviceName) bool {
		return name.highLevelService || name.hostnameService || name.unknownService || name.namespace == "eshop" || name.aggrService
	})
	// instance query
	invoke(queryGenerate(nil, timestamppb.New(end.Truncate(time.Minute)), d.instance, services, func(q *measurev1.QueryRequest, data *serviceName) string {
		le := queryCriteriaLe(q.Criteria)
		queryCriteriaCondition(le.Left).Value = queryCriterialMinuteTimeBucketValue(start)
		queryCriteriaCondition(queryCriteriaLe(le.Right).Left).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))
	// service query
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), d.services, services, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))
	// tags query
	invoke(queryGenerate(timestamppb.New(start.Truncate(24*time.Hour)), timestamppb.New(end.Truncate(24*time.Hour)),
		d.tags, []string{""}, func(q *measurev1.QueryRequest, data string) string {
			return ""
		}))
}

type dashboardGatewayServicesQuery struct {
	services []*measurev1.QueryRequest
}

func newDashboardGatewayServiceQuery(path string) measureQuery {
	data := readYamlAsMap(path)
	return &dashboardGatewayServicesQuery{
		services: queryUnwrapFromAccessLog(data["services"]),
	}
}

func (d *dashboardGatewayServicesQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest, []string) []*measurev1.QueryResponse) {
	services := ctx.randomService(20, func(name *serviceName) bool {
		return !name.hostnameService || name.aggrService
	})

	// service query
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), d.services, services, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))
}

type orgServicesQuery struct {
	services []*measurev1.QueryRequest
	apdex    []*measurev1.QueryRequest
}

func newOrgServiceQuery(path string) measureQuery {
	data := readYamlAsMap(path)
	return &orgServicesQuery{
		services: queryUnwrapFromAccessLog(data["services"]),
		apdex:    queryUnwrapFromAccessLog(data["apdex"]),
	}
}

func (o *orgServicesQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest, []string) []*measurev1.QueryResponse) {
	services := ctx.randomService(20, func(name *serviceName) bool {
		return name.highLevelService || name.hostnameService || name.unknownService || name.namespace == "eshop" ||
			name.subset == serviceNameAny || name.cluster == serviceNameAny || name.aggrService
	})

	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.services, services, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))

	// reduce subset, cluster, env
	highLevelServices := make([]*serviceName, 0, len(services))
	for _, s := range services {
		highLevelServices = append(highLevelServices, s.toService(serviceNameAny, serviceNameAny, s.service, serviceNameAny))
	}
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.services, highLevelServices, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))
}

type orgServiceDetailQuery struct {
	nsWithNameServices []*measurev1.QueryRequest
	otherServices      []*measurev1.QueryRequest
}

func newOrgServiceDetailQuery(path string) measureQuery {
	data := readYamlAsMap(path)
	return &orgServiceDetailQuery{
		nsWithNameServices: queryUnwrapFromAccessLog(data["nsWithNameServices"]),
		otherServices:      queryUnwrapFromAccessLog(data["otherServices"]),
	}
}

func (o *orgServiceDetailQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest, []string) []*measurev1.QueryResponse) {
	// service must not contain host name
	services := ctx.randomServiceWithGroup(1, func(names *serviceNameCombine) bool {
		var highLevelCount int
		var nsWithNameServiceCount int
		for _, n := range names.names {
			if n.hostnameService || n.unknownService {
				return true
			}
			if n.highLevelService {
				highLevelCount++
			}
			if n.subset == serviceNameAny && n.env == serviceNameAny && n.cluster == serviceNameAny {
				nsWithNameServiceCount++
			}
		}
		if highLevelCount == len(names.names) || nsWithNameServiceCount == 0 {
			return true
		}
		return false
	})

	var nsWithNameService *serviceName
	var otherServices []*serviceName

	for _, n := range services[0].names {
		if n.highLevelService {
			continue
		}
		if n.subset == serviceNameAny && n.env == serviceNameAny && n.cluster == serviceNameAny {
			nsWithNameService = n
		} else {
			otherServices = append(otherServices, n)
		}
	}

	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.nsWithNameServices, []*serviceName{nsWithNameService}, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.otherServices, otherServices, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))
}

type orgServiceMetricsQuery struct {
	service         []*measurev1.QueryRequest
	instanceList    []*measurev1.QueryRequest
	instanceMetrics []*measurev1.QueryRequest
}

func newOrgServiceMetricsQuery(path string) measureQuery {
	data := readYamlAsMap(path)
	return &orgServiceMetricsQuery{
		service:         queryUnwrapFromAccessLog(data["service"]),
		instanceList:    queryUnwrapFromAccessLog(data["instanceList"]),
		instanceMetrics: queryUnwrapFromAccessLog(data["instance"]),
	}
}

type instanceInfo struct {
	service  *serviceName
	instance string
	id       string
}

func (o *orgServiceMetricsQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest, []string) []*measurev1.QueryResponse) {
	// query service
	services := ctx.randomService(1, func(name *serviceName) bool {
		return name.highLevelService || name.hostnameService || name.unknownService || name.namespace == "eshop" ||
			name.aggrService
	})

	serviceIdList := make([]*serviceName, 0, 2)
	serviceIdList = append(serviceIdList, services[0])
	serviceIdList = append(serviceIdList, services[0].toService(services[0].subset, serviceNameAny, services[0].service, serviceNameAny))

	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.service, serviceIdList, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))

	// instance list
	responses := invoke(queryGenerate(nil, timestamppb.New(end.Truncate(time.Minute)), o.instanceList, []*serviceName{services[0]}, func(q *measurev1.QueryRequest, data *serviceName) string {
		le := queryCriteriaLe(q.Criteria)
		queryCriteriaCondition(le.Left).Value = queryCriterialMinuteTimeBucketValue(end)
		queryCriteriaCondition(queryCriteriaLe(le.Right).Left).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))

	// query instance metrics
	instanceIds := make([]*instanceInfo, 0)
	for _, dp := range responses[0].DataPoints {
		for _, tf := range dp.TagFamilies {
			if tf.Name == "storage-only" {
				for _, tag := range tf.Tags {
					if tag.Key == "instance_traffic_name" {
						instanceName := tag.Value.Value.(*modelv1.TagValue_Str).Str.Value
						instanceID := services[0].serviceID() + "_" + base64.StdEncoding.EncodeToString([]byte(instanceName))
						instanceIds = append(instanceIds, &instanceInfo{
							service:  services[0],
							instance: instanceName,
							id:       instanceID,
						})
						break
					}
				}
			}
		}
	}
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.instanceMetrics, instanceIds, func(q *measurev1.QueryRequest, data *instanceInfo) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.id)
		return fmt.Sprintf("%s/%s", data.service.original, data.instance)
	}))
}

type workspacePerformanceQuery struct {
	topn    []*measurev1.QueryRequest
	service []*measurev1.QueryRequest
}

func newWorkspacePerformanceQuery(path string) measureQuery {
	data := readYamlAsMap(path)
	return &workspacePerformanceQuery{
		topn:    queryUnwrapFromAccessLog(data["topn"]),
		service: queryUnwrapFromAccessLog(data["service"]),
	}
}

func (w *workspacePerformanceQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest, []string) []*measurev1.QueryResponse) {
	allServices := ctx.allService(func(name *serviceName) bool {
		return name.highLevelService || name.hostnameService || name.unknownService || name.cluster == serviceNameAny ||
			name.subset != serviceNameAny || name.env == serviceNameAny || !strings.HasPrefix(name.namespace, "dev-")
	})

	type serviceNameWithoutService struct {
		env       string
		cluster   string
		namespace string
		subset    string
	}

	withoutServiceCache := make(map[serviceNameWithoutService]bool)
	for _, s := range allServices {
		withoutServiceCache[serviceNameWithoutService{
			env:       s.env,
			cluster:   s.cluster,
			namespace: s.namespace,
			subset:    s.subset,
		}] = true
	}
	topNArray := make([]*serviceNameWithoutService, 0, len(withoutServiceCache))
	for k := range withoutServiceCache {
		topNArray = append(topNArray, &k)
	}

	// topn query
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), w.topn, topNArray, func(q *measurev1.QueryRequest, data *serviceNameWithoutService) string {
		le := queryCriteriaLe(q.Criteria)
		queryCriteriaCondition(le.Left).Value = queryCriterialStringValue(data.env)

		le = queryCriteriaLe(le.Right)
		queryCriteriaCondition(le.Left).Value = queryCriterialStringValue(data.cluster)

		le = queryCriteriaLe(le.Right)
		queryCriteriaCondition(le.Left).Value = queryCriterialStringValue(data.namespace)

		le = queryCriteriaLe(le.Right)
		queryCriteriaCondition(le.Left).Value = queryCriterialStringValue(data.subset)

		le = queryCriteriaLe(le.Right)
		queryCriteriaCondition(le.Left).Value = queryCriterialStringValue("service")
		return fmt.Sprintf("%s|%s|%s|%s", data.subset, data.namespace, data.cluster, data.env)
	}))

	// service query
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), w.service, allServices, func(q *measurev1.QueryRequest, data *serviceName) string {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
		return data.original
	}))
}

func readYamlAsMap(path string) map[string]string {
	file, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	result := make(map[string]string)
	if err = yaml.Unmarshal(file, result); err != nil {
		panic(err)
	}
	return result
}

type metricsQueryLogEntry struct {
	StartTime time.Time       `json:"start_time"`
	Request   json.RawMessage `json:"request"`
	Service   string          `json:"service"`
	Error     string          `json:"error,omitempty"`
	Duration  time.Duration   `json:"duration_ms"`
}

func queryCriteriaLe(q *modelv1.Criteria) *modelv1.LogicalExpression {
	return q.Exp.(*modelv1.Criteria_Le).Le
}

func queryCriteriaCondition(q *modelv1.Criteria) *modelv1.Condition {
	return q.Exp.(*modelv1.Criteria_Condition).Condition
}

func queryCriterialIntValue(val int64) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Int{
			Int: &modelv1.Int{
				Value: val,
			},
		},
	}
}

func queryCriterialStringValue(val string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{
				Value: val,
			},
		},
	}
}

func queryCriterialMinuteTimeBucketValue(tb time.Time) *modelv1.TagValue {
	format := tb.Format("200601021504")
	i, err := strconv.ParseInt(format, 10, 64)
	if err != nil {
		panic(err)
	}
	return queryCriterialIntValue(i)
}

type queryStats struct {
	path string
	f    *os.File

	totalCount    *int64
	emptyReqCount *int64
	errReqCount   *int64

	totalDurations []float64
	errorStats     map[string]*int64
	emptyRespStats map[string]map[string]*int64
	totalCounts    map[string]*int64

	lock sync.Mutex
}

func newQueryStats(path string) (*queryStats, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	q := &queryStats{
		path: path,
		f:    f,
	}
	q.newQueryRound()
	return q, nil
}

func (q *queryStats) newQueryRound() {
	q.totalCount = new(int64)
	q.emptyReqCount = new(int64)
	q.errReqCount = new(int64)

	q.totalDurations = make([]float64, 0)
	q.errorStats = make(map[string]*int64)
	q.emptyRespStats = make(map[string]map[string]*int64)
	q.totalCounts = make(map[string]*int64)
}

func (q *queryStats) recordPartLatencies(v []float64) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.totalDurations = append(q.totalDurations, v...)
}

func (q *queryStats) recordJob(j *measureQueryExecuteJob, response *measurev1.QueryResponse, err error) {
	atomic.AddInt64(q.totalCount, 1)
	q.lock.Lock()
	defer q.lock.Unlock()
	if err != nil {
		atomic.AddInt64(q.errReqCount, 1)
		d, exist := q.errorStats[err.Error()]
		if !exist {
			d = new(int64)
			q.errorStats[err.Error()] = d
		}
		atomic.AddInt64(d, 1)
	} else if len(response.DataPoints) == 0 {
		atomic.AddInt64(q.emptyReqCount, 1)
		d, exist := q.emptyRespStats[j.req.Name]
		if !exist {
			d = make(map[string]*int64)
			q.emptyRespStats[j.req.Name] = d
		}
		v, exist := d[j.entity]
		if !exist {
			v = new(int64)
			d[j.entity] = v
		}
		atomic.AddInt64(v, 1)
	}
	counts, exist := q.totalCounts[j.req.Name]
	if !exist {
		counts = new(int64)
		q.totalCounts[j.req.Name] = counts
	}
	atomic.AddInt64(counts, 1)
}

func (q *queryStats) writeQueryResult() {
	minVal, _ := stats.Min(q.totalDurations)
	maxVal, _ := stats.Max(q.totalDurations)
	mean, _ := stats.Mean(q.totalDurations)
	median, _ := stats.Median(q.totalDurations)
	p90, _ := stats.Percentile(q.totalDurations, 90)
	p95, _ := stats.Percentile(q.totalDurations, 95)
	p98, _ := stats.Percentile(q.totalDurations, 98)
	p99, _ := stats.Percentile(q.totalDurations, 99)

	details := ""
	if len(q.errorStats) > 0 {
		details += fmt.Sprintf("Errors: ")
		for e, c := range q.errorStats {
			details += fmt.Sprintf("\n\t%s: %d", e, c)
		}
	}
	if len(q.emptyRespStats) > 0 {
		if len(details) > 0 {
			details += "\n"
		}
		details += fmt.Sprintf("Empties: ")
		for e, i := range q.emptyRespStats {
			var counts int64
			if totalCounts, exist := q.totalCounts[e]; exist {
				counts = atomic.LoadInt64(totalCounts)
			}
			details += fmt.Sprintf("\n\t%s: %d", e, counts)
			for entity, c := range i {
				details += fmt.Sprintf("\n\t\t%s: %d", entity, atomic.LoadInt64(c))
			}
		}
	}
	if len(details) > 0 {
		details = "\n" + details
	}
	stats := fmt.Sprintf(
		`Query statics: %s
empty,error,total count: %d/%d/%d
min,max,mean,median,p90,p95,p98,p99 duration(nanosecond): %f, %f, %f, %f, %f, %f, %f, %f%s`, time.Now().Format("2006-01-02 15:04:05"),
		atomic.LoadInt64(q.emptyReqCount), atomic.LoadInt64(q.errReqCount), atomic.LoadInt64(q.totalCount),
		NanoToMillis(minVal), NanoToMillis(maxVal), NanoToMillis(mean), NanoToMillis(median), NanoToMillis(p90),
		NanoToMillis(p95), NanoToMillis(p98), NanoToMillis(p99), details)
	fmt.Println(stats)
	_, _ = q.f.Write([]byte(stats))
	_ = q.f.Sync()
}

func NanoToMillis(nano float64) float64 {
	return float64(nano) / 1e6
}
