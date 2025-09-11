package stable

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	g "github.com/onsi/ginkgo/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
	rand2 "math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type measureQueryExecutor struct {
	queries   []measureQuery
	parallels int
	eachTimes int

	ctx              *executeContext
	measureGenerator *measureDataGenerator
}

func newMeasureQueryExecutor(parallels, times int, measureGenerator *measureDataGenerator, queries ...measureQuery) *measureQueryExecutor {
	return &measureQueryExecutor{
		queries:          queries,
		parallels:        parallels,
		eachTimes:        times,
		measureGenerator: measureGenerator,
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
	idx int
	req *measurev1.QueryRequest
}

func (m *measureQueryExecutor) executeOnce(c measurev1.MeasureServiceClient) {
	invoke := func(v []*measurev1.QueryRequest) []*measurev1.QueryResponse {
		result := make([]*measurev1.QueryResponse, len(v))
		queue := make(chan *measureQueryExecuteJob, len(v))
		wg := sync.WaitGroup{}
		for i := 0; i < m.parallels; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer g.GinkgoRecover()
				for j := range queue {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					resp, err := c.Query(ctx, j.req)
					cancel()
					if err != nil {
						panic(err)
					}

					result[j.idx] = resp
				}
			}()
		}

		for i, req := range v {
			queue <- &measureQueryExecuteJob{idx: i, req: req}
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
	now := time.Now()
	for i := range m.eachTimes {
		partNow := time.Now()
		for _, q := range m.queries {
			q.generate(ctx, start, end, invoke)
		}
		fmt.Printf("Queries executed(%d/%d), time cost: %s\n", i, m.eachTimes, time.Since(partNow).String())
	}

	fmt.Printf("Total queries executed, time cost: %s\n", time.Since(now).String())
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
	generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest) []*measurev1.QueryResponse)
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

func queryGenerate[M any](start, end *timestamppb.Timestamp, original []*measurev1.QueryRequest, data []M, setting func(q *measurev1.QueryRequest, data M)) []*measurev1.QueryRequest {
	result := make([]*measurev1.QueryRequest, 0, len(original)*len(data))
	for _, r := range original {
		for _, d := range data {
			request := proto.Clone(r).(*measurev1.QueryRequest)
			if start != nil {
				request.TimeRange.Begin = start
			}
			if end != nil {
				request.TimeRange.End = end
			}
			setting(request, d)
			result = append(result, request)
		}
	}
	return result
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

func (d *dashboardServicesQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest) []*measurev1.QueryResponse) {
	services := ctx.randomService(20, func(name *serviceName) bool {
		return name.highLevelService || name.hostnameService || name.unknownService || name.namespace == "eshop"
	})
	// instance query
	invoke(queryGenerate(nil, timestamppb.New(end.Truncate(time.Minute)), d.instance, services, func(q *measurev1.QueryRequest, data *serviceName) {
		le := queryCriteriaLe(q.Criteria)
		queryCriteriaCondition(le.Left).Value = queryCriterialMinuteTimeBucketValue(end)
		queryCriteriaCondition(queryCriteriaLe(le.Right).Left).Value = queryCriterialStringValue(data.serviceID())
	}))
	// service query
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), d.services, services, func(q *measurev1.QueryRequest, data *serviceName) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
	}))
	// tags query
	invoke(queryGenerate(timestamppb.New(start.Truncate(24*time.Hour)), timestamppb.New(end.Truncate(24*time.Hour)),
		d.tags, []string{""}, func(q *measurev1.QueryRequest, data string) {
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

func (d *dashboardGatewayServicesQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest) []*measurev1.QueryResponse) {
	services := ctx.randomService(20, func(name *serviceName) bool {
		return !name.hostnameService
	})

	// service query
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), d.services, services, func(q *measurev1.QueryRequest, data *serviceName) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
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

func (o *orgServicesQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest) []*measurev1.QueryResponse) {
	services := ctx.randomService(20, func(name *serviceName) bool {
		return name.highLevelService || name.hostnameService || name.unknownService || name.namespace == "eshop" ||
			name.subset == serviceNameAny || name.cluster == serviceNameAny
	})

	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.services, services, func(q *measurev1.QueryRequest, data *serviceName) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
	}))

	// reduce subset, cluster, env
	highLevelServices := make([]string, 0, len(services))
	for _, s := range services {
		highLevelService := s.toService(serviceNameAny, serviceNameAny, s.service, serviceNameAny)
		highLevelServices = append(highLevelServices, serviceNameToID(highLevelService))
	}
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.services, highLevelServices, func(q *measurev1.QueryRequest, data string) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data)
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

func (o *orgServiceDetailQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest) []*measurev1.QueryResponse) {
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

	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.nsWithNameServices, []*serviceName{nsWithNameService}, func(q *measurev1.QueryRequest, data *serviceName) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
	}))
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.otherServices, otherServices, func(q *measurev1.QueryRequest, data *serviceName) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data.serviceID())
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

func (o *orgServiceMetricsQuery) generate(ctx *executeContext, start, end time.Time, invoke func([]*measurev1.QueryRequest) []*measurev1.QueryResponse) {
	// query service
	services := ctx.randomService(1, func(name *serviceName) bool {
		return name.highLevelService || name.hostnameService || name.unknownService || name.namespace == "eshop" ||
			name.subset == serviceNameAny || name.cluster == serviceNameAny
	})

	serviceIdList := make([]string, 0, 2)
	serviceIdList = append(serviceIdList, services[0].serviceID())
	serviceIdList = append(serviceIdList, serviceNameToID(services[0].toService(services[0].subset, serviceNameAny, services[0].service, serviceNameAny)))

	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.service, serviceIdList, func(q *measurev1.QueryRequest, data string) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data)
	}))

	// instance list
	responses := invoke(queryGenerate(nil, timestamppb.New(end.Truncate(time.Minute)), o.instanceList, []string{services[0].serviceID()}, func(q *measurev1.QueryRequest, data string) {
		le := queryCriteriaLe(q.Criteria)
		queryCriteriaCondition(le.Left).Value = queryCriterialMinuteTimeBucketValue(end)
		queryCriteriaCondition(queryCriteriaLe(le.Right).Left).Value = queryCriterialStringValue(services[0].serviceID())
	}))

	// query instance metrics
	instanceIds := make([]string, 0)
	for _, dp := range responses[0].DataPoints {
		for _, tf := range dp.TagFamilies {
			if tf.Name == "storage-only" {
				for _, tag := range tf.Tags {
					if tag.Key == "instance_traffic_name" {
						instanceName := tag.Value.Value.(*modelv1.TagValue_Str).Str.Value
						instanceID := services[0].serviceID() + "_" + base64.StdEncoding.EncodeToString([]byte(instanceName))
						instanceIds = append(instanceIds, instanceID)
						break
					}
				}
			}
		}
	}
	invoke(queryGenerate(timestamppb.New(start.Truncate(time.Minute)), timestamppb.New(end.Truncate(time.Minute)), o.instanceMetrics, instanceIds, func(q *measurev1.QueryRequest, data string) {
		queryCriteriaCondition(q.Criteria).Value = queryCriterialStringValue(data)
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
