package stable

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

type requestWrapper[Request proto.Message] struct {
	request Request
	minute  time.Time
}

type baseGenerator[Request proto.Message] struct {
	context             context.Context
	requestChannel      chan requestWrapper[Request]
	currentReader       *bufio.Reader
	downstreamInfo      *downstreamTimeInfo
	scaler              *dataScaler[Request]
	requestGenerator    func() Request
	getSchema           func(Request) schema[Request]
	getDataFromChannels func(dataChannel chan requestWrapper[Request]) (*requestWrapper[Request], bool)
	postGenerate        func(scaler *dataScaler[Request], serviceNames []string, subEntityName string, request Request, downstream *downstreamTimeInfo) bool
	normalWriteFinish   func()
	currentFile         *os.File
	cancel              context.CancelFunc
	scales              *scalerCounts
	downstreamLock      sync.RWMutex
	mode                string
}

type generatorCallback interface {
	afterInitDataRound(downstream *downstreamTimeInfo)
}

func newBaseGenerator[Request proto.Message](
	path string,
	mode string,
	scales *scalerCounts,
	requestChannelLen int,
	generate func() Request,
	getSchema func(Request) schema[Request],
	getDataFromChannels func(dataChannel chan requestWrapper[Request]) (*requestWrapper[Request], bool),
	postGenerate func(scaler *dataScaler[Request], serviceNames []string, subEntityName string, request Request, downstream *downstreamTimeInfo) bool,
) (*baseGenerator[Request], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &baseGenerator[Request]{
		scales:              scales,
		mode:                mode,
		currentFile:         file,
		currentReader:       bufio.NewReader(file),
		requestChannel:      make(chan requestWrapper[Request], requestChannelLen),
		requestGenerator:    generate,
		getSchema:           getSchema,
		getDataFromChannels: getDataFromChannels,
		postGenerate:        postGenerate,
	}, nil
}

func (b *baseGenerator[Request]) start(owner generatorCallback) {
	b.context, b.cancel = context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-b.context.Done():
				return
			default:
				b.generateData(owner)
			}
		}
	}()
}

//func (b *baseGenerator[Request]) generateData() {
//	for !b.changeDownstreamTime() {
//		// wait until the next minute
//		now := time.Now()
//		next := now.Truncate(time.Minute).Add(time.Minute)
//		time.Sleep(next.Sub(now))
//	}
//	// otherwise, reload the data from the beginning
//	b.generateDataFromFileStart(nil)
//}

func (b *baseGenerator[Request]) generateData(owner generatorCallback) {
	downstream := newCurrentMinuteDownstream()
	start := time.Now()

	////fast write next 1h data
	//for i := range 60 * 24 * 3 {
	//for i := range 60 {
	for i := range 1 {
		fmt.Println("starting fast write next 1 minute data, index: ", i)
		b.generateDataFromFileStart(downstream)
		downstream.increaseOneMinute()
	}

	// wait the client write finished
	for {
		if len(b.requestChannel) == 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	duration := time.Now().Sub(start)
	fmt.Println("fast write next 2d data finished, duration:", duration)

	var shouldSkipWaitNextMinute bool
	for {
		// wait until the next minute
		if shouldSkipWaitNextMinute {
			fmt.Println("because write data duration is longer than 1 minute, so skip wait next minute")
		} else {
			now := time.Now()
			next := now.Truncate(time.Minute).Add(time.Minute)
			fmt.Println("wait until the next minute:", next, "sleep duration:", next.Sub(now))
			time.Sleep(next.Sub(now))
		}

		// write the data of current minute twice
		start = time.Now()
		if owner != nil {
			owner.afterInitDataRound(downstream)
		}
		if b.mode == "read" {
			fmt.Println("read mode, just skip the data write in the normal round")
			continue
		}
		b.generateDataFromFileStart(downstream)
		for {
			if len(b.requestChannel) == 0 {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		duration = time.Now().Sub(start)
		shouldSkipWaitNextMinute = duration > time.Minute
		fmt.Println("write current minute data finished, duration:", duration)
		if b.normalWriteFinish != nil {
			b.normalWriteFinish()
		}

		downstream.increaseOneMinute()
	}
}

func (b *baseGenerator[Request]) generateDataFromFileStart(info *downstreamTimeInfo) {
	_, err := b.currentFile.Seek(0, io.SeekStart)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	b.currentReader.Reset(b.currentFile)
	b.generate(info)
}

func (b *baseGenerator[Request]) take() Request {
	for {
		req := b.getFromChannel()
		if req != nil {
			return *req
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (b *baseGenerator[Request]) notifyNormalWriteFinishRound(f func()) {
	b.normalWriteFinish = f
}

func (b *baseGenerator[Request]) getFromChannel() *Request {
	// take the data from queue first
	for {
		wrapper, force := b.getDataFromChannels(b.requestChannel)
		if force {
			return &wrapper.request
		}

		if wrapper != nil {
			// if there is data in the queue, then check the time minute
			// verifies the time minute is right or not
			//if wrapper.minute == time.Now().Truncate(time.Minute) {
			return &wrapper.request
			//}
			// if not correct, then ignore it and keep query from channel until empty or take correct data
			//continue
		} else {
			break
		}
	}

	return nil
}

func (b *baseGenerator[Request]) changeDownstreamTime() bool {
	b.downstreamLock.Lock()
	defer b.downstreamLock.Unlock()

	// generate the downstream info if not exist
	if b.downstreamInfo == nil {
		b.downstreamInfo = newCurrentMinuteDownstream()
		return true
	}

	// otherwise, check if the minute is changed
	if b.downstreamInfo.minute != time.Now().Truncate(time.Minute) {
		b.downstreamInfo = newCurrentMinuteDownstream()
		return true
	}
	return false
}

func (b *baseGenerator[Request]) downstreamCorrect() (*downstreamTimeInfo, bool) {
	b.downstreamLock.RLock()
	defer b.downstreamLock.RUnlock()

	if b.downstreamInfo == nil {
		return nil, false
	}
	return b.downstreamInfo, b.downstreamInfo.minute == time.Now().Truncate(time.Minute)
}

func (b *baseGenerator[Request]) generate(appoint *downstreamTimeInfo) {
	for {
		var downstream *downstreamTimeInfo
		if appoint != nil {
			downstream = appoint
		} else {
			info, correct := b.downstreamCorrect()
			if !correct {
				return
			}
			downstream = info
		}
		line, err := b.currentReader.ReadString('\n')
		// reading the file error
		if err != nil && err.Error() != "EOF" {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if err != nil && err.Error() == "EOF" {
			fmt.Printf("--- current data(%s) read to the end ---\n", b.currentFile.Name())
			break
		}

		req := b.requestGenerator()
		errUnmarshal := protojson.Unmarshal([]byte(line), req)
		gomega.Expect(errUnmarshal).NotTo(gomega.HaveOccurred())

		b.scaler = newDataScaler[Request](b.getSchema(req), req, b.scales)
		b.scaler.generate(downstream, b, func(d Request) {
			b.requestChannel <- requestWrapper[Request]{request: d, minute: downstream.minute}
		})
	}
}

type downstreamTimeInfo struct {
	minute time.Time
	hour   time.Time
	day    time.Time

	now time.Time

	minuteTimeBucket int64
}

func newCurrentMinuteDownstream() *downstreamTimeInfo {
	now := time.Now()
	minuteTimeBucket := now.Format("200601021504")
	timeBucketVal, err := strconv.ParseInt(minuteTimeBucket, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("failed to parse time bucket: %v", minuteTimeBucket))
	}
	return &downstreamTimeInfo{
		minute:           now.Truncate(time.Minute),
		hour:             now.Truncate(time.Hour),
		day:              now.Truncate(24 * time.Hour),
		now:              now,
		minuteTimeBucket: timeBucketVal,
	}
}

func (d *downstreamTimeInfo) increaseOneMinute() {
	d.now = d.now.Add(time.Minute)

	d.minute = d.now.Truncate(time.Minute)
	d.hour = d.now.Truncate(time.Hour)
	d.day = d.now.Truncate(24 * time.Hour)
	format := d.now.Format("200601021504")
	timeBucketVal, err := strconv.ParseInt(format, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("failed to parse time bucket: %v", format))
	}
	d.minuteTimeBucket = timeBucketVal
}

type scalerCounts struct {
	cluster  int
	service  int
	instance int
	endpoint int
}

type dataScaler[T proto.Message] struct {
	schema             schema[T]
	measureReq         T
	subEntityName      string
	relatedFieldValues []*EntityFieldValue
	subEntity          *EntityFieldValue
	scales             *scalerCounts
	baseServiceNames   []*serviceName
}

func newDataScaler[T proto.Message](schema schema[T], request T, scales *scalerCounts) *dataScaler[T] {
	baseServiceNames, err := schema.GetServiceName(request)
	if err != nil {
		panic(err)
	}
	serviceNames := make([]*serviceName, 0, len(baseServiceNames))
	for _, name := range baseServiceNames {
		serviceNames = append(serviceNames, parseServiceName(name))
	}
	subEntity, all := schema.GetRelatedFieldValues(request)
	var subEntityName string
	if subEntity != nil {
		subEntityName = subEntity.Value
	}
	return &dataScaler[T]{
		schema:             schema,
		measureReq:         request,
		scales:             scales,
		subEntity:          subEntity,
		baseServiceNames:   serviceNames,
		relatedFieldValues: all,
		subEntityName:      subEntityName,
	}
}

func (d *dataScaler[T]) getSubEntityName() string {
	if d.subEntityName != "" {
		return d.subEntityName
	}

	schemaName := d.schema.GetName()
	var subEntityName string
	if d.schema.getScope() == EntityScopeTypeServiceInstance {
		for _, v := range d.relatedFieldValues {
			if v.field.scope == EntityScopeTypeServiceInstance {
				name, err := base64.StdEncoding.DecodeString(v.Value)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to decode service instance name: %s, mesure name: %s", v.Value, schemaName))
				subEntityName = string(name)
			}
		}
	} else if d.schema.getScope() == EntityScopeTypeEndpoint {
		for _, v := range d.relatedFieldValues {
			if v.field.scope == EntityScopeTypeEndpoint {
				name, err := base64.StdEncoding.DecodeString(v.Value)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to decode service endpoint name: %s, mesure name: %s", v.Value, schemaName))
				subEntityName = string(name)
			}
		}
	}
	gomega.Expect(subEntityName).ToNot(gomega.BeNil(), fmt.Sprintf("%s: subEntityName is empty", d.schema.GetName()))
	return subEntityName
}

func (d *dataScaler[T]) generate(downstream *downstreamTimeInfo, generator *baseGenerator[T], add func(T)) {
	var totalCount = d.scales.service
	if d.schema.getScope() == EntityScopeTypeServiceInstance {
		totalCount *= d.scales.instance
	} else if d.schema.getScope() == EntityScopeTypeEndpoint {
		totalCount *= d.scales.endpoint
	}
	addOrIgnore := func(data T, serviceNames []string, subEntity string) {
		if ignore := generator.postGenerate(d, serviceNames, subEntity, data, downstream); ignore {
			return
		}
		add(data)
	}
	if d.schema.GetType() == schemaTypeMeasure && len(d.baseServiceNames) == 0 {
		panic(fmt.Sprintf("schema cannot found any service entity: %s", d.schema.GetName()))
	}

	serviceNamesList := d.schema.getBaseSchema().generateServiceName(d.baseServiceNames, d.scales)
	for _, serviceNames := range serviceNamesList {
		switch d.schema.getScope() {
		case EntityScopeTypeServiceInstance:
			for instanceSeq := 0; instanceSeq < d.scales.instance; instanceSeq++ {
				newReq := proto.Clone(d.measureReq).(T)
				instanceName := d.schema.getBaseSchema().generateInstanceName(serviceNames[0], d.subEntityName, instanceSeq)
				for _, fv := range d.relatedFieldValues {
					d.schema.ApplyFieldChange(newReq, serviceNames, instanceName, fv)
				}
				addOrIgnore(newReq, serviceNames, instanceName)
			}
		case EntityScopeTypeEndpoint:
			for endpointSeq := 0; endpointSeq < d.scales.endpoint; endpointSeq++ {
				newReq := proto.Clone(d.measureReq).(T)
				endpointName := d.schema.getBaseSchema().generateEndpointName(serviceNames[0], d.subEntityName, endpointSeq)
				for _, fv := range d.relatedFieldValues {
					d.schema.ApplyFieldChange(newReq, serviceNames, endpointName, fv)
				}
				addOrIgnore(newReq, serviceNames, endpointName)
			}
		case EntityScopeTypeService:
			newReq := proto.Clone(d.measureReq).(T)
			for _, fv := range d.relatedFieldValues {
				d.schema.ApplyFieldChange(newReq, serviceNames, "", fv)
			}
			addOrIgnore(newReq, serviceNames, "")
		case entityScopeTypeServiceRelation:
			newReq := proto.Clone(d.measureReq).(T)
			for _, fv := range d.relatedFieldValues {
				d.schema.ApplyFieldChange(newReq, serviceNames, "", fv)
			}
			addOrIgnore(newReq, serviceNames, "")
		default:
			panic(fmt.Errorf("unsupported entity scope: %d", d.schema.getScope()))
		}
	}
}

type measureDataGenerator struct {
	*baseGenerator[*measurev1.WriteRequest]
	trafficChannel         chan *measurev1.WriteRequest
	trafficRegister        map[string]bool
	trafficRequests        map[EntityScopeType][]*measurev1.WriteRequest
	schemas                map[string]*MeasureSchema
	trafficCounterService  int64
	trafficCounterInstance int64
	trafficCounterEndpoint int64
	trafficRegisterLock    sync.RWMutex
	normalWriteRoundStart  func()
	serviceInstanceCache   map[string]map[string]bool
	serviceEndpointCache   map[string]map[string]bool

	mode string

	hourGenerator *baseGenerator[*measurev1.WriteRequest]
	dayGenerator  *baseGenerator[*measurev1.WriteRequest]
}

func newMeasureDataGenerator(dir string, mode string, scales *scalerCounts, requestChannelSize int, schemas map[string]*MeasureSchema) (*measureDataGenerator, error) {
	var err error
	measureGenerator := &measureDataGenerator{
		trafficChannel:       make(chan *measurev1.WriteRequest, requestChannelSize),
		schemas:              schemas,
		trafficRegister:      make(map[string]bool),
		trafficRequests:      make(map[EntityScopeType][]*measurev1.WriteRequest),
		serviceInstanceCache: make(map[string]map[string]bool),
		serviceEndpointCache: make(map[string]map[string]bool),
		mode:                 mode,
	}
	go func() {
		for {
			time.Sleep(30 * time.Second)
			fmt.Printf("traffic counter: service %d, instance %d, endpoint %d\n",
				measureGenerator.trafficCounterService,
				measureGenerator.trafficCounterInstance,
				measureGenerator.trafficCounterEndpoint)
		}
	}()
	measureGenerator.hourGenerator, err = newBaseGenerator[*measurev1.WriteRequest](filepath.Join(dir, "hour"), mode, scales, requestChannelSize, func() *measurev1.WriteRequest {
		return &measurev1.WriteRequest{}
	}, func(request *measurev1.WriteRequest) schema[*measurev1.WriteRequest] {
		return schemas[request.Metadata.Name]
	}, func(dataChannel chan requestWrapper[*measurev1.WriteRequest]) (*requestWrapper[*measurev1.WriteRequest], bool) {
		return nil, false
	}, func(scaler *dataScaler[*measurev1.WriteRequest], serviceNames []string, _ string, request *measurev1.WriteRequest, downstream *downstreamTimeInfo) bool {
		request.DataPoint.Timestamp = timestamppb.New(measureGenerator.adjustTimeFromTime(request.DataPoint.Timestamp.AsTime(), downstream))
		request.Metadata.ModRevision = 0
		return false
	})
	if err != nil {
		return nil, err
	}
	measureGenerator.dayGenerator, err = newBaseGenerator[*measurev1.WriteRequest](filepath.Join(dir, "day"), mode, scales, requestChannelSize, func() *measurev1.WriteRequest {
		return &measurev1.WriteRequest{}
	}, func(request *measurev1.WriteRequest) schema[*measurev1.WriteRequest] {
		return schemas[request.Metadata.Name]
	}, func(dataChannel chan requestWrapper[*measurev1.WriteRequest]) (*requestWrapper[*measurev1.WriteRequest], bool) {
		return nil, false
	}, func(scaler *dataScaler[*measurev1.WriteRequest], serviceNames []string, _ string, request *measurev1.WriteRequest, downstream *downstreamTimeInfo) bool {
		request.DataPoint.Timestamp = timestamppb.New(measureGenerator.adjustTimeFromTime(request.DataPoint.Timestamp.AsTime(), downstream))
		request.Metadata.ModRevision = 0
		return false
	})
	if err != nil {
		return nil, err
	}

	measureGenerator.baseGenerator, err = newBaseGenerator[*measurev1.WriteRequest](filepath.Join(dir, "minute"), mode, scales, requestChannelSize, func() *measurev1.WriteRequest {
		return &measurev1.WriteRequest{}
	}, func(request *measurev1.WriteRequest) schema[*measurev1.WriteRequest] {
		return schemas[request.Metadata.Name]
	}, func(dataChannel chan requestWrapper[*measurev1.WriteRequest]) (*requestWrapper[*measurev1.WriteRequest], bool) {
		select {
		case hourData := <-measureGenerator.hourGenerator.requestChannel:
			return &hourData, true
		case dayData := <-measureGenerator.dayGenerator.requestChannel:
			return &dayData, true
		case req := <-measureGenerator.trafficChannel:
			return &requestWrapper[*measurev1.WriteRequest]{request: req}, true
		case req := <-dataChannel:
			return &req, false
		default:
		}
		return nil, false
	}, func(scaler *dataScaler[*measurev1.WriteRequest], serviceNames []string, subEntityName string, request *measurev1.WriteRequest, downstream *downstreamTimeInfo) bool {
		for _, serviceName := range serviceNames {
			measureGenerator.registerServiceTraffic(serviceName)
		}
		if scaler.schema.getScope() == EntityScopeTypeServiceInstance {
			measureGenerator.registerServiceInstanceTraffic(serviceNames[0], subEntityName)
		} else if scaler.schema.getScope() == EntityScopeTypeEndpoint {
			measureGenerator.registerServiceEndpointTraffic(serviceNames[0], subEntityName)
		}
		measureName := scaler.schema.GetName()
		// if the access log is related to the traffic, then just ignore it, because the register will send them
		if measureName == "service_traffic_minute" || measureName == "instance_traffic_minute" ||
			measureName == "endpoint_traffic_minute" {
			return true
		}
		request.DataPoint.Timestamp = timestamppb.New(measureGenerator.adjustTimeFromTime(request.DataPoint.Timestamp.AsTime(), downstream))
		request.Metadata.ModRevision = 0

		return false
	})
	if err != nil {
		return nil, err
	}

	return measureGenerator, nil
}

func (b *measureDataGenerator) setNormalWriteRoundStart(f func()) {
	b.normalWriteRoundStart = f
}

func (b *measureDataGenerator) adjustTimeFromTime(t time.Time, info *downstreamTimeInfo) time.Time {
	switch {
	case t == t.Truncate(time.Minute):
		return info.minute
	case t == t.Truncate(time.Hour):
		return info.hour
	case t == t.Truncate(24*time.Hour):
		return info.day
	default:
		panic("invalid time:" + t.String())
	}
}

func (b *measureDataGenerator) registerTraffic(trafficName string, counter *int64, tp EntityScopeType, generator func() *measurev1.WriteRequest, addedCallback func()) {
	b.trafficRegisterLock.RLock()
	_, exist := b.trafficRegister[trafficName]
	b.trafficRegisterLock.RUnlock()
	if exist {
		return
	}

	b.trafficRegisterLock.Lock()
	defer b.trafficRegisterLock.Unlock()
	b.trafficRegister[trafficName] = true

	unixNano := time.Now().UnixNano()
	traffic := generator()
	traffic.MessageId = uint64(unixNano)
	traffic.DataPoint.Version = unixNano
	select {
	case b.trafficChannel <- traffic:
		*counter++
		if b.trafficRequests[tp] == nil {
			b.trafficRequests[tp] = make([]*measurev1.WriteRequest, 0)
		}
		b.trafficRequests[tp] = append(b.trafficRequests[tp], traffic)
		if addedCallback != nil {
			addedCallback()
		}
	default:
		// delete the traffic register if the channel is full
		delete(b.trafficRegister, trafficName)
	}
}

func (b *measureDataGenerator) registerServiceEndpointTraffic(serviceName, endpointName string) {
	b.registerTraffic(fmt.Sprintf("%s/%s", serviceName, endpointName), &b.trafficCounterEndpoint, EntityScopeTypeEndpoint, func() *measurev1.WriteRequest {
		now := time.Now()
		format := now.Format("200601021504")
		timeBucket, err := strconv.ParseInt(format, 10, 64)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to parse time bucket: %v", timeBucket))
		return &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Group:       "metadata",
				Name:        "endpoint_traffic_minute",
				ModRevision: 0,
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(now.Truncate(time.Minute)),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: endpointName}}},
							{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: timeBucket}}},
						},
					},
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: base64.StdEncoding.EncodeToString([]byte(serviceName)) + ".1"}}},
						},
					},
				},
				Version: now.UnixNano(),
			},
			MessageId: uint64(now.UnixNano()),
		}
	}, func() {
		endpoints, exist := b.serviceEndpointCache[serviceName]
		if !exist {
			endpoints = make(map[string]bool)
			b.serviceEndpointCache[serviceName] = endpoints
		}
		endpoints[endpointName] = true
	})
}

func (b *measureDataGenerator) registerServiceInstanceTraffic(serviceName, instanceName string) {
	b.registerTraffic(fmt.Sprintf("%s/%s", serviceName, instanceName), &b.trafficCounterInstance, EntityScopeTypeServiceInstance, func() *measurev1.WriteRequest {
		now := time.Now()
		format := now.Format("200601021504")
		timeBucket, err := strconv.ParseInt(format, 10, 64)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to parse time bucket: %v", timeBucket))
		return &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Group:       "metadata",
				Name:        "instance_traffic_minute",
				ModRevision: 0,
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(now.Truncate(time.Minute)),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: timeBucket}}},
						},
					},
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: base64.StdEncoding.EncodeToString([]byte(serviceName)) + ".1"}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: instanceName}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{}}},
						},
					},
				},
				Version: now.UnixNano(),
			},
			MessageId: uint64(now.UnixNano()),
		}
	}, func() {
		instances, exist := b.serviceInstanceCache[serviceName]
		if !exist {
			instances = make(map[string]bool)
			b.serviceInstanceCache[serviceName] = instances
		}
		instances[instanceName] = true
	})
}

func (b *measureDataGenerator) registerServiceTraffic(serviceName string) {
	b.registerTraffic(serviceName, &b.trafficCounterService, EntityScopeTypeService, func() *measurev1.WriteRequest {
		now := time.Now()
		return &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Group:       "metadata",
				Name:        "service_traffic_minute",
				ModRevision: 0,
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(now.Truncate(time.Minute)),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: serviceName}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: base64.StdEncoding.EncodeToString([]byte(serviceName)) + ".1"}}},
						},
					},
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: serviceName}}},
							{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 7}}},
						},
					},
				},
				Version: now.UnixNano(),
			},
			MessageId: uint64(now.UnixNano()),
		}
	}, nil)
}

func (b *measureDataGenerator) changeMeasureBasicTimestamp(req *measurev1.WriteRequest, t time.Time) {
	req.DataPoint.Timestamp = timestamppb.New(t.Truncate(time.Minute))
	req.DataPoint.Version = t.UnixNano()
	req.MessageId = uint64(t.UnixNano())
}

func (b *measureDataGenerator) start() {
	b.baseGenerator.start(b)
}

func (b *measureDataGenerator) afterInitDataRound(downstream *downstreamTimeInfo) {
	if b.normalWriteRoundStart != nil {
		b.normalWriteRoundStart()
	}
	if b.mode == "read" {
		return
	}
	// sending the traffic data again to make sure the traffic data is always exist
	requests := b.trafficRequests[EntityScopeTypeServiceInstance]
	for _, r := range requests {
		for _ = range 2 {
			req := proto.Clone(r).(*measurev1.WriteRequest)
			b.changeMeasureBasicTimestamp(req, downstream.now)
			req.DataPoint.TagFamilies[0].Tags[0].Value = &modelv1.TagValue_Int{Int: &modelv1.Int{Value: downstream.minuteTimeBucket}}
			serviceIDTag := req.DataPoint.TagFamilies[1].Tags[0].Value
			sn, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(serviceIDTag.(*modelv1.TagValue_Str).Str.Value, ".1"))
			if err != nil {
				panic(err)
			}
			marshal, err := protojson.Marshal(req)
			if err != nil {
				panic(err)
			}
			fmt.Println("generated instance traffic data for ", string(sn), req.DataPoint.TagFamilies[1].Tags[1].Value.(*modelv1.TagValue_Str).Str.Value,
				":", string(marshal))
			b.trafficChannel <- req
		}
	}
	requests = b.trafficRequests[EntityScopeTypeEndpoint]
	for _, r := range requests {
		for _ = range 2 {
			req := proto.Clone(r).(*measurev1.WriteRequest)
			b.changeMeasureBasicTimestamp(req, downstream.now)
			req.DataPoint.TagFamilies[0].Tags[1].Value = &modelv1.TagValue_Int{Int: &modelv1.Int{Value: downstream.minuteTimeBucket}}
			b.trafficChannel <- req
		}
	}

	// adding the hour and day data
	if downstream.now.Minute()%2 == 0 {
		b.hourGenerator.generateDataFromFileStart(downstream)
		b.dayGenerator.generateDataFromFileStart(downstream)
	}
}

type streamDataGenerator struct {
	*baseGenerator[*streamv1.WriteRequest]

	schema map[string]*StreamSchema
}

func newStreamDataGenerator(path, mode string, scaleCounts *scalerCounts, requestChannelSize int, schemas map[string]*StreamSchema, measureGenerator *measureDataGenerator) (*streamDataGenerator, error) {
	generator, err := newBaseGenerator[*streamv1.WriteRequest](path, mode, scaleCounts, requestChannelSize, func() *streamv1.WriteRequest {
		return &streamv1.WriteRequest{}
	}, func(request *streamv1.WriteRequest) schema[*streamv1.WriteRequest] {
		return schemas[request.Metadata.Name]
	}, func(dataChannel chan requestWrapper[*streamv1.WriteRequest]) (*requestWrapper[*streamv1.WriteRequest], bool) {
		select {
		case req := <-dataChannel:
			return &req, false
		default:
		}
		return nil, false
	}, func(scaler *dataScaler[*streamv1.WriteRequest], serviceNames []string, subEntity string, request *streamv1.WriteRequest, downstream *downstreamTimeInfo) bool {
		for _, serviceName := range serviceNames {
			measureGenerator.registerServiceTraffic(serviceName)
		}
		request.Element.Timestamp = timestamppb.New(time.Now())
		request.Metadata.ModRevision = 0

		return false
	})
	if err != nil {
		return nil, err
	}
	return &streamDataGenerator{
		baseGenerator: generator,
		schema:        schemas,
	}, nil
}

func (s *streamDataGenerator) start() {
	s.baseGenerator.start(nil)
}
