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
	postGenerate        func(scaler *dataScaler[Request], serviceNames []string, request Request, downstream *downstreamTimeInfo) bool
	currentFile         *os.File
	cancel              context.CancelFunc
	scaleServiceCount   int
	downstreamLock      sync.RWMutex
}

type generatorCallback interface {
	afterInitDataRound(downstream *downstreamTimeInfo)
}

func newBaseGenerator[Request proto.Message](
	path string,
	scaleServiceCount int,
	requestChannelLen int,
	generate func() Request,
	getSchema func(Request) schema[Request],
	getDataFromChannels func(dataChannel chan requestWrapper[Request]) (*requestWrapper[Request], bool),
	postGenerate func(scaler *dataScaler[Request], serviceNames []string, request Request, downstream *downstreamTimeInfo) bool,
) (*baseGenerator[Request], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &baseGenerator[Request]{
		scaleServiceCount:   scaleServiceCount,
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

	//fast write next 1h data
	for i := range 60 * 24 * 3 {
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
	fmt.Println("fast write next 3d data finished, duration:", duration)

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
		b.generateDataFromFileStart(downstream)
		b.generateDataFromFileStart(downstream)
		duration = time.Now().Sub(start)
		fmt.Println("write current minute data twice finished, duration:", duration)
		shouldSkipWaitNextMinute = duration > time.Minute

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

		b.scaler = newDataScaler[Request](b.getSchema(req), req, b.scaleServiceCount)
		data := b.scaler.generate(b.scaleServiceCount, downstream, b)
		for _, d := range data {
			b.requestChannel <- requestWrapper[Request]{request: d, minute: downstream.minute}
		}
	}
}

type downstreamTimeInfo struct {
	minute time.Time
	hour   time.Time
	day    time.Time

	now time.Time
}

func newCurrentMinuteDownstream() *downstreamTimeInfo {
	now := time.Now()
	return &downstreamTimeInfo{
		minute: now.Truncate(time.Minute),
		hour:   now.Truncate(time.Hour),
		day:    now.Truncate(24 * time.Hour),
		now:    now,
	}
}

func (d *downstreamTimeInfo) increaseOneMinute() {
	d.now = d.now.Add(time.Minute)

	d.minute = d.now.Truncate(time.Minute)
	d.hour = d.now.Truncate(time.Hour)
	d.day = d.now.Truncate(24 * time.Hour)
}

type dataScaler[T proto.Message] struct {
	schema             schema[T]
	measureReq         T
	subEntityName      string
	baseServiceNames   []string
	relatedFieldValues []*EntityIDFieldValue
	curScaleCount      int
	maxScaleCount      int
}

func newDataScaler[T proto.Message](schema schema[T], request T, maxScaleCount int) *dataScaler[T] {
	baseServiceNames, err := schema.GetServiceName(request)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to get service names from measure: %s", schema.GetName()))
	relatedFieldValues := schema.GetRelatedFieldValues(request)
	return &dataScaler[T]{
		schema:             schema,
		measureReq:         request,
		maxScaleCount:      maxScaleCount,
		baseServiceNames:   baseServiceNames,
		relatedFieldValues: relatedFieldValues,
	}
}

func (d *dataScaler[T]) getSubEntityName() string {
	if d.subEntityName != "" {
		return d.subEntityName
	}

	schemaName := d.schema.GetName()
	var subEntityName string
	if d.schema.getScope() == entityScopeTypeServiceInstance {
		for _, v := range d.relatedFieldValues {
			if v.field.scope == entityScopeTypeServiceInstance {
				name, err := base64.StdEncoding.DecodeString(v.Value)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to decode service instance name: %s, mesure name: %s", v.Value, schemaName))
				subEntityName = string(name)
			}
		}
	} else if d.schema.getScope() == entityScopeTypeEndpoint {
		for _, v := range d.relatedFieldValues {
			if v.field.scope == entityScopeTypeEndpoint {
				name, err := base64.StdEncoding.DecodeString(v.Value)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to decode service endpoint name: %s, mesure name: %s", v.Value, schemaName))
				subEntityName = string(name)
			}
		}
	}
	gomega.Expect(subEntityName).ToNot(gomega.BeNil(), fmt.Sprintf("%s: subEntityName is empty", d.schema.GetName()))
	return subEntityName
}

func (d *dataScaler[T]) generate(count int, downstream *downstreamTimeInfo, generator *baseGenerator[T]) []T {
	if d.curScaleCount >= d.maxScaleCount {
		return nil
	}

	result := make([]T, 0, count)
	for ; d.curScaleCount < d.maxScaleCount; d.curScaleCount++ {
		newReq := proto.Clone(d.measureReq).(T)
		serviceNames := d.applySequenceToData(newReq, d.curScaleCount)
		if ignore := generator.postGenerate(d, serviceNames, newReq, downstream); ignore {
			continue
		}
		result = append(result, newReq)
	}
	return result
}

func (d *dataScaler[T]) applySequenceToData(req T, sequence int) []string {
	if d.schema.GetType() == schemaTypeMeasure && len(d.baseServiceNames) == 0 {
		panic(fmt.Sprintf("schema cannot found any service entity: %s", d.schema.GetName()))
	}

	serviceNames := make([]string, 0, len(d.baseServiceNames))
	for _, base := range d.baseServiceNames {
		serviceName := fmt.Sprintf("%s-%d", base, sequence)
		serviceNames = append(serviceNames, serviceName)
	}
	for _, fv := range d.relatedFieldValues {
		d.schema.ApplyFieldChange(req, serviceNames, fv)
	}
	return serviceNames
}

type measureDataGenerator struct {
	*baseGenerator[*measurev1.WriteRequest]
	trafficChannel         chan *measurev1.WriteRequest
	trafficRegister        map[string]bool
	trafficRequests        map[entityScopeType][]*measurev1.WriteRequest
	schemas                map[string]*MeasureSchema
	trafficCounterService  int64
	trafficCounterInstance int64
	trafficCounterEndpoint int64
	trafficRegisterLock    sync.RWMutex

	hourGenerator *baseGenerator[*measurev1.WriteRequest]
	dayGenerator  *baseGenerator[*measurev1.WriteRequest]
}

func newMeasureDataGenerator(dir string, scaleServiceCount, requestChannelSize int, schemas map[string]*MeasureSchema) (*measureDataGenerator, error) {
	var err error
	measureGenerator := &measureDataGenerator{
		trafficChannel:  make(chan *measurev1.WriteRequest, requestChannelSize),
		schemas:         schemas,
		trafficRegister: make(map[string]bool),
		trafficRequests: make(map[entityScopeType][]*measurev1.WriteRequest),
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
	measureGenerator.hourGenerator, err = newBaseGenerator[*measurev1.WriteRequest](filepath.Join(dir, "hour"), scaleServiceCount, requestChannelSize, func() *measurev1.WriteRequest {
		return &measurev1.WriteRequest{}
	}, func(request *measurev1.WriteRequest) schema[*measurev1.WriteRequest] {
		return schemas[request.Metadata.Name]
	}, func(dataChannel chan requestWrapper[*measurev1.WriteRequest]) (*requestWrapper[*measurev1.WriteRequest], bool) {
		return nil, false
	}, func(scaler *dataScaler[*measurev1.WriteRequest], serviceNames []string, request *measurev1.WriteRequest, downstream *downstreamTimeInfo) bool {
		request.DataPoint.Timestamp = timestamppb.New(measureGenerator.adjustTimeFromTime(request.DataPoint.Timestamp.AsTime(), downstream))
		request.Metadata.ModRevision = 0
		return false
	})
	if err != nil {
		return nil, err
	}
	measureGenerator.dayGenerator, err = newBaseGenerator[*measurev1.WriteRequest](filepath.Join(dir, "day"), scaleServiceCount, requestChannelSize, func() *measurev1.WriteRequest {
		return &measurev1.WriteRequest{}
	}, func(request *measurev1.WriteRequest) schema[*measurev1.WriteRequest] {
		return schemas[request.Metadata.Name]
	}, func(dataChannel chan requestWrapper[*measurev1.WriteRequest]) (*requestWrapper[*measurev1.WriteRequest], bool) {
		return nil, false
	}, func(scaler *dataScaler[*measurev1.WriteRequest], serviceNames []string, request *measurev1.WriteRequest, downstream *downstreamTimeInfo) bool {
		request.DataPoint.Timestamp = timestamppb.New(measureGenerator.adjustTimeFromTime(request.DataPoint.Timestamp.AsTime(), downstream))
		request.Metadata.ModRevision = 0
		return false
	})
	if err != nil {
		return nil, err
	}

	measureGenerator.baseGenerator, err = newBaseGenerator[*measurev1.WriteRequest](filepath.Join(dir, "minute"), scaleServiceCount, requestChannelSize, func() *measurev1.WriteRequest {
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
	}, func(scaler *dataScaler[*measurev1.WriteRequest], serviceNames []string, request *measurev1.WriteRequest, downstream *downstreamTimeInfo) bool {
		for _, serviceName := range serviceNames {
			measureGenerator.registerServiceTraffic(serviceName)
		}
		if scaler.schema.getScope() == entityScopeTypeServiceInstance {
			measureGenerator.registerServiceInstanceTraffic(serviceNames[0], scaler.getSubEntityName())
		} else if scaler.schema.getScope() == entityScopeTypeEndpoint {
			measureGenerator.registerServiceEndpointTraffic(serviceNames[0], scaler.getSubEntityName())
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

func (b *measureDataGenerator) registerTraffic(trafficName string, counter *int64, tp entityScopeType, generator func() *measurev1.WriteRequest) {
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
	default:
		// delete the traffic register if the channel is full
		delete(b.trafficRegister, trafficName)
	}
}

func (b *measureDataGenerator) registerServiceEndpointTraffic(serviceName, endpointName string) {
	b.registerTraffic(fmt.Sprintf("%s/%s", serviceName, endpointName), &b.trafficCounterEndpoint, entityScopeTypeEndpoint, func() *measurev1.WriteRequest {
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
	})
}

func (b *measureDataGenerator) registerServiceInstanceTraffic(serviceName, instanceName string) {
	b.registerTraffic(fmt.Sprintf("%s/%s", serviceName, instanceName), &b.trafficCounterInstance, entityScopeTypeServiceInstance, func() *measurev1.WriteRequest {
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
	})
}

func (b *measureDataGenerator) registerServiceTraffic(serviceName string) {
	b.registerTraffic(serviceName, &b.trafficCounterService, entityScopeTypeService, func() *measurev1.WriteRequest {
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
	})
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
	// sending the traffic data again to make sure the traffic data is always exist
	requests := b.trafficRequests[entityScopeTypeServiceInstance]
	for _, r := range requests {
		for _ = range 4 {
			req := proto.Clone(r).(*measurev1.WriteRequest)
			b.changeMeasureBasicTimestamp(req, downstream.now)
			b.trafficChannel <- r
		}
	}
	requests = b.trafficRequests[entityScopeTypeEndpoint]
	for _, r := range requests {
		for _ = range 4 {
			req := proto.Clone(r).(*measurev1.WriteRequest)
			b.changeMeasureBasicTimestamp(req, downstream.now)
			b.trafficChannel <- r
		}
	}

	// adding the hour and day data
	b.hourGenerator.generateDataFromFileStart(downstream)
	b.dayGenerator.generateDataFromFileStart(downstream)
}

type streamDataGenerator struct {
	*baseGenerator[*streamv1.WriteRequest]

	schema map[string]*StreamSchema
}

func newStreamDataGenerator(path string, scaleServiceCount, requestChannelSize int, schemas map[string]*StreamSchema, measureGenerator *measureDataGenerator) (*streamDataGenerator, error) {
	generator, err := newBaseGenerator[*streamv1.WriteRequest](path, scaleServiceCount, requestChannelSize, func() *streamv1.WriteRequest {
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
	}, func(scaler *dataScaler[*streamv1.WriteRequest], serviceNames []string, request *streamv1.WriteRequest, downstream *downstreamTimeInfo) bool {
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
