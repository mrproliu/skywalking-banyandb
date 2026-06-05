// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package streammigration

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

const (
	testGroup      = "default"
	testStream     = "sw"
	totalElements  = 20
	durationHi     = int64(999) // INVERTED-indexed value used for filtering.
	durationLo     = int64(100)
	hiElementCount = 8 // number of elements written with duration == durationHi.
)

// TestStreamMigrationE2E proves the full real stream-migration pipeline through
// the external gRPC services:
//
//	register schema -> write elements -> snapshot -> StreamMigrationCopy ->
//	StreamMigrationVerify -> start a second standalone on the migrated data ->
//	query it back over gRPC and assert rows + INVERTED-indexed-tag filtering.
func TestStreamMigrationE2E(t *testing.T) {
	gomega.RegisterTestingT(t)
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())

	// All timestamps land inside a single day-aligned segment (fast path): pick a
	// fixed midday base time and space elements by one minute, keeping all 20
	// within the same UTC day.
	now := time.Now().UTC()
	baseTime := time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, time.UTC).Truncate(time.Millisecond)

	// ── Stage 1: standalone #1 (source) with the stream schema preloaded. ──
	srcRoot, srcRootCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer srcRootCleanup()
	srcAddr, srcClose := startStandalone(t, srcRoot)
	defer srcClose()

	srcConn, err := grpchelper.Conn(srcAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() { _ = srcConn.Close() }()

	// ── Stage 2: write totalElements elements over gRPC StreamService.Write. ──
	writeElements(t, srcConn, baseTime)

	// ── Stage 3: poll-until-queryable, then snapshot. ──
	pollUntilQueryable(t, srcConn, baseTime, totalElements)
	streamSnapshotDir, schemaPropertyDir := takeSnapshot(t, srcConn, srcRoot)

	// ── Stage 4: run StreamMigrationCopy. ──
	tgtRoot, tgtRootCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer tgtRootCleanup()
	sidxStaging, sidxStagingCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer sidxStagingCleanup()

	migrateTarget := filepath.Join(tgtRoot, "stream", "data")
	cfg := stream.StreamDirectCopyConfig{
		SchemaPropertyPath: schemaPropertyDir,
		SidxStagingDir:     sidxStaging,
		Groups:             []string{testGroup},
		Entries: []stream.StreamDirectCopyEntry{
			{
				Stage:  stream.StreamStageHot,
				Target: migrateTarget,
				Source: []string{streamSnapshotDir},
			},
		},
	}
	res, err := stream.StreamMigrationCopy(context.Background(), cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(res.Rows).To(gomega.Equal(int64(totalElements)),
		"migration copied row count must equal written element count")
	t.Logf("StreamMigrationCopy: rows=%d srcParts=%d targetParts=%d segs=%d fastPath=%d slowPath=%d",
		res.Rows, res.SourceParts, res.TargetParts, res.Segments,
		stream.StreamFastPathHits(), stream.StreamSlowPathHits())
	gomega.Expect(stream.StreamSlowPathHits()).To(gomega.BeZero(),
		"single-segment write must take the fast path")

	// ── Stage 5: StreamMigrationVerify — source rows must equal target rows. ──
	var srcRows, tgtRows uint64
	var verifySegs []stream.StreamSegmentReport
	verifyErr := stream.StreamMigrationVerify(context.Background(), cfg, func(r stream.StreamEntryGroupReport) {
		srcRows += r.SrcRows
		for _, seg := range r.TargetSegs {
			tgtRows += seg.Rows
			verifySegs = append(verifySegs, seg)
		}
	})
	gomega.Expect(verifyErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(srcRows).To(gomega.Equal(uint64(totalElements)))
	gomega.Expect(tgtRows).To(gomega.Equal(srcRows),
		"stream never deduplicates: target rows must equal source rows exactly")

	// Assert the migrated segment's element index (idx/) is non-empty:
	// at least one shard must have IdxOpened==true and IdxDocCount>0.
	var idxOpenedAndNonEmpty bool
	for _, seg := range verifySegs {
		for _, sh := range seg.Shards {
			if sh.IdxOpened && sh.IdxDocCount > 0 {
				idxOpenedAndNonEmpty = true
			}
		}
	}
	gomega.Expect(idxOpenedAndNonEmpty).To(gomega.BeTrue(),
		"migrated segment must have at least one shard with IdxOpened==true and IdxDocCount>0")

	// ── Stage 6: standalone #2 over the migrated data; query back over gRPC. ──
	tgtAddr, tgtClose := startStandalone(t, tgtRoot)
	defer tgtClose()
	tgtConn, err := grpchelper.Conn(tgtAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() { _ = tgtConn.Close() }()

	// (a) Query all → all rows present on the migrated standalone.
	pollUntilQueryable(t, tgtConn, baseTime, totalElements)
	allElems := queryStream(t, tgtConn, baseTime, nil)
	gomega.Expect(allElems).To(gomega.HaveLen(totalElements),
		"all migrated rows must be queryable on the second standalone")
	assertAllDurations(t, allElems)

	// (b) Filter by the INVERTED-indexed `duration` tag → exactly the matching
	// elements. This proves the migrated idx/ element index works.
	hiCriteria := &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: "duration",
				Op:   modelv1.Condition_BINARY_OP_EQ,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: durationHi}},
				},
			},
		},
	}
	hiElems := queryStream(t, tgtConn, baseTime, hiCriteria)
	gomega.Expect(hiElems).To(gomega.HaveLen(hiElementCount),
		"INVERTED duration=%d filter on migrated idx/ must return exactly %d elements", durationHi, hiElementCount)
	for _, e := range hiElems {
		gomega.Expect(tagInt(e, "searchable", "duration")).To(gomega.Equal(durationHi))
	}

	// (c) Series/entity query path (proves sidx/): filter by an entity tag.
	// service_instance_id is part of the entity (service_id, service_instance_id, state).
	entityCriteria := &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: "service_instance_id",
				Op:   modelv1.Condition_BINARY_OP_EQ,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-0"}},
				},
			},
		},
	}
	entityElems := queryStream(t, tgtConn, baseTime, entityCriteria)
	gomega.Expect(len(entityElems)).To(gomega.Equal(10),
		"entity/series query over the migrated sidx/ must return exactly 10 elements (i%%2==0 → instance-0)")
	for _, e := range entityElems {
		gomega.Expect(tagStr(e, "searchable", "service_instance_id")).To(gomega.Equal("instance-0"))
	}
	t.Logf("query results: all=%d duration=%d→%d entity(instance-0)=%d",
		len(allElems), durationHi, len(hiElems), len(entityElems))
}

// startStandalone starts a standalone server rooted at root with the stream
// schema preloaded over the property schema registry, and returns its gRPC addr.
func startStandalone(t *testing.T, root string) (string, func()) {
	t.Helper()
	tmpDir, tmpDirCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	config.AddLoadedKinds(schema.KindStream)
	ports, err := test.AllocateFreePorts(5)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, _, closeFn := setup.ClosableStandaloneWithSchemaLoaders(
		config, root, ports,
		[]setup.SchemaLoader{newStreamSchemaLoader()},
		"--stream-flush-timeout=500ms",
	)
	return addr, func() {
		closeFn()
		tmpDirCleanup()
	}
}

// streamSchemaLoader preloads only the stream schema into the registry.
type streamSchemaLoader struct {
	registry schema.Registry
}

func newStreamSchemaLoader() *streamSchemaLoader { return &streamSchemaLoader{} }

func (s *streamSchemaLoader) Name() string { return "preload-stream" }

func (s *streamSchemaLoader) PreRun(ctx context.Context) error {
	return test_stream.PreloadSchema(ctx, s.registry)
}

func (s *streamSchemaLoader) SetRegistry(registry schema.Registry) { s.registry = registry }

// writeElements writes totalElements elements over gRPC StreamService.Write,
// draining responses and asserting each status == SUCCEED.
func writeElements(t *testing.T, conn *grpc.ClientConn, baseTime time.Time) {
	t.Helper()
	schemaClient := databasev1.NewStreamRegistryServiceClient(conn)
	getResp, err := schemaClient.Get(context.Background(), &databasev1.StreamRegistryServiceGetRequest{
		Metadata: &commonv1.Metadata{Name: testStream, Group: testGroup},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	md := getResp.GetStream().GetMetadata()

	c := streamv1.NewStreamServiceClient(conn)
	writeClient, err := c.Write(context.Background())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for i := 0; i < totalElements; i++ {
		duration := durationLo
		if i < hiElementCount {
			duration = durationHi
		}
		instance := "instance-0"
		if i%2 == 1 {
			instance = "instance-1"
		}
		ts := baseTime.Add(time.Duration(i) * time.Minute)
		element := buildElementValue(i, ts, instance, duration)
		gomega.Expect(writeClient.Send(&streamv1.WriteRequest{
			Metadata:  md,
			Element:   element,
			MessageId: uint64(time.Now().UnixNano()),
		})).To(gomega.Succeed())
	}
	gomega.Expect(writeClient.CloseSend()).To(gomega.Succeed())
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		gomega.Expect(recvErr).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.GetStatus()).To(gomega.Equal(modelv1.Status_STATUS_SUCCEED.String()),
			"write status must be SUCCEED")
	}
}

// buildElementValue builds one element. Tag order in the "searchable" family
// must match pkg/test/stream/testdata/streams/sw.json.
func buildElementValue(id int, ts time.Time, instance string, duration int64) *streamv1.ElementValue {
	strTag := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	intTag := func(v int64) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
	}
	nullTag := func() *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
	searchable := []*modelv1.TagValue{
		strTag("trace-" + itoa(id)), // trace_id
		intTag(1),                   // state
		strTag("service-0"),         // service_id
		strTag(instance),            // service_instance_id
		strTag("/endpoint"),         // endpoint_id
		intTag(duration),            // duration (INVERTED)
		intTag(ts.UnixNano()),       // start_time
		strTag("GET"),               // http.method
		intTag(200),                 // status_code
		strTag("span-" + itoa(id)),  // span_id
		strTag("mysql"),             // db.type
		strTag("db-" + itoa(id)),    // db.instance (INVERTED, url analyzer)
		nullTag(),                   // mq.queue
		nullTag(),                   // mq.topic
		nullTag(),                   // mq.broker
		nullTag(),                   // extended_tags (str array, INVERTED)
		nullTag(),                   // non_indexed_tags (str array)
	}
	binary := []byte("payload-" + itoa(id))
	return &streamv1.ElementValue{
		ElementId: itoa(id),
		Timestamp: timestamppb.New(ts),
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: binary}}}},
			{Tags: searchable},
		},
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// pollUntilQueryable queries the stream until it returns at least want rows.
func pollUntilQueryable(t *testing.T, conn *grpc.ClientConn, baseTime time.Time, want int) {
	t.Helper()
	gomega.Eventually(func() int {
		return len(queryStream(t, conn, baseTime, nil))
	}, flags.EventuallyTimeout).WithPolling(500 * time.Millisecond).
		Should(gomega.BeNumerically(">=", want))
}

// queryStream runs a StreamService.Query over [baseTime, baseTime+1 day) with an
// explicit Limit and an optional criteria, returning the matched elements.
func queryStream(t *testing.T, conn *grpc.ClientConn, baseTime time.Time, criteria *modelv1.Criteria) []*streamv1.Element {
	t.Helper()
	c := streamv1.NewStreamServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &streamv1.QueryRequest{
		Groups: []string{testGroup},
		Name:   testStream,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(baseTime.Add(-time.Hour)),
			End:   timestamppb.New(baseTime.Add(48 * time.Hour)),
		},
		Limit:    1000,
		Criteria: criteria,
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: "searchable",
					Tags: []string{"trace_id", "service_id", "service_instance_id", "duration", "db.instance"},
				},
			},
		},
	}
	resp, err := c.Query(ctx, req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), req.String())
	return resp.GetElements()
}

// assertAllDurations checks the migrated rows carry the expected duration split.
func assertAllDurations(t *testing.T, elems []*streamv1.Element) {
	t.Helper()
	var hi, lo int
	for _, e := range elems {
		switch tagInt(e, "searchable", "duration") {
		case durationHi:
			hi++
		case durationLo:
			lo++
		}
	}
	gomega.Expect(hi).To(gomega.Equal(hiElementCount))
	gomega.Expect(lo).To(gomega.Equal(totalElements - hiElementCount))
}

func tagInt(e *streamv1.Element, family, name string) int64 {
	tv := lookupTag(e, family, name)
	gomega.Expect(tv).NotTo(gomega.BeNil(), "tag %s.%s not found", family, name)
	return tv.GetInt().GetValue()
}

func tagStr(e *streamv1.Element, family, name string) string {
	tv := lookupTag(e, family, name)
	gomega.Expect(tv).NotTo(gomega.BeNil(), "tag %s.%s not found", family, name)
	return tv.GetStr().GetValue()
}

func lookupTag(e *streamv1.Element, family, name string) *modelv1.TagValue {
	for _, tf := range e.GetTagFamilies() {
		if tf.GetName() != family {
			continue
		}
		for _, tag := range tf.GetTags() {
			if tag.GetKey() == name {
				return tag.GetValue()
			}
		}
	}
	return nil
}

// takeSnapshot calls SnapshotService.Snapshot and returns the on-disk stream
// snapshot dir (whose children are group dirs) and the schema-property _schema dir.
func takeSnapshot(t *testing.T, conn *grpc.ClientConn, root string) (streamSnapshotDir, schemaPropertyDir string) {
	t.Helper()
	client := databasev1.NewSnapshotServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := client.Snapshot(ctx, &databasev1.SnapshotRequest{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.GetSnapshots()).NotTo(gomega.BeEmpty())

	for _, snp := range resp.GetSnapshots() {
		dir, dirErr := backupsnapshot.Dir(snp, root, root, root, root, root)
		gomega.Expect(dirErr).NotTo(gomega.HaveOccurred())
		// schema-property snapshots are named "schema-property/<name>"; the
		// resolved Dir already points at .../data/_schema.
		if strings.HasPrefix(snp.GetName(), backupsnapshot.SchemaPropertyCatalogName+"/") {
			// Dir resolves to .../schema-property/snapshots/<name>/data; the bluge
			// schema index lives in the _schema subdir, which is what
			// StreamMigrationCopy expects as SchemaPropertyPath.
			schemaPropertyDir = filepath.Join(dir, schema.SchemaGroup)
			continue
		}
		if snp.GetCatalog() == commonv1.Catalog_CATALOG_STREAM {
			streamSnapshotDir = dir
		}
	}
	gomega.Expect(streamSnapshotDir).NotTo(gomega.BeEmpty(), "stream snapshot dir not found")
	gomega.Expect(schemaPropertyDir).NotTo(gomega.BeEmpty(), "schema-property _schema snapshot dir not found")

	// Sanity: the stream snapshot must contain the group dir, and the _schema dir
	// must contain shard-* children (the bluge schema index).
	groupEntries, err := os.ReadDir(streamSnapshotDir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var sawGroup bool
	for _, e := range groupEntries {
		if e.IsDir() && e.Name() == testGroup {
			sawGroup = true
		}
	}
	gomega.Expect(sawGroup).To(gomega.BeTrue(), "stream snapshot %s missing group %s", streamSnapshotDir, testGroup)
	schemaEntries, err := os.ReadDir(schemaPropertyDir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(schemaEntries).NotTo(gomega.BeEmpty(), "_schema dir %s is empty", schemaPropertyDir)
	t.Logf("snapshots: stream=%s schema-property=%s", streamSnapshotDir, schemaPropertyDir)
	return streamSnapshotDir, schemaPropertyDir
}
