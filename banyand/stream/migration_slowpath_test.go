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

package stream

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// TestMigrationSlowPathElementIndexRebuild proves the slow-path element-index
// rebuild. A single source (seg, shard) holds four elements whose timestamps span
// TWO target day-segments; an INVERTED-indexed non-entity string tag "status" is
// present. After running directCopyStreamGroup with a 1-day target SegmentInterval
// (which splits the source rows into two target segs), it asserts:
//  1. Each target seg's data-part row count summed equals the source total (no loss).
//  2. Each target seg has an idx/ that, opened via inverted.NewStore (BatchWaitSec:0)
//     + MatchTerms, returns ONLY the elementIDs whose data is in THAT seg.
func TestMigrationSlowPathElementIndexRebuild(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group     = "sw_segment"
		streamN   = "sw_segment"
		shardN    = "shard-0"
		statusRID = uint32(7)
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := nofsyncStreamFS{FileSystem: fileSystem}
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}

	// Two distinct day segments.
	day1 := ir.Standard(time.Date(2026, 6, 1, 10, 0, 0, 0, time.Local))
	day2 := ir.Standard(time.Date(2026, 6, 3, 10, 0, 0, 0, time.Local))
	require.NotEqual(t, day1, day2)

	// Source seg spans both days; its name comes from the source-min day.
	srcSegName := formatStreamDirectCopySegName(day1, storage.DAY)
	sourceGroupRoot := filepath.Join(tmpDir, "source", group)
	srcShardDir := filepath.Join(sourceGroupRoot, srcSegName, shardN)
	require.NoError(t, os.MkdirAll(srcShardDir, storage.DirPerm))

	// Four elements: two in day1, two in day2; same series; status ok/err per day.
	d1ts0 := day1.Add(1 * time.Hour).UnixNano()
	d1ts1 := day1.Add(2 * time.Hour).UnixNano()
	d2ts0 := day2.Add(1 * time.Hour).UnixNano()
	d2ts1 := day2.Add(2 * time.Hour).UnixNano()
	const (
		eD1ok  = uint64(1001)
		eD1err = uint64(1002)
		eD2ok  = uint64(2001)
		eD2err = uint64(2002)
	)
	rows := []DumpRow{
		{
			Entity: "svc-A", Timestamp: d1ts0, ElementID: eD1ok,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "ok"}},
		},
		{
			Entity: "svc-A", Timestamp: d1ts1, ElementID: eD1err,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "err"}},
		},
		{
			Entity: "svc-A", Timestamp: d2ts0, ElementID: eD2ok,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "ok"}},
		},
		{
			Entity: "svc-A", Timestamp: d2ts1, ElementID: eD2err,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "err"}},
		},
	}
	const srcPartID = uint64(1)
	_, rows, cleanup := BuildPartForDump(srcShardDir, noFsync, srcPartID, rows)
	defer cleanup()
	srcRows := uint64(len(rows))
	seriesID := rows[0].SeriesID

	// A source idx/ must exist so the finalize step knows this (seg, shard) has an
	// element index (its contents are never read on the rebuild path).
	srcIdxDir := filepath.Join(srcShardDir, directStreamCopyIdxDirName)
	require.NoError(t, os.MkdirAll(srcIdxDir, storage.DirPerm))
	seedStore, err := inverted.NewStore(inverted.StoreOpts{Path: srcIdxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	require.NoError(t, seedStore.Close())

	// Hand-built locator: one tag family "default" with an INVERTED-indexed
	// non-entity string tag "status"; entity is meta.name (not indexed).
	families := []*databasev1.TagFamilySpec{
		{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
	entity := &databasev1.Entity{TagNames: []string{"name"}}
	statusRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: statusRID, Name: "status"},
		Tags:     []string{"status"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
		Analyzer: index.AnalyzerKeyword,
	}
	locators, _ := partition.ParseIndexRuleLocators(entity, families, []*databasev1.IndexRule{statusRule}, false)
	indexLocators := map[string]*streamIndexLocator{
		streamN: {
			Locators:   locators,
			IndexRules: []*databasev1.IndexRule{statusRule},
			Entity:     entity,
			Families:   families,
		},
	}

	// Discover tasks and run the group copy.
	tasks, err := discoverStreamPartTasks(context.Background(), []string{sourceGroupRoot})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	dstGroupRoot := filepath.Join(tmpDir, "target", group)
	tagProjection := []model.TagProjection{{Family: "default", Names: []string{"status"}}}

	res, err := directCopyStreamGroup(
		context.Background(),
		"entry [1/1]",
		group,
		StreamDirectCopyEntry{Stage: StreamStageHot, Target: dstGroupRoot},
		dstGroupRoot,
		ir, tagProjection,
		tasks, "",
		indexLocators,
	)
	require.NoError(t, err)
	require.EqualValues(t, srcRows, res.Rows, "stream invariant: target rows == source rows")

	d1Seg := formatStreamDirectCopySegName(day1, storage.DAY)
	d2Seg := formatStreamDirectCopySegName(day2, storage.DAY)
	require.NotEqual(t, d1Seg, d2Seg)

	// (1) Row counts: each target seg's data rows summed equals the source total.
	d1Rows, _, vErr := StreamVerifyShardParts(filepath.Join(dstGroupRoot, d1Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	d2Rows, _, vErr := StreamVerifyShardParts(filepath.Join(dstGroupRoot, d2Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	require.EqualValues(t, 2, d1Rows, "day1 target must hold 2 elements")
	require.EqualValues(t, 2, d2Rows, "day2 target must hold 2 elements")
	require.EqualValues(t, srcRows, d1Rows+d2Rows, "no loss across target segs")

	// (2) Each target seg has an idx/ returning ONLY its own elementIDs.
	assertIdxTerm := func(seg, value string, want []uint64) {
		idxPath := filepath.Join(dstGroupRoot, seg, shardN, directStreamCopyIdxDirName)
		info, statErr := os.Stat(idxPath)
		require.NoError(t, statErr, "target seg %s must have an idx/", seg)
		require.True(t, info.IsDir())
		store, openErr := inverted.NewStore(inverted.StoreOpts{Path: idxPath, BatchWaitSec: 0})
		require.NoError(t, openErr)
		defer func() { require.NoError(t, store.Close()) }()
		list, _, matchErr := store.MatchTerms(index.NewStringField(index.FieldKey{
			IndexRuleID: statusRID,
			SeriesID:    seriesID,
		}, value))
		require.NoError(t, matchErr)
		got := map[uint64]struct{}{}
		iter := list.Iterator()
		for iter.Next() {
			got[iter.Current()] = struct{}{}
		}
		require.NoError(t, iter.Close())
		require.Len(t, got, len(want), "seg %s status=%s wrong doc count", seg, value)
		for _, id := range want {
			_, ok := got[id]
			require.Truef(t, ok, "seg %s status=%s missing elementID %d", seg, value, id)
		}
	}

	// day1: only eD1ok for "ok", only eD1err for "err"; no day2 contamination.
	assertIdxTerm(d1Seg, "ok", []uint64{eD1ok})
	assertIdxTerm(d1Seg, "err", []uint64{eD1err})
	// day2: only eD2ok / eD2err.
	assertIdxTerm(d2Seg, "ok", []uint64{eD2ok})
	assertIdxTerm(d2Seg, "err", []uint64{eD2err})

	// Sanity: the slow path was actually exercised (source seg split into 2 targets).
	require.GreaterOrEqual(t, res.Segments, 2, "expected at least two target segments")
	require.GreaterOrEqual(t, StreamSlowPathHits(), int64(1),
		"slow path must have been exercised at least once for a cross-segment source part")
}

// streamSeries builds a marshaled pbv1.Series (subject + one string entity value)
// and returns its SeriesID plus the marshaled buffer used to seed the source seg's
// series index (sidx). This mirrors how the write path keys series documents.
func streamSeries(t *testing.T, subject, entityValue string) (common.SeriesID, []byte) {
	t.Helper()
	s := &pbv1.Series{
		Subject:      subject,
		EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityValue}}}},
	}
	require.NoError(t, s.Marshal())
	return s.ID, append([]byte(nil), s.Buffer...)
}

// TestMigrationSlowPathMultiStreamElementIndexRebuild proves the slow-path
// element-index rebuild for a group that owns MORE THAN ONE stream (the case the
// implementation previously aborted on). The group holds two streams:
//   - streamA: entity tag "name", INVERTED non-entity tag "status"
//   - streamB: entity tag "service", INVERTED non-entity tag "code" + INVERTED
//     entity tag "service" (to also exercise per-row entity resolution)
//
// Rows of BOTH streams span two target day-segments. Each row's owning stream is
// recoverable only from its seriesID -> source-sidx Series.Subject. After the copy
// it asserts each target seg's idx/ returns exactly the right docs per stream's
// indexed tag, with no cross-stream or cross-segment contamination.
func TestMigrationSlowPathMultiStreamElementIndexRebuild(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group   = "multi"
		streamA = "streamA"
		streamB = "streamB"
		shardN  = "shard-0"

		statusRID  = uint32(7) // streamA non-entity tag
		codeRID    = uint32(8) // streamB non-entity tag
		serviceRID = uint32(9) // streamB entity tag (indexed entity)
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := nofsyncStreamFS{FileSystem: fileSystem}
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}

	day1 := ir.Standard(time.Date(2026, 6, 1, 10, 0, 0, 0, time.Local))
	day2 := ir.Standard(time.Date(2026, 6, 3, 10, 0, 0, 0, time.Local))
	require.NotEqual(t, day1, day2)

	srcSegName := formatStreamDirectCopySegName(day1, storage.DAY)
	sourceGroupRoot := filepath.Join(tmpDir, "source", group)
	srcShardDir := filepath.Join(sourceGroupRoot, srcSegName, shardN)
	require.NoError(t, os.MkdirAll(srcShardDir, storage.DirPerm))

	// Two distinct series per stream is unnecessary; one series per stream suffices
	// to prove Subject-based routing. Each series carries rows in both day segments.
	aSeriesID, aBuf := streamSeries(t, streamA, "svc-A")
	bSeriesID, bBuf := streamSeries(t, streamB, "svc-B")
	require.NotEqual(t, aSeriesID, bSeriesID)

	d1t0 := day1.Add(1 * time.Hour).UnixNano()
	d1t1 := day1.Add(2 * time.Hour).UnixNano()
	d2t0 := day2.Add(1 * time.Hour).UnixNano()
	d2t1 := day2.Add(2 * time.Hour).UnixNano()

	const (
		aD1ok  = uint64(1101)
		aD1err = uint64(1102)
		aD2ok  = uint64(2101)
		aD2err = uint64(2102)
		bD1ok  = uint64(1201)
		bD1err = uint64(1202)
		bD2ok  = uint64(2201)
		bD2err = uint64(2202)
	)
	rows := []DumpRow{
		// streamA rows (tag family "fa", tag "status").
		{SeriesID: aSeriesID, Timestamp: d1t0, ElementID: aD1ok, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "ok"}}},
		{SeriesID: aSeriesID, Timestamp: d1t1, ElementID: aD1err, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "err"}}},
		{SeriesID: aSeriesID, Timestamp: d2t0, ElementID: aD2ok, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "ok"}}},
		{SeriesID: aSeriesID, Timestamp: d2t1, ElementID: aD2err, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "err"}}},
		// streamB rows (tag family "fb", tag "code").
		{SeriesID: bSeriesID, Timestamp: d1t0, ElementID: bD1ok, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "200"}}},
		{SeriesID: bSeriesID, Timestamp: d1t1, ElementID: bD1err, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "500"}}},
		{SeriesID: bSeriesID, Timestamp: d2t0, ElementID: bD2ok, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "200"}}},
		{SeriesID: bSeriesID, Timestamp: d2t1, ElementID: bD2err, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "500"}}},
	}
	const srcPartID = uint64(1)
	_, rows, cleanup := BuildPartForDump(srcShardDir, noFsync, srcPartID, rows)
	defer cleanup()
	srcRows := uint64(len(rows))

	// Seed the source segment's series index (sidx) so the rebuild can map each
	// row's seriesID -> Series.Subject (the stream name) + entity values.
	srcSegPath := filepath.Join(sourceGroupRoot, srcSegName)
	sidxDir := filepath.Join(srcSegPath, "sidx")
	require.NoError(t, os.MkdirAll(sidxDir, storage.DirPerm))
	sidxStore, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	require.NoError(t, sidxStore.InsertSeriesBatch(index.Batch{Documents: index.Documents{
		{EntityValues: aBuf},
		{EntityValues: bBuf},
	}}))
	require.NoError(t, sidxStore.Close())

	// A source idx/ must exist so the finalize step knows this (seg, shard) carries
	// an element index (its contents are never read on the rebuild path).
	srcIdxDir := filepath.Join(srcShardDir, directStreamCopyIdxDirName)
	require.NoError(t, os.MkdirAll(srcIdxDir, storage.DirPerm))
	seedStore, err := inverted.NewStore(inverted.StoreOpts{Path: srcIdxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	require.NoError(t, seedStore.Close())

	// Per-stream locators. streamA: entity "name" (not indexed), INVERTED "status".
	// streamB: entity "service" (INVERTED), INVERTED "code".
	famA := []*databasev1.TagFamilySpec{{
		Name: "fa",
		Tags: []*databasev1.TagSpec{{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}}
	entityA := &databasev1.Entity{TagNames: []string{"name"}}
	statusRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: statusRID, Name: "status"},
		Tags:     []string{"status"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword,
	}
	locA, _ := partition.ParseIndexRuleLocators(entityA, famA, []*databasev1.IndexRule{statusRule}, false)

	famB := []*databasev1.TagFamilySpec{{
		Name: "fb",
		Tags: []*databasev1.TagSpec{
			{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "code", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}}
	entityB := &databasev1.Entity{TagNames: []string{"service"}}
	codeRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: codeRID, Name: "code"},
		Tags:     []string{"code"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword,
	}
	serviceRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: serviceRID, Name: "service"},
		Tags:     []string{"service"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword,
	}
	locB, _ := partition.ParseIndexRuleLocators(entityB, famB, []*databasev1.IndexRule{codeRule, serviceRule}, false)

	indexLocators := map[string]*streamIndexLocator{
		streamA: {Locators: locA, IndexRules: []*databasev1.IndexRule{statusRule}, Entity: entityA, Families: famA},
		streamB: {Locators: locB, IndexRules: []*databasev1.IndexRule{codeRule, serviceRule}, Entity: entityB, Families: famB},
	}

	tasks, err := discoverStreamPartTasks(context.Background(), []string{sourceGroupRoot})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	dstGroupRoot := filepath.Join(tmpDir, "target", group)
	tagProjection := []model.TagProjection{
		{Family: "fa", Names: []string{"status"}},
		{Family: "fb", Names: []string{"service", "code"}},
	}

	res, err := directCopyStreamGroup(
		context.Background(),
		"entry [1/1]",
		group,
		StreamDirectCopyEntry{Stage: StreamStageHot, Target: dstGroupRoot},
		dstGroupRoot,
		ir, tagProjection,
		tasks, "",
		indexLocators,
	)
	require.NoError(t, err)
	require.EqualValues(t, srcRows, res.Rows, "stream invariant: target rows == source rows")

	d1Seg := formatStreamDirectCopySegName(day1, storage.DAY)
	d2Seg := formatStreamDirectCopySegName(day2, storage.DAY)
	require.NotEqual(t, d1Seg, d2Seg)

	d1Rows, _, vErr := StreamVerifyShardParts(filepath.Join(dstGroupRoot, d1Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	d2Rows, _, vErr := StreamVerifyShardParts(filepath.Join(dstGroupRoot, d2Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	require.EqualValues(t, 4, d1Rows, "day1 target must hold 4 elements (2 per stream)")
	require.EqualValues(t, 4, d2Rows, "day2 target must hold 4 elements (2 per stream)")
	require.EqualValues(t, srcRows, d1Rows+d2Rows, "no loss across target segs")

	assertIdxTerm := func(seg string, ruleID uint32, seriesID common.SeriesID, value string, want []uint64) {
		idxPath := filepath.Join(dstGroupRoot, seg, shardN, directStreamCopyIdxDirName)
		info, statErr := os.Stat(idxPath)
		require.NoError(t, statErr, "target seg %s must have an idx/", seg)
		require.True(t, info.IsDir())
		store, openErr := inverted.NewStore(inverted.StoreOpts{Path: idxPath, BatchWaitSec: 0})
		require.NoError(t, openErr)
		defer func() { require.NoError(t, store.Close()) }()
		list, _, matchErr := store.MatchTerms(index.NewStringField(index.FieldKey{
			IndexRuleID: ruleID,
			SeriesID:    seriesID,
		}, value))
		require.NoError(t, matchErr)
		got := map[uint64]struct{}{}
		iter := list.Iterator()
		for iter.Next() {
			got[iter.Current()] = struct{}{}
		}
		require.NoError(t, iter.Close())
		require.Lenf(t, got, len(want), "seg %s rule %d value=%s wrong doc count", seg, ruleID, value)
		for _, id := range want {
			_, ok := got[id]
			require.Truef(t, ok, "seg %s rule %d value=%s missing elementID %d", seg, ruleID, value, id)
		}
	}

	// streamA "status": each seg holds only its own elements, no cross contamination.
	assertIdxTerm(d1Seg, statusRID, aSeriesID, "ok", []uint64{aD1ok})
	assertIdxTerm(d1Seg, statusRID, aSeriesID, "err", []uint64{aD1err})
	assertIdxTerm(d2Seg, statusRID, aSeriesID, "ok", []uint64{aD2ok})
	assertIdxTerm(d2Seg, statusRID, aSeriesID, "err", []uint64{aD2err})

	// streamB "code": likewise.
	assertIdxTerm(d1Seg, codeRID, bSeriesID, "200", []uint64{bD1ok})
	assertIdxTerm(d1Seg, codeRID, bSeriesID, "500", []uint64{bD1err})
	assertIdxTerm(d2Seg, codeRID, bSeriesID, "200", []uint64{bD2ok})
	assertIdxTerm(d2Seg, codeRID, bSeriesID, "500", []uint64{bD2err})

	// streamB indexed ENTITY tag "service" (value resolved from series index):
	// both rows of streamB in each seg carry service="svc-B".
	assertIdxTerm(d1Seg, serviceRID, bSeriesID, "svc-B", []uint64{bD1ok, bD1err})
	assertIdxTerm(d2Seg, serviceRID, bSeriesID, "svc-B", []uint64{bD2ok, bD2err})

	// Cross-stream isolation: streamA's seriesID must NOT appear under streamB's
	// rule and vice versa (different SeriesID in the FieldKey already enforces this,
	// but assert empties explicitly for the wrong (rule, series) combo).
	assertIdxTerm(d1Seg, codeRID, aSeriesID, "200", nil)
	assertIdxTerm(d1Seg, statusRID, bSeriesID, "ok", nil)

	require.GreaterOrEqual(t, res.Segments, 2, "expected at least two target segments")
	require.GreaterOrEqual(t, StreamSlowPathHits(), int64(1),
		"slow path must have been exercised at least once for a cross-segment source part")
}
