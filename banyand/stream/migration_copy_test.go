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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// TestMigrationFastPath builds a minimal source tree (one group, one segment,
// one shard, three elements) using the internal helpers, then exercises the
// fast-path copy (all rows align to one target segment) and asserts:
//   - srcRows == tgtRows (stream invariant: no dedup)
//   - target shard has exactly one .snp file
//   - target segment has a valid metadata file with endTime
//
// Note: the test does NOT build a real schema-property catalog or a real bluge
// element index (idx/) — creating those requires the full write-path with
// index rules. What's covered:
//   - part flush via BuildPartForDump + memPart (fast-path data copy)
//   - writeStreamDirectCopySnp / writeStreamDirectCopySegmentMetadata
//   - StreamVerifyShardParts row count assertion
//   - streamSegStateRegistry registration + snapshot
//
// What's NOT covered (stubbed/unverified):
//   - Bluge element index rebuild on slow path (requires index rules + entity resolution)
//   - Series index union (requires real bluge sidx in source)
//   - End-to-end StreamMigrationCopy (requires full schema-property catalog)
func TestMigrationFastPath(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group  = "sw_segment"
		shardN = "shard-0"
	)

	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	segStart := ir.Standard(time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC))
	segName := formatStreamDirectCopySegName(segStart, storage.DAY)

	// Source layout: <tmpDir>/source/<group>/<segName>/<shardN>/<partID>/
	sourceGroupRoot := filepath.Join(tmpDir, "source", group)
	shardDir := filepath.Join(sourceGroupRoot, segName, shardN)
	require.NoError(t, os.MkdirAll(shardDir, storage.DirPerm))

	fileSystem := fs.NewLocalFileSystem()
	noFsync := nofsyncStreamFS{FileSystem: fileSystem}

	// Write three elements into a single part under the shard dir.
	ts0 := segStart.Add(1 * time.Hour).UnixNano()
	ts1 := segStart.Add(2 * time.Hour).UnixNano()
	ts2 := segStart.Add(3 * time.Hour).UnixNano()
	rows := []DumpRow{
		{
			Entity: "entity-A", Timestamp: ts0, ElementID: 100,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "alpha"}},
		},
		{
			Entity: "entity-A", Timestamp: ts1, ElementID: 101,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "alpha"}},
		},
		{
			Entity: "entity-B", Timestamp: ts2, ElementID: 200,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "beta"}},
		},
	}
	const srcPartID = uint64(1)
	_, rows, cleanup := BuildPartForDump(shardDir, noFsync, srcPartID, rows)
	defer cleanup()
	srcRows := uint64(len(rows))

	// Write .snp for the source shard (so StreamVerifyShardParts works on src too).
	_, err := writeStreamDirectCopySnp(shardDir, []string{fmt.Sprintf("%016x", srcPartID)})
	require.NoError(t, err)

	// Write source segment metadata.
	_, err = writeStreamDirectCopySegmentMetadata(
		filepath.Join(sourceGroupRoot, segName),
		ir.NextTime(segStart),
	)
	require.NoError(t, err)

	// Verify source row count before copy.
	srcVerifiedRows, srcParts, srcVerErr := StreamVerifyShardParts(shardDir, fileSystem)
	require.NoError(t, srcVerErr)
	require.EqualValues(t, srcRows, srcVerifiedRows, "source rows must match written count")
	require.Equal(t, 1, srcParts)

	// Read source part metadata to confirm single-segment alignment (fast path).
	partIDStr := fmt.Sprintf("%016x", srcPartID)
	partDir := filepath.Join(shardDir, partIDStr)
	var srcMeta partMetadata
	srcMeta.mustReadMetadata(noFsync, partDir)
	require.EqualValues(t, 3, srcMeta.TotalCount)

	alignedMin := ir.Standard(time.Unix(0, srcMeta.MinTimestamp))
	alignedMax := ir.Standard(time.Unix(0, srcMeta.MaxTimestamp))
	require.Equal(t, alignedMin, alignedMax,
		"all timestamps land in the same segment — fast path must apply")

	// Target layout.
	dstRoot := filepath.Join(tmpDir, "target", group)
	require.NoError(t, os.MkdirAll(dstRoot, storage.DirPerm))

	var partIDGen atomic.Uint64
	flushCh := make(chan streamFlushJob, 2)
	close(flushCh) // fast path never sends to flushCh
	segReg := newStreamSegStateRegistry()

	alignedSegName := formatStreamDirectCopySegName(alignedMin, storage.DAY)
	pr, copyErr := fastCopyOneStreamPart(
		streamProcessPartInput{
			ir:           ir,
			fileSystem:   noFsync,
			shardName:    shardN,
			shardDir:     shardDir,
			partIDStr:    partIDStr,
			dstGroupRoot: dstRoot,
			segStates:    segReg,
			partIDGen:    &partIDGen,
			flushCh:      flushCh,
		},
		partDir,
		alignedSegName,
		alignedMin,
		srcMeta.TotalCount,
	)
	require.NoError(t, copyErr)
	require.EqualValues(t, srcRows, pr.rows, "fast-path must preserve all rows")
	require.Equal(t, 1, pr.targetParts)

	// Finalize: write metadata + snp for every aligned segment in the registry.
	snap := segReg.snapshot()
	require.Len(t, snap, 1, "exactly one aligned target segment expected")

	for alignedSeg, ss := range snap {
		segDir := filepath.Join(dstRoot, alignedSeg)
		endTime := ir.NextTime(ss.alignedTime)

		_, err := writeStreamDirectCopySegmentMetadata(segDir, endTime)
		require.NoError(t, err)

		for shName, pids := range ss.shards {
			names := make([]string, len(pids))
			for i, pid := range pids {
				names[i] = fmt.Sprintf("%016x", pid)
			}
			_, snpErr := writeStreamDirectCopySnp(filepath.Join(segDir, shName), names)
			require.NoError(t, snpErr)
		}

		// Assert metadata file content.
		metaPath := filepath.Join(segDir, storage.SegmentMetadataFilename)
		require.FileExists(t, metaPath)
		raw, readErr := os.ReadFile(metaPath)
		require.NoError(t, readErr)
		var segMeta storage.SegmentMetadata
		require.NoError(t, json.Unmarshal(raw, &segMeta))
		require.NotEmpty(t, segMeta.EndTime, "segment metadata must carry endTime")

		// Assert exactly one .snp under each shard.
		for shName := range ss.shards {
			shardTarget := filepath.Join(segDir, shName)
			entries, rdErr := os.ReadDir(shardTarget)
			require.NoError(t, rdErr)
			snpCount := 0
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), directStreamCopySnpSuffix) {
					snpCount++
				}
			}
			require.Equal(t, 1, snpCount, "target shard must have exactly one .snp file")
		}

		// Assert target row count == source row count (stream no-dedup invariant).
		for shName := range ss.shards {
			shardTarget := filepath.Join(segDir, shName)
			tgtRows, tgtParts, vErr := StreamVerifyShardParts(shardTarget, fileSystem)
			require.NoError(t, vErr)
			require.EqualValues(t, srcRows, tgtRows,
				"stream invariant violated: target rows must equal source rows")
			require.Equal(t, 1, tgtParts, "expected one target part")
		}
	}
}
