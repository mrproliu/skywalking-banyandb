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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blugelabs/bluge"
	blugesearch "github.com/blugelabs/bluge/search"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// directStreamCopyDayFormat / directStreamCopyHourFormat mirror segmentController.format.
const (
	directStreamCopyDayFormat   = "20060102"
	directStreamCopyHourFormat  = "2006010215"
	directStreamCopySegPrefix   = "seg-"
	directStreamCopyShardPrefix = "shard-"
	directStreamCopySidxDirName = "sidx"
	directStreamCopyIdxDirName  = "idx"
	directStreamCopySnpSuffix   = ".snp"
)

var directStreamCopyPartDirPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// nofsyncStreamFS wraps a local FileSystem and skips per-file fsync calls.
type nofsyncStreamFS struct {
	fs.FileSystem
}

func (f nofsyncStreamFS) Write(buffer []byte, name string, permission fs.Mode) (int, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(permission))
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", name, err)
	}
	n, err := file.Write(buffer)
	if cerr := file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (f nofsyncStreamFS) WriteAtomic(buffer []byte, name string, permission fs.Mode) (int, error) {
	return f.Write(buffer, name, permission)
}

func (nofsyncStreamFS) SyncPath(string) {}

// StreamFastPathHits / StreamSlowPathHits expose copy-path instrumentation to the CLI.
func StreamFastPathHits() int64 { return streamFastPathHits.Load() }

// StreamSlowPathHits returns the number of source parts that went through the slow path.
func StreamSlowPathHits() int64 { return streamSlowPathHits.Load() }

// StreamSlowPathRows returns the total source-row count across slow-path parts.
func StreamSlowPathRows() int64 { return streamSlowPathRows.Load() }

var (
	streamFastPathHits atomic.Int64
	streamSlowPathHits atomic.Int64
	streamSlowPathRows atomic.Int64
)

// StreamDirectCopyConfig configures a one-shot direct-file copy migration of
// stream data from a backup snapshot into one or more local target roots.
type StreamDirectCopyConfig struct {
	BackupDir          string
	Date               string
	SchemaPropertyPath string
	SidxStagingDir     string
	Groups             []string
	Entries            []StreamDirectCopyEntry
}

// StreamDirectCopyEntry names one fan-out destination for stream data.
type StreamDirectCopyEntry struct {
	Stage  string
	Target string
	Source []string
	Nodes  []string
}

// StreamDirectCopyResult summarizes one StreamMigrationCopy invocation.
type StreamDirectCopyResult struct {
	Duration    time.Duration
	Rows        int64
	Bytes       int64
	SourceParts int
	TargetParts int
	Segments    int
}

// StreamMigrationCopy is the package-public entry point for stream migration copy.
//
// Flow:
//  1. Load schemas + group resource opts from the backup's schema-property catalog.
//  2. Build one union sidx per group (Phase A).
//  3. For each (entry, group): align rows by stage's SegmentInterval and write parts
//     under the entry's target group root; byte-copy idx/ on fast path; broadcast union sidx.
func StreamMigrationCopy(ctx context.Context, cfg StreamDirectCopyConfig) (StreamDirectCopyResult, error) {
	var res StreamDirectCopyResult
	if err := validateStreamDirectCopyConfig(&cfg); err != nil {
		return res, err
	}
	if cfg.SidxStagingDir == "" {
		return res, errors.New("sidxStagingDir is required (caller owns its lifecycle)")
	}
	streamFastPathHits.Store(0)
	streamSlowPathHits.Store(0)
	streamSlowPathRows.Store(0)
	start := time.Now()

	logStreamStep("loading stream schemas")
	//nolint:contextcheck // bluge reader.Search inside walkStreamSchemaPropertyShard already uses its own context.
	schemas, err := loadStreamSchemasFromSchemaCatalog(cfg.BackupDir, cfg.Date, cfg.SchemaPropertyPath, cfg.Groups)
	if err != nil {
		return res, fmt.Errorf("load stream schemas: %w", err)
	}
	for _, g := range cfg.Groups {
		if len(schemas[g]) == 0 {
			return res, fmt.Errorf("group %q has no streams in backup schema-property catalog — typo in groups or empty group?", g)
		}
	}

	//nolint:contextcheck // bluge reader.Search inside walkStreamSchemaPropertyShard already uses its own context.
	resourceOpts, err := loadGroupResourceOptsFromStreamSchema(cfg.BackupDir, cfg.Date, cfg.SchemaPropertyPath, cfg.Groups)
	if err != nil {
		return res, fmt.Errorf("load group resource opts: %w", err)
	}

	//nolint:contextcheck // bluge reader.Search inside walkStreamSchemaPropertyShard already uses its own context.
	indexLocators, err := loadStreamIndexLocators(cfg.BackupDir, cfg.Date, cfg.SchemaPropertyPath, cfg.Groups)
	if err != nil {
		return res, fmt.Errorf("load stream index locators: %w", err)
	}

	for entryIdx, entry := range cfg.Entries {
		for _, group := range cfg.Groups {
			opts, ok := resourceOpts[group]
			if !ok || opts == nil {
				return res, fmt.Errorf("group %s: ResourceOpts not found in backup schema-property catalog", group)
			}
			if _, err := resolveStreamStageInterval(opts, entry.Stage); err != nil {
				return res, fmt.Errorf("entry %d (target=%s) group %s: %w",
					entryIdx, entry.Target, group, err)
			}
		}
	}

	// Phase A: build one union sidx per group.
	groupUnionSidx := make(map[string]string, len(cfg.Groups))
	for _, group := range cfg.Groups {
		if ctx.Err() != nil {
			res.Duration = time.Since(start)
			return res, ctx.Err()
		}
		srcRoots := collectAllStreamSrcGroupRoots(cfg, group)
		if len(srcRoots) == 0 {
			logStreamStep("group %s: no source dirs across any entry — skipping union sidx build", group)
			continue
		}
		logStreamStep("group %s: building union sidx from %d source dir(s) merged across all entries",
			group, len(srcRoots))
		unionSidxPath, buildErr := buildStreamGroupUnionSidx(ctx, srcRoots,
			filepath.Join(cfg.SidxStagingDir, "groups", group, directStreamCopySidxDirName))
		if buildErr != nil {
			res.Duration = time.Since(start)
			return res, fmt.Errorf("group %s: build union sidx: %w", group, buildErr)
		}
		groupUnionSidx[group] = unionSidxPath
	}

	// Phase B: per entry × group, align source rows and write parts.
	for entryIdx, entry := range cfg.Entries {
		if ctx.Err() != nil {
			res.Duration = time.Since(start)
			return res, ctx.Err()
		}
		entryTag := fmt.Sprintf("entry [%d/%d]", entryIdx+1, len(cfg.Entries))
		logStreamStep("%s stage=%s nodes=%v target=%s",
			entryTag, entry.Stage, entry.Nodes, entry.Target)
		for _, group := range cfg.Groups {
			if ctx.Err() != nil {
				res.Duration = time.Since(start)
				return res, ctx.Err()
			}
			srcRoots := resolveStreamEntrySrcRoots(cfg, entry, group)
			if len(srcRoots) == 0 {
				if len(entry.Source) > 0 {
					logStreamStep("%s stage=%s group %s: skipped (none of source paths %v contain this group)",
						entryTag, entry.Stage, group, entry.Source)
				} else {
					logStreamStep("%s stage=%s group %s: skipped (none of nodes %v carry this group)",
						entryTag, entry.Stage, group, entry.Nodes)
				}
				continue
			}
			tasks, err := discoverStreamPartTasks(ctx, srcRoots)
			if err != nil {
				res.Duration = time.Since(start)
				return res, fmt.Errorf("%s stage=%s group %s: discover parts: %w",
					entryTag, entry.Stage, group, err)
			}
			if len(tasks) == 0 {
				logStreamStep("%s stage=%s group %s: skipped (no parts in selected nodes)",
					entryTag, entry.Stage, group)
				continue
			}
			opts, ok := resourceOpts[group]
			if !ok || opts == nil {
				return res, fmt.Errorf("group %s: ResourceOpts not found in backup schema-property catalog", group)
			}
			ir, err := resolveStreamStageInterval(opts, entry.Stage)
			if err != nil {
				return res, fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
			}
			unionSidxPath := groupUnionSidx[group]
			targetGroupRoot := filepath.Join(entry.Target, group)
			logStreamStep("%s stage=%s group %s: %d source dir(s), %d parts, interval=%v×%d — writing target (target=%q, union sidx=%q)",
				entryTag, entry.Stage, group, len(srcRoots), len(tasks), ir.Unit, ir.Num,
				targetGroupRoot, unionSidxPath)
			tagProjection := buildStreamTagProjectionFromGroupSchemas(schemas[group])
			groupRes, err := directCopyStreamGroup(ctx,
				entryTag,
				group, entry,
				targetGroupRoot,
				ir, tagProjection,
				tasks, unionSidxPath,
				indexLocators[group])
			if err != nil {
				res.Duration = time.Since(start)
				return res, fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
			}
			logStreamStep("%s stage=%s group %s: done rows=%d segs=%d srcParts=%d targetParts=%d",
				entryTag, entry.Stage, group,
				groupRes.Rows, groupRes.Segments, groupRes.SourceParts, groupRes.TargetParts)
			debug.FreeOSMemory()
			res.SourceParts += groupRes.SourceParts
			res.TargetParts += groupRes.TargetParts
			res.Rows += groupRes.Rows
			res.Bytes += groupRes.Bytes
			res.Segments += groupRes.Segments
		}
	}
	res.Duration = time.Since(start)
	return res, nil
}

func logStreamStep(format string, args ...any) {
	fmt.Fprintf(os.Stdout, time.Now().Format("2006/01/02 15:04:05")+" [migration/stream] "+format+"\n", args...)
}

// StreamStageHot is the conventional sentinel for "use the group's default SegmentInterval".
const StreamStageHot = "hot"

func resolveStreamStageInterval(opts *commonv1.ResourceOpts, stage string) (storage.IntervalRule, error) {
	var zero storage.IntervalRule
	if stage == StreamStageHot {
		if opts.GetSegmentInterval() == nil {
			return zero, fmt.Errorf("stage %q maps to default SegmentInterval but the group has none", stage)
		}
		return streamIntervalRuleFromProto(opts.GetSegmentInterval())
	}
	for _, st := range opts.GetStages() {
		if st.GetName() == stage {
			if st.GetSegmentInterval() == nil {
				return zero, fmt.Errorf("stage %q has no segmentInterval", stage)
			}
			return streamIntervalRuleFromProto(st.GetSegmentInterval())
		}
	}
	available := make([]string, 0, len(opts.GetStages())+1)
	available = append(available, StreamStageHot+" (default)")
	for _, st := range opts.GetStages() {
		available = append(available, st.GetName())
	}
	return zero, fmt.Errorf("stage %q not found in group schema; available: %s",
		stage, strings.Join(available, ", "))
}

func streamIntervalRuleFromProto(ir *commonv1.IntervalRule) (storage.IntervalRule, error) {
	var zero storage.IntervalRule
	if ir.GetNum() <= 0 {
		return zero, fmt.Errorf("segmentInterval.num must be > 0, got %d", ir.GetNum())
	}
	switch ir.GetUnit() {
	case commonv1.IntervalRule_UNIT_DAY:
		return storage.IntervalRule{Unit: storage.DAY, Num: int(ir.GetNum())}, nil
	case commonv1.IntervalRule_UNIT_HOUR:
		return storage.IntervalRule{Unit: storage.HOUR, Num: int(ir.GetNum())}, nil
	}
	return zero, fmt.Errorf("unsupported segmentInterval.unit %v", ir.GetUnit())
}

func resolveStreamEntrySrcRoots(cfg StreamDirectCopyConfig, entry StreamDirectCopyEntry, group string) []string {
	if len(entry.Source) > 0 {
		var roots []string
		for _, p := range entry.Source {
			candidate := filepath.Join(p, group)
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				roots = append(roots, candidate)
			}
		}
		sort.Strings(roots)
		return roots
	}
	return collectStreamEntrySrcGroupRoots(cfg.BackupDir, cfg.Date, group, entry.Nodes)
}

// collectStreamEntrySrcGroupRoots returns `<backupDir>/<node>/<date>/stream/<group>` directories.
func collectStreamEntrySrcGroupRoots(backupDir, date, group string, nodes []string) []string {
	var roots []string
	for _, node := range nodes {
		if node == "" {
			continue
		}
		candidate := filepath.Join(backupDir, node, date, "stream", group)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			roots = append(roots, candidate)
		}
	}
	sort.Strings(roots)
	return roots
}

func collectAllStreamSrcGroupRoots(cfg StreamDirectCopyConfig, group string) []string {
	seen := make(map[string]struct{})
	var roots []string
	for _, entry := range cfg.Entries {
		for _, r := range resolveStreamEntrySrcRoots(cfg, entry, group) {
			if _, dup := seen[r]; dup {
				continue
			}
			seen[r] = struct{}{}
			roots = append(roots, r)
		}
	}
	sort.Strings(roots)
	return roots
}

func validateStreamDirectCopyConfig(cfg *StreamDirectCopyConfig) error {
	if cfg.SchemaPropertyPath == "" {
		if cfg.BackupDir == "" {
			return errors.New("backupDir is required when SchemaPropertyPath is unset")
		}
		if cfg.Date == "" {
			return errors.New("date is required when SchemaPropertyPath is unset")
		}
	}
	if len(cfg.Groups) == 0 {
		return errors.New("groups is required")
	}
	if len(cfg.Entries) == 0 {
		return errors.New("entries is required")
	}
	seenTarget := make(map[string]int, len(cfg.Entries))
	for i, e := range cfg.Entries {
		if e.Stage == "" {
			return fmt.Errorf("entries[%d].Stage is required", i)
		}
		if e.Target == "" {
			return fmt.Errorf("entries[%d].Target is required", i)
		}
		if len(e.Source) == 0 && len(e.Nodes) == 0 {
			return fmt.Errorf("entries[%d]: at least one of Source or Nodes must be set", i)
		}
		if prev, dup := seenTarget[e.Target]; dup {
			return fmt.Errorf("entries[%d].Target %q duplicates Entries[%d].Target", i, e.Target, prev)
		}
		seenTarget[e.Target] = i
	}
	return nil
}

// StreamResolveEntrySrcRoots is the exported wrapper so cmd/migration can use it.
func StreamResolveEntrySrcRoots(cfg StreamDirectCopyConfig, entry StreamDirectCopyEntry, group string) []string {
	return resolveStreamEntrySrcRoots(cfg, entry, group)
}

// ── Part discovery ────────────────────────────────────────────────.

type streamPartTask struct {
	srcSegName string
	shardName  string
	shardDir   string
	partIDStr  string
}

func discoverStreamPartTasks(ctx context.Context, srcGroupRoots []string) ([]streamPartTask, error) {
	var tasks []streamPartTask
	for _, srcGroupRoot := range srcGroupRoots {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		segEntries, err := os.ReadDir(srcGroupRoot)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", srcGroupRoot, err)
		}
		for _, segE := range segEntries {
			if !segE.IsDir() || !strings.HasPrefix(segE.Name(), directStreamCopySegPrefix) {
				continue
			}
			srcSegName := segE.Name()
			srcSegDir := filepath.Join(srcGroupRoot, srcSegName)
			shardEntries, err := os.ReadDir(srcSegDir)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", srcSegDir, err)
			}
			for _, shardE := range shardEntries {
				if !shardE.IsDir() || !strings.HasPrefix(shardE.Name(), directStreamCopyShardPrefix) {
					continue
				}
				shardName := shardE.Name()
				shardDir := filepath.Join(srcSegDir, shardName)
				partEntries, err := os.ReadDir(shardDir)
				if err != nil {
					return nil, fmt.Errorf("read %s: %w", shardDir, err)
				}
				for _, partE := range partEntries {
					if !partE.IsDir() || !directStreamCopyPartDirPattern.MatchString(partE.Name()) {
						continue
					}
					tasks = append(tasks, streamPartTask{
						srcSegName: srcSegName,
						shardName:  shardName,
						shardDir:   shardDir,
						partIDStr:  partE.Name(),
					})
				}
			}
		}
	}
	return tasks, nil
}

// ── Segment state registry ─────────────────────────────────────────────.

type streamSegStateForGroup struct {
	alignedTime time.Time
	shards      map[string][]uint64 // shard-N -> partIDs
}

type streamSegStateRegistry struct {
	m  map[string]*streamSegStateForGroup
	mu sync.Mutex
}

func newStreamSegStateRegistry() *streamSegStateRegistry {
	return &streamSegStateRegistry{m: map[string]*streamSegStateForGroup{}}
}

func (r *streamSegStateRegistry) register(alignedSegName string, alignedTime time.Time, shardName string, partID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ss, ok := r.m[alignedSegName]
	if !ok {
		ss = &streamSegStateForGroup{
			alignedTime: alignedTime,
			shards:      map[string][]uint64{},
		}
		r.m[alignedSegName] = ss
	}
	ss.shards[shardName] = append(ss.shards[shardName], partID)
}

func (r *streamSegStateRegistry) snapshot() map[string]*streamSegStateForGroup {
	return r.m
}

// ── Aligned segment cache ──────────────────────────────────────────────.

type streamAlignedSegCache struct {
	name       string
	startNanos int64
	endNanos   int64
}

func (c *streamAlignedSegCache) segNameFor(ir storage.IntervalRule, ts int64) string {
	if c.name != "" && ts >= c.startNanos && ts < c.endNanos {
		return c.name
	}
	start := ir.Standard(time.Unix(0, ts))
	c.name = formatStreamDirectCopySegName(start, ir.Unit)
	c.startNanos = start.UnixNano()
	c.endNanos = ir.NextTime(start).UnixNano()
	return c.name
}

// ── Tag projection ────────────────────────────────────────────────.

func buildStreamTagProjectionFromGroupSchemas(schemas map[string]*streamSchemaInfo) []model.TagProjection {
	if len(schemas) == 0 {
		return nil
	}
	families := map[string]map[string]bool{}
	for _, s := range schemas {
		for _, tf := range s.TagFamilies {
			if families[tf.Name] == nil {
				families[tf.Name] = map[string]bool{}
			}
			for _, t := range tf.Tags {
				families[tf.Name][t] = true
			}
		}
	}
	out := make([]model.TagProjection, 0, len(families))
	famNames := make([]string, 0, len(families))
	for name := range families {
		famNames = append(famNames, name)
	}
	sort.Strings(famNames)
	for _, name := range famNames {
		tagSet := families[name]
		tags := make([]string, 0, len(tagSet))
		for t := range tagSet {
			tags = append(tags, t)
		}
		sort.Strings(tags)
		out = append(out, model.TagProjection{Family: name, Names: tags})
	}
	return out
}

// ── Main group copy driver ─────────────────────────────────────────────.

func directCopyStreamGroup(
	ctx context.Context,
	entryTag string,
	group string, entry StreamDirectCopyEntry, dstGroupRoot string,
	ir storage.IntervalRule, tagProjection []model.TagProjection,
	tasks []streamPartTask, unionSidxPath string,
	indexLocators map[string]*streamIndexLocator,
) (StreamDirectCopyResult, error) {
	var res StreamDirectCopyResult

	if err := directCopyStreamPrepareTarget(dstGroupRoot); err != nil {
		return res, err
	}

	segStates := newStreamSegStateRegistry()
	elementIdx := newStreamElementIndexRegistry()
	var partIDGen atomic.Uint64
	fileSystem := nofsyncStreamFS{FileSystem: fs.NewLocalFileSystem()}

	totalTasks := len(tasks)
	workerCount := runtime.NumCPU()
	if workerCount > totalTasks {
		workerCount = totalTasks
	}
	if workerCount < 1 {
		workerCount = 1
	}

	flushWorkerCount := workerCount
	flushCh := make(chan streamFlushJob, flushWorkerCount*2)
	var flushWg sync.WaitGroup
	for i := 0; i < flushWorkerCount; i++ {
		flushWg.Add(1)
		go func() {
			defer flushWg.Done()
			runStreamFlushWorker(flushCh, fileSystem)
		}()
	}
	defer func() {
		close(flushCh)
		flushWg.Wait()
	}()

	taskCh := make(chan streamPartTask, workerCount*2)
	var (
		wg             sync.WaitGroup
		resMu          sync.Mutex
		sourceParts    int
		targetParts    int
		rowsCopied     int64
		bytesWritten   int64
		firstErr       error
		errMu          sync.Mutex
		partsCompleted atomic.Int64
	)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decoder := &encoding.BytesBlockDecoder{}
			for task := range taskCh {
				if workerCtx.Err() != nil {
					return
				}
				partResult, err := func() (pr streamProcessPartResult, err error) {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic processing %s/%s/%s: %v",
								task.srcSegName, task.shardName, task.partIDStr, r)
						}
					}()
					return processOneStreamSourcePart(
						streamProcessPartInput{
							ir:            ir,
							decoder:       decoder,
							fileSystem:    fileSystem,
							tagProjection: tagProjection,
							srcSegName:    task.srcSegName,
							shardName:     task.shardName,
							shardDir:      task.shardDir,
							partIDStr:     task.partIDStr,
							dstGroupRoot:  dstGroupRoot,
							segStates:     segStates,
							elementIdx:    elementIdx,
							partIDGen:     &partIDGen,
							flushCh:       flushCh,
						},
					)
				}()
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancelWorkers()
					}
					errMu.Unlock()
					return
				}
				done := partsCompleted.Add(1)
				logStreamStep("%s stage=%s group %s: part %d/%d (%.1f%%) done %s/%s/%s rows=%d targetParts=%d",
					entryTag, entry.Stage, group, done, totalTasks,
					100.0*float64(done)/float64(totalTasks),
					task.srcSegName, task.shardName, task.partIDStr,
					partResult.rows, partResult.targetParts)
				resMu.Lock()
				sourceParts++
				targetParts += partResult.targetParts
				rowsCopied += partResult.rows
				bytesWritten += partResult.bytes
				resMu.Unlock()
			}
		}()
	}

	for _, t := range tasks {
		select {
		case taskCh <- t:
		case <-workerCtx.Done():
		}
	}
	close(taskCh)
	wg.Wait()
	if firstErr != nil {
		return res, firstErr
	}
	res.SourceParts = sourceParts
	res.TargetParts = targetParts
	res.Rows = rowsCopied
	res.Bytes = bytesWritten

	unionSidxAvailable := false
	if unionSidxPath != "" {
		if info, statErr := os.Stat(unionSidxPath); statErr == nil && info.IsDir() {
			unionSidxAvailable = true
		}
	}
	segSnapshot := segStates.snapshot()
	logStreamStep("%s stage=%s group %s: finalizing %d aligned target segments (writing metadata + snp + union sidx)",
		entryTag, entry.Stage, group, len(segSnapshot))
	for alignedSeg, ss := range segSnapshot {
		res.Segments++
		segDir := filepath.Join(dstGroupRoot, alignedSeg)
		endTime := ir.NextTime(ss.alignedTime)
		metaBytes, err := writeStreamDirectCopySegmentMetadata(segDir, endTime)
		if err != nil {
			return res, fmt.Errorf("seg %s metadata: %w", alignedSeg, err)
		}
		res.Bytes += metaBytes
		for shardName, partIDs := range ss.shards {
			if partIDs != nil {
				sort.Slice(partIDs, func(i, j int) bool { return partIDs[i] < partIDs[j] })
				names := make([]string, len(partIDs))
				for i, pid := range partIDs {
					names[i] = fmt.Sprintf("%016x", pid)
				}
				snpBytes, err := writeStreamDirectCopySnp(
					filepath.Join(segDir, shardName), names,
				)
				if err != nil {
					return res, fmt.Errorf("seg %s shard %s snp: %w", alignedSeg, shardName, err)
				}
				res.Bytes += snpBytes
			}
		}
		if unionSidxAvailable {
			sidxBytes, err := directCopyStreamDir(
				unionSidxPath,
				filepath.Join(segDir, directStreamCopySidxDirName),
			)
			if err != nil {
				return res, fmt.Errorf("seg %s union sidx: %w", alignedSeg, err)
			}
			res.Bytes += sidxBytes
		}
	}

	// Element index (idx/) finalize: per source (seg, shard), byte-copy when its
	// rows landed in exactly one target seg, otherwise rebuild per target seg from
	// rows + index rules. Runs single-threaded (slow path is rare) so concurrent
	// writers never open the same target idx store.
	idxBytes, err := finalizeStreamElementIndex(ctx, finalizeStreamElementIndexInput{
		group:         group,
		dstGroupRoot:  dstGroupRoot,
		ir:            ir,
		fileSystem:    fileSystem,
		decoder:       &encoding.BytesBlockDecoder{},
		tagProjection: tagProjection,
		indexLocators: indexLocators,
		registry:      elementIdx,
	})
	if err != nil {
		return res, fmt.Errorf("finalize element index: %w", err)
	}
	res.Bytes += idxBytes

	return res, nil
}

// ── Per-part processing ──────────────────────────────────────────────.

//nolint:govet // internal-only helper, readability > minor padding savings
type streamProcessPartInput struct {
	fileSystem    fs.FileSystem
	decoder       *encoding.BytesBlockDecoder
	segStates     *streamSegStateRegistry
	elementIdx    *streamElementIndexRegistry
	partIDGen     *atomic.Uint64
	flushCh       chan<- streamFlushJob
	shardDir      string
	srcSegName    string
	shardName     string
	partIDStr     string
	dstGroupRoot  string
	tagProjection []model.TagProjection
	ir            storage.IntervalRule
}

type streamProcessPartResult struct {
	rows        int64
	bytes       int64
	targetParts int
}

func processOneStreamSourcePart(in streamProcessPartInput) (streamProcessPartResult, error) {
	var pr streamProcessPartResult
	srcPartID, err := strconv.ParseUint(in.partIDStr, 16, 64)
	if err != nil {
		return pr, fmt.Errorf("parse partID %s: %w", in.partIDStr, err)
	}
	srcPartDir := filepath.Join(in.shardDir, in.partIDStr)

	var srcMeta partMetadata
	srcMeta.mustReadMetadata(in.fileSystem, srcPartDir)

	alignedMin := in.ir.Standard(time.Unix(0, srcMeta.MinTimestamp))
	alignedMax := in.ir.Standard(time.Unix(0, srcMeta.MaxTimestamp))
	if alignedMin.Equal(alignedMax) {
		alignedSegName := formatStreamDirectCopySegName(alignedMin, in.ir.Unit)
		streamFastPathHits.Add(1)
		return fastCopyOneStreamPart(in, srcPartDir, alignedSegName, alignedMin, srcMeta.TotalCount)
	}

	streamSlowPathHits.Add(1)
	streamSlowPathRows.Add(int64(srcMeta.TotalCount))
	return slowCopyOneStreamPart(in, srcPartID)
}

// fastCopyOneStreamPart handles the case where every row in a source part lands
// in the same target aligned segment. Byte-copies part dir AND the shard's idx/.
func fastCopyOneStreamPart(
	in streamProcessPartInput,
	srcPartDir, alignedSegName string,
	alignedTime time.Time,
	totalCount uint64,
) (streamProcessPartResult, error) {
	var pr streamProcessPartResult
	targetPartID := in.partIDGen.Add(1)
	targetPartIDStr := fmt.Sprintf("%016x", targetPartID)
	dstPart := filepath.Join(in.dstGroupRoot, alignedSegName, in.shardName, targetPartIDStr)
	pr.rows = int64(totalCount)
	pr.targetParts = 1

	if err := os.MkdirAll(filepath.Dir(dstPart), storage.DirPerm); err != nil {
		return pr, fmt.Errorf("mkdir %s: %w", filepath.Dir(dstPart), err)
	}
	bytesCopied, err := directCopyStreamDir(srcPartDir, dstPart)
	if err != nil {
		return pr, fmt.Errorf("fast-copy %s -> %s: %w", srcPartDir, dstPart, err)
	}
	pr.bytes = bytesCopied

	in.segStates.register(alignedSegName, alignedTime, in.shardName, targetPartID)

	// Element index handling is decided per source (seg, shard) in a finalize step,
	// NOT per part: only record which target seg this part's rows landed in. If all
	// of this source (seg, shard)'s rows land in this one target seg, the finalize
	// step byte-copies the source idx/ faithfully; otherwise it rebuilds per target.
	if in.elementIdx != nil {
		in.elementIdx.recordTargetSeg(in.srcSegName, in.shardName, in.shardDir, alignedSegName)
	}
	return pr, nil
}

// ── Slow path ──────────────────────────────────────────────────.

const streamSlowCopyOnePartChunkRows = 50_000

// streamSlowCopyArena is a per-call slab for tagValue and tagValues backing arrays.
type streamSlowCopyArena struct {
	tvs      []tagValue
	tvPtrs   []*tagValue
	famSlots []tagValues
}

func (a *streamSlowCopyArena) reset() {
	a.tvs = a.tvs[:0]
	a.tvPtrs = a.tvPtrs[:0]
	a.famSlots = a.famSlots[:0]
}

func (a *streamSlowCopyArena) allocTagValue(tag string, value []byte, valueType pbv1.ValueType) *tagValue {
	if len(a.tvs)+1 > cap(a.tvs) {
		return &tagValue{tag: tag, value: value, valueType: valueType}
	}
	a.tvs = append(a.tvs, tagValue{tag: tag, value: value, valueType: valueType})
	return &a.tvs[len(a.tvs)-1]
}

func streamArenaTakePtrs(a *streamSlowCopyArena, n int) []*tagValue {
	if len(a.tvPtrs)+n > cap(a.tvPtrs) {
		return make([]*tagValue, n)
	}
	start := len(a.tvPtrs)
	a.tvPtrs = a.tvPtrs[:start+n]
	return a.tvPtrs[start : start+n : start+n]
}

func streamArenaTakeFamSlots(a *streamSlowCopyArena, n int) []tagValues {
	if len(a.famSlots)+n > cap(a.famSlots) {
		return make([]tagValues, n)
	}
	start := len(a.famSlots)
	a.famSlots = a.famSlots[:start+n]
	return a.famSlots[start : start+n : start+n]
}

var streamSlowCopyArenaPool = sync.Pool{
	New: func() any {
		const estColumnsPerRow = 8
		return &streamSlowCopyArena{
			tvs:      make([]tagValue, 0, streamSlowCopyOnePartChunkRows*estColumnsPerRow),
			tvPtrs:   make([]*tagValue, 0, streamSlowCopyOnePartChunkRows*estColumnsPerRow),
			famSlots: make([]tagValues, 0, streamSlowCopyOnePartChunkRows*2),
		}
	},
}

func acquireStreamSlowCopyArena() *streamSlowCopyArena {
	a := streamSlowCopyArenaPool.Get().(*streamSlowCopyArena)
	a.reset()
	return a
}

func releaseStreamSlowCopyArena(a *streamSlowCopyArena) {
	a.reset()
	streamSlowCopyArenaPool.Put(a)
}

// appendStreamBlockRowToBuckets routes one row to the correct per-aligned-segment bucket.
func appendStreamBlockRowToBuckets(
	ir storage.IntervalRule,
	b *block,
	seriesID common.SeriesID,
	k uint64,
	arena *streamSlowCopyArena,
	buckets map[string]*elements,
	segCache *streamAlignedSegCache,
) {
	ts := b.timestamps[k]
	alignedSegName := segCache.segNameFor(ir, ts)

	el, exists := buckets[alignedSegName]
	if !exists {
		el = generateElements()
		el.reset()
		buckets[alignedSegName] = el
	}
	el.seriesIDs = append(el.seriesIDs, seriesID)
	el.timestamps = append(el.timestamps, ts)
	el.elementIDs = append(el.elementIDs, b.elementIDs[k])

	rowTagFamilies := streamArenaTakeFamSlots(arena, len(b.tagFamilies))
	for fi := range b.tagFamilies {
		cf := &b.tagFamilies[fi]
		ptrs := streamArenaTakePtrs(arena, len(cf.tags))
		for ci := range cf.tags {
			c := &cf.tags[ci]
			var v []byte
			if uint64(len(c.values)) > k {
				v = c.values[k]
			}
			ptrs[ci] = arena.allocTagValue(c.name, v, c.valueType)
		}
		rowTagFamilies[fi] = tagValues{tag: cf.name, values: ptrs}
	}
	el.tagFamilies = append(el.tagFamilies, rowTagFamilies)
}

// slowCopyOneStreamPart handles the row-level rewrite path for stream parts.
// No dedup: stream preserves every element including same (seriesID, ts) with different elementID.
func slowCopyOneStreamPart(in streamProcessPartInput, srcPartID uint64) (streamProcessPartResult, error) {
	var pr streamProcessPartResult
	p := mustOpenFilePart(srcPartID, in.shardDir, in.fileSystem)
	defer p.close()

	flushBuckets := func(buckets map[string]*elements) error {
		if len(buckets) == 0 {
			return nil
		}
		var (
			chunkWg    sync.WaitGroup
			chunkErrMu sync.Mutex
			chunkErr   error
			chunkBytes atomic.Int64
		)
		setErr := func(e error) {
			chunkErrMu.Lock()
			if chunkErr == nil {
				chunkErr = e
			}
			chunkErrMu.Unlock()
		}
		addBytes := func(b int64) { chunkBytes.Add(b) }
		for alignedSegName, el := range buckets {
			if in.elementIdx != nil {
				in.elementIdx.recordTargetSeg(in.srcSegName, in.shardName, in.shardDir, alignedSegName)
			}
			chunkWg.Add(1)
			targetPartID := in.partIDGen.Add(1)
			targetPartIDStr := fmt.Sprintf("%016x", targetPartID)
			dstPart := filepath.Join(in.dstGroupRoot, alignedSegName, in.shardName, targetPartIDStr)
			in.flushCh <- streamFlushJob{
				el:           el,
				alignedSeg:   alignedSegName,
				dstPart:      dstPart,
				targetPartID: targetPartID,
				shardName:    in.shardName,
				ir:           in.ir,
				segStates:    in.segStates,
				chunkWg:      &chunkWg,
				setErr:       setErr,
				addBytes:     addBytes,
			}
		}
		chunkWg.Wait()
		pr.bytes += chunkBytes.Load()
		if chunkErr != nil {
			return chunkErr
		}
		pr.targetParts += len(buckets)
		return nil
	}

	buckets := map[string]*elements{}
	liveBlocks := make([]*block, 0, 8)
	chunkRows := 0
	arena := acquireStreamSlowCopyArena()
	defer releaseStreamSlowCopyArena(arena)
	var segCache streamAlignedSegCache

	releaseLive := func() {
		for _, b := range liveBlocks {
			releaseBlock(b)
		}
		liveBlocks = liveBlocks[:0]
	}
	defer releaseLive()
	defer func() {
		for _, el := range buckets {
			releaseElements(el)
		}
	}()

	flushChunk := func() error {
		if chunkRows == 0 {
			return nil
		}
		err := flushBuckets(buckets)
		buckets = map[string]*elements{}
		if err != nil {
			return err
		}
		releaseLive()
		arena.reset()
		chunkRows = 0
		return nil
	}

	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
	)
	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		compressed = bytes.ResizeOver(compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), compressed)
		var err error
		raw, err = zstd.Decompress(raw[:0], compressed)
		if err != nil {
			releaseLive()
			return pr, fmt.Errorf("decompress primary block: %w", err)
		}
		bms, err = unmarshalBlockMetadata(bms[:0], raw)
		if err != nil {
			releaseLive()
			return pr, fmt.Errorf("unmarshal block metadata: %w", err)
		}
		for j := range bms {
			bm := &bms[j]
			bm.tagProjection = in.tagProjection
			b := generateBlock()
			b.mustReadFrom(in.decoder, p, *bm)
			liveBlocks = append(liveBlocks, b)

			for k := uint64(0); k < bm.count; k++ {
				appendStreamBlockRowToBuckets(in.ir, b, bm.seriesID, k, arena, buckets, &segCache)
				pr.rows++
				chunkRows++
			}
			if chunkRows >= streamSlowCopyOnePartChunkRows {
				if err := flushChunk(); err != nil {
					return pr, err
				}
			}
		}
	}
	if err := flushChunk(); err != nil {
		return pr, err
	}
	return pr, nil
}

// ── Flush pool ─────────────────────────────────────────────────.

type streamFlushJob struct {
	el           *elements
	chunkWg      *sync.WaitGroup
	setErr       func(error)
	addBytes     func(int64)
	segStates    *streamSegStateRegistry
	alignedSeg   string
	dstPart      string
	shardName    string
	targetPartID uint64
	ir           storage.IntervalRule
}

func runStreamFlushWorker(flushCh <-chan streamFlushJob, fileSystem fs.FileSystem) {
	for job := range flushCh {
		sz, err := directCopyStreamFlushBucket(job.el, fileSystem, job.dstPart)
		if err != nil {
			job.setErr(fmt.Errorf("flush %s: %w", job.dstPart, err))
			job.chunkWg.Done()
			continue
		}
		job.addBytes(sz)
		alignedTime, err := parseStreamDirectCopySegStart(job.alignedSeg, job.ir.Unit)
		if err != nil {
			job.setErr(fmt.Errorf("parse seg start %s: %w", job.alignedSeg, err))
			job.chunkWg.Done()
			continue
		}
		job.segStates.register(job.alignedSeg, alignedTime, job.shardName, job.targetPartID)
		job.chunkWg.Done()
	}
}

func directCopyStreamFlushBucket(el *elements, fileSystem fs.FileSystem, partPath string) (sz int64, err error) {
	defer releaseElements(el)
	if mkErr := os.MkdirAll(filepath.Dir(partPath), storage.DirPerm); mkErr != nil {
		return 0, mkErr
	}
	mp := generateMemPart()
	defer releaseMemPart(mp)
	// mustInitFromElements / mustFlush panic on unrecoverable conditions (e.g. a
	// full disk). Convert the panic into an error so the flush pool reports it via
	// setErr and the run aborts gracefully (first error + staging cleanup) instead
	// of an unrecovered goroutine panic crashing the whole process.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("flush part %s panicked: %v", partPath, r)
		}
	}()
	mp.mustInitFromElements(el)
	mp.mustFlush(fileSystem, partPath)
	return int64(mp.partMetadata.CompressedSizeBytes), nil
}

// ── Series index union (sidx/) — identical logic to measure ─────────────────.

const (
	streamUnionSidxDocIDField     = "_id"
	streamUnionSidxTimestampField = "_timestamp"
	streamUnionSidxVersionField   = "_version"
	streamUnionSidxBatchSize      = 50000
)

// buildStreamGroupUnionSidx walks every srcGroupRoot/seg-*/sidx/ directory,
// scans series-index docs, deduplicates by SeriesID, and re-emits into stagingPath.
func buildStreamGroupUnionSidx(ctx context.Context, srcGroupRoots []string, stagingPath string) (string, error) {
	if err := os.MkdirAll(stagingPath, storage.DirPerm); err != nil {
		return "", fmt.Errorf("mkdir staging %q: %w", stagingPath, err)
	}

	writer, err := bluge.OpenWriter(bluge.DefaultConfig(stagingPath))
	if err != nil {
		return "", fmt.Errorf("open union sidx writer at %q: %w", stagingPath, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()

	var sidxPaths []string
	for _, srcGroupRoot := range srcGroupRoots {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		segEntries, err := os.ReadDir(srcGroupRoot)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("read src group root %q: %w", srcGroupRoot, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directStreamCopySegPrefix) {
				continue
			}
			srcSidxPath := filepath.Join(srcGroupRoot, se.Name(), directStreamCopySidxDirName)
			info, statErr := os.Stat(srcSidxPath)
			if statErr != nil || !info.IsDir() {
				continue
			}
			sidxPaths = append(sidxPaths, srcSidxPath)
		}
	}

	seen := make(map[common.SeriesID]struct{}, 1_000_000)
	var seenMu sync.Mutex
	var writerMu sync.Mutex
	var insertedAtomic atomic.Int64
	var firstErr atomic.Pointer[error]

	workerCount := runtime.NumCPU()
	if workerCount > len(sidxPaths) {
		workerCount = len(sidxPaths)
	}
	if workerCount < 1 {
		workerCount = 1
	}

	pathCh := make(chan string)
	var wg sync.WaitGroup
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for srcSidxPath := range pathCh {
				if workerCtx.Err() != nil {
					return
				}
				count, mergeErr := mergeOneStreamSourceSidxInto(workerCtx, srcSidxPath, writer, seen, &seenMu, &writerMu)
				if mergeErr != nil {
					e := fmt.Errorf("merge %s: %w", srcSidxPath, mergeErr)
					if firstErr.CompareAndSwap(nil, &e) {
						cancelWorkers()
					}
					return
				}
				insertedAtomic.Add(int64(count))
			}
		}()
	}
	for _, p := range sidxPaths {
		select {
		case pathCh <- p:
		case <-workerCtx.Done():
		}
	}
	close(pathCh)
	wg.Wait()
	if errPtr := firstErr.Load(); errPtr != nil {
		return "", *errPtr
	}
	inserted := int(insertedAtomic.Load())

	closed = true
	if closeErr := writer.Close(); closeErr != nil {
		return "", fmt.Errorf("close union sidx writer: %w", closeErr)
	}
	if inserted == 0 {
		_ = os.RemoveAll(stagingPath)
		return "", nil
	}
	return stagingPath, nil
}

func mergeOneStreamSourceSidxInto(
	ctx context.Context,
	srcPath string,
	dst *bluge.Writer,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
	writerMu *sync.Mutex,
) (int, error) {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(srcPath))
	if err != nil {
		if strings.Contains(err.Error(), "unable to find a usable snapshot") {
			return 0, nil
		}
		return 0, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	dmi, err := reader.Search(ctx, bluge.NewAllMatches(bluge.NewMatchAllQuery()))
	if err != nil {
		return 0, fmt.Errorf("search: %w", err)
	}

	batch := bluge.NewBatch()
	batched := 0
	inserted := 0

	flush := func() error {
		writerMu.Lock()
		defer writerMu.Unlock()
		return dst.Batch(batch)
	}

	for {
		next, nextErr := dmi.Next()
		if nextErr != nil {
			return inserted, fmt.Errorf("iterate docs: %w", nextErr)
		}
		if next == nil {
			break
		}
		doc, dup, buildErr := buildStreamDocFromMatchLocked(next, seen, seenMu)
		if buildErr != nil {
			return inserted, buildErr
		}
		if dup || doc == nil {
			continue
		}
		batch.Insert(doc)
		batched++
		inserted++
		if batched >= streamUnionSidxBatchSize {
			if err := flush(); err != nil {
				return inserted, fmt.Errorf("flush batch: %w", err)
			}
			batch = bluge.NewBatch()
			batched = 0
		}
	}
	if batched > 0 {
		if err := flush(); err != nil {
			return inserted, fmt.Errorf("flush tail batch: %w", err)
		}
	}
	return inserted, nil
}

func buildStreamDocFromMatchLocked(
	match *blugesearch.DocumentMatch,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
) (*bluge.Document, bool, error) {
	var entityValues []byte
	type storedField struct {
		name  string
		value []byte
	}
	var fields []storedField
	visitErr := match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case streamUnionSidxDocIDField:
			entityValues = append([]byte(nil), value...)
		default:
			fields = append(fields, storedField{
				name:  field,
				value: append([]byte(nil), value...),
			})
		}
		return true
	})
	if visitErr != nil {
		return nil, false, fmt.Errorf("visit stored fields: %w", visitErr)
	}
	if len(entityValues) == 0 {
		return nil, false, nil
	}
	var series pbv1.Series
	if err := series.Unmarshal(entityValues); err != nil {
		return nil, false, nil
	}
	seenMu.Lock()
	if _, dup := seen[series.ID]; dup {
		seenMu.Unlock()
		return nil, true, nil
	}
	seen[series.ID] = struct{}{}
	seenMu.Unlock()

	doc := bluge.NewDocument(string(entityValues))
	for _, f := range fields {
		switch f.name {
		case streamUnionSidxTimestampField:
			ts, decErr := bluge.DecodeDateTime(f.value)
			if decErr != nil {
				return nil, false, fmt.Errorf("decode timestamp on series %d: %w", series.ID, decErr)
			}
			doc.AddField(bluge.NewDateTimeField(f.name, ts).StoreValue())
		case streamUnionSidxVersionField:
			doc.AddField(bluge.NewStoredOnlyField(f.name, f.value))
		default:
			doc.AddField(bluge.NewKeywordFieldBytes(f.name, f.value).StoreValue())
		}
	}
	return doc, false, nil
}

// ── Utility helpers ───────────────────────────────────────────────.

func directCopyStreamPrepareTarget(dstGroupRoot string) error {
	entries, err := os.ReadDir(dstGroupRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dstGroupRoot, storage.DirPerm)
		}
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("target %s is not empty; remove it before re-running", dstGroupRoot)
	}
	return nil
}

func formatStreamDirectCopySegName(t time.Time, unit storage.IntervalUnit) string {
	switch unit {
	case storage.HOUR:
		return directStreamCopySegPrefix + t.Format(directStreamCopyHourFormat)
	case storage.DAY:
		return directStreamCopySegPrefix + t.Format(directStreamCopyDayFormat)
	}
	panic(fmt.Sprintf("formatStreamDirectCopySegName: unsupported interval unit %v", unit))
}

func parseStreamDirectCopySegStart(segName string, unit storage.IntervalUnit) (time.Time, error) {
	suffix := strings.TrimPrefix(segName, directStreamCopySegPrefix)
	switch unit {
	case storage.HOUR:
		return time.ParseInLocation(directStreamCopyHourFormat, suffix, time.Local)
	case storage.DAY:
		return time.ParseInLocation(directStreamCopyDayFormat, suffix, time.Local)
	}
	return time.Time{}, fmt.Errorf("unrecognized interval unit %v", unit)
}

func writeStreamDirectCopySegmentMetadata(segDir string, endTime time.Time) (int64, error) {
	if err := os.MkdirAll(segDir, storage.DirPerm); err != nil {
		return 0, err
	}
	body := storage.SegmentMetadata{
		Version: storage.CurrentSegmentVersion,
		EndTime: endTime.Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(
		filepath.Join(segDir, storage.SegmentMetadataFilename),
		data, storage.FilePerm,
	); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func writeStreamDirectCopySnp(dstShard string, partNames []string) (int64, error) {
	if err := os.MkdirAll(dstShard, storage.DirPerm); err != nil {
		return 0, err
	}
	data, err := json.Marshal(partNames)
	if err != nil {
		return 0, err
	}
	snpPath := filepath.Join(dstShard,
		fmt.Sprintf("%016x%s", time.Now().UnixNano(), directStreamCopySnpSuffix))
	if err := os.WriteFile(snpPath, data, storage.FilePerm); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func directCopyStreamDir(src, dst string) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dst), storage.DirPerm); err != nil {
		return 0, err
	}
	return directCopyStreamDirByteCopy(src, dst)
}

func directCopyStreamDirByteCopy(src, dst string) (int64, error) {
	var total int64
	if err := os.MkdirAll(dst, storage.DirPerm); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		srcPath := filepath.Join(src, e.Name())
		dstPath := filepath.Join(dst, e.Name())
		if e.IsDir() {
			sub, recErr := directCopyStreamDirByteCopy(srcPath, dstPath)
			if recErr != nil {
				return total, recErr
			}
			total += sub
			continue
		}
		n, copyErr := directCopyStreamFile(srcPath, dstPath)
		if copyErr != nil {
			return total, copyErr
		}
		total += n
	}
	return total, nil
}

func directCopyStreamFile(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, storage.FilePerm)
	if err != nil {
		return 0, err
	}
	defer out.Close()
	n, copyErr := io.Copy(out, in)
	return n, copyErr
}
