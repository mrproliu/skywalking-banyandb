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

package property

import (
	"context"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// CreateTestShardForDump creates a test property shard for testing the dump tool.
// It takes a temporary path and a file system as input, generates test properties with various tag types,
// creates a shard, inserts properties, and returns the path to the created shard directory.
// Parameters:
//
//	tmpPath:    the base directory where the shard will be created.
//	fileSystem: the file system to use for writing the shard.
//
// Returns:
//
//	The path to the created shard directory and a cleanup function.
func CreateTestShardForDump(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	now := time.Now().UnixNano()

	// Create a database with a shard
	snapshotDir := tmpPath // Use same directory for snapshot
	db, err := openDB(context.Background(), tmpPath, 3*time.Second, time.Hour, 32, observability.BypassRegistry, fileSystem,
		true, snapshotDir, "@every 10m", time.Second*10, "* 2 * * *", nil, nil, nil)
	if err != nil {
		panic(err)
	}

	// Load shard 0
	shard, err := db.loadShard(context.Background(), "test-group", 0)
	if err != nil {
		db.close()
		panic(err)
	}

	// Create test properties with various tag types
	properties := []*propertyv1.Property{
		{
			Metadata: &commonv1.Metadata{
				Group:       "test-group",
				Name:        "test-name",
				ModRevision: now,
			},
			Id: "test-id1",
			Tags: []*modelv1.Tag{
				{
					Key: "strTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{Value: "test-value"},
						},
					},
				},
				{
					Key: "intTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Int{
							Int: &modelv1.Int{Value: 100},
						},
					},
				},
			},
		},
		{
			Metadata: &commonv1.Metadata{
				Group:       "test-group",
				Name:        "test-name",
				ModRevision: now + 1000,
			},
			Id: "test-id2",
			Tags: []*modelv1.Tag{
				{
					Key: "strArrTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_StrArray{
							StrArray: &modelv1.StrArray{
								Value: []string{"value1", "value2"},
							},
						},
					},
				},
				{
					Key: "intArrTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_IntArray{
							IntArray: &modelv1.IntArray{
								Value: []int64{25, 30},
							},
						},
					},
				},
			},
		},
		{
			Metadata: &commonv1.Metadata{
				Group:       "test-group2",
				Name:        "test-name2",
				ModRevision: now + 2000,
			},
			Id: "test-id3",
			Tags: []*modelv1.Tag{
				{
					Key: "strTag1",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{Value: "tag1"},
						},
					},
				},
				{
					Key: "strTag2",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{Value: "tag2"},
						},
					},
				},
			},
		},
	}

	// Insert properties
	for _, p := range properties {
		if err := shard.update(GetPropertyID(p), p); err != nil {
			db.close()
			panic(err)
		}
	}

	// Wait a bit for the data to be persisted
	time.Sleep(100 * time.Millisecond)

	// Get shard path before closing the database
	shardPath := shard.location

	// Close the database to release the lock on the directory
	// This allows the dump tool to open the same directory
	if err := db.close(); err != nil {
		panic(err)
	}

	// Wait a bit more to ensure all file handles are released
	time.Sleep(50 * time.Millisecond)

	cleanup := func() {
		// Cleanup is handled by the caller's test.Space cleanup
		// Database is already closed, so nothing to do here
	}

	return shardPath, cleanup
}

// testService is a Service implementation for testing that implements the full property.Service interface.
type testService struct {
	db    *database
	l     *logger.Logger
	close chan struct{}
}

// Ensure testService implements Service interface.
var _ Service = (*testService)(nil)

// NewTestService creates a Service for testing purposes.
// It returns the Service, a close function, and any error encountered.
func NewTestService(dataDir, snapshotDir string, omr observability.MetricsRegistry, fileSystem fs.FileSystem) (Service, func() error, error) {
	db, dbErr := openDB(context.Background(), dataDir, 3*time.Second, time.Hour, 32, omr, fileSystem,
		false, snapshotDir, "@every 10m", time.Second*10, "* 2 * * *", nil, nil, nil)
	if dbErr != nil {
		return nil, nil, dbErr
	}
	l := logger.GetLogger("property-test")
	return &testService{db: db, l: l, close: make(chan struct{})}, db.close, nil
}

func (s *testService) GetGossIPMessenger() gossip.Messenger {
	return nil
}

// PreRun implements run.PreRunner.
func (s *testService) PreRun(_ context.Context) error {
	return nil
}

// FlagSet implements run.Config.
func (s *testService) FlagSet() *run.FlagSet {
	return run.NewFlagSet("property-test")
}

// Validate implements run.Config.
func (s *testService) Validate() error {
	return nil
}

// Name implements run.Service.
func (s *testService) Name() string {
	return "property-test"
}

// Role implements run.Service.
func (s *testService) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

// Serve implements run.Service.
func (s *testService) Serve() run.StopNotify {
	return s.close
}

// GracefulStop implements run.Service.
func (s *testService) GracefulStop() {
	select {
	case <-s.close:
	default:
		close(s.close)
	}
}

// GetRouteTable implements route.TableProvider.
func (s *testService) GetRouteTable() *databasev1.RouteTable {
	return nil
}

// GetGossIPGrpcPort implements Service.
func (s *testService) GetGossIPGrpcPort() *uint32 {
	return nil
}

// DirectUpdate implements DirectService.DirectUpdate.
func (s *testService) DirectUpdate(ctx context.Context, group string, shardID uint32, id []byte, prop *propertyv1.Property) error {
	return s.db.update(ctx, common.ShardID(shardID), id, prop)
}

// DirectDelete implements DirectService.DirectDelete.
func (s *testService) DirectDelete(ctx context.Context, ids [][]byte) error {
	return s.db.delete(ctx, ids)
}

// DirectQuery implements DirectService.DirectQuery.
func (s *testService) DirectQuery(ctx context.Context, req *propertyv1.QueryRequest) ([]*PropertyWithDeleteTime, error) {
	results, queryErr := s.db.query(ctx, req)
	if queryErr != nil {
		return nil, queryErr
	}
	props := make([]*PropertyWithDeleteTime, 0, len(results))
	for _, r := range results {
		prop := &propertyv1.Property{}
		if unmarshalErr := protojson.Unmarshal(r.source, prop); unmarshalErr != nil {
			s.l.Warn().Err(unmarshalErr).Msg("failed to unmarshal property")
			continue
		}
		props = append(props, &PropertyWithDeleteTime{
			Property:   prop,
			DeleteTime: r.deleteTime,
		})
	}
	return props, nil
}

// DirectGet implements DirectService.DirectGet.
func (s *testService) DirectGet(ctx context.Context, group, name, id string) (*propertyv1.Property, error) {
	req := &propertyv1.QueryRequest{
		Groups: []string{group},
		Name:   name,
		Ids:    []string{id},
		Limit:  1,
	}
	results, queryErr := s.DirectQuery(ctx, req)
	if queryErr != nil {
		return nil, queryErr
	}
	for _, r := range results {
		if r.DeleteTime == 0 {
			return r.Property, nil
		}
	}
	return nil, nil
}

// DirectRepair implements DirectService.DirectRepair.
func (s *testService) DirectRepair(ctx context.Context, shardID uint64, id []byte, prop *propertyv1.Property, deleteTime int64) error {
	return s.db.repair(ctx, id, shardID, prop, deleteTime)
}
