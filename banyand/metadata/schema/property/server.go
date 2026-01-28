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
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	lockFilename        = "lock"
	defaultRepairCron   = "@every 10m"
	metadataSubDir      = "metadata"
	metadataServiceName = "metadata-property"
)

var (
	errEmptyRootPath = errors.New("root path is empty")
	metadataScope    = observability.RootScope.SubScope("metadata_property")
)

// Server is the metadata property server that stores schema data as properties.
type Server struct {
	propertyService   property.Service
	omr               observability.MetricsRegistry
	pm                protector.Memory
	lfs               fs.FileSystem
	lock              fs.File
	metadataSvc       metadata.Service
	l                 *logger.Logger
	repairScheduler   *repairScheduler
	snapshotManager   *snapshotManager
	close             chan struct{}
	root              string
	repairTriggerCron string
	flushInterval     time.Duration
	mu                sync.RWMutex
	repairEnabled     bool
	closed            atomic.Bool
}

// NewServer creates a new metadata property server.
func NewServer(propertyService property.Service, omr observability.MetricsRegistry, metadataSvc metadata.Service, pm protector.Memory) *Server {
	return &Server{
		propertyService:   propertyService,
		omr:               omr,
		pm:                pm,
		close:             make(chan struct{}),
		repairTriggerCron: defaultRepairCron,
		flushInterval:     5 * time.Second,
		repairEnabled:     true,
		metadataSvc:       metadataSvc,
	}
}

// Name returns the server name.
func (s *Server) Name() string {
	return metadataServiceName
}

// Role returns the server role.
func (s *Server) Role() databasev1.Role {
	return databasev1.Role_ROLE_META
}

// FlagSet returns the flag set for configuration.
func (s *Server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("metadata-property")
	fs.StringVar(&s.root, "metadata-property-root-path", "/tmp", "the root path of metadata property storage")
	fs.StringVar(&s.repairTriggerCron, "metadata-property-repair-trigger-cron", defaultRepairCron, "the cron expression for repair trigger")
	fs.BoolVar(&s.repairEnabled, "metadata-property-repair-enabled", true, "enable repair mechanism")
	return fs
}

// Validate validates the server configuration.
func (s *Server) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	if _, cronErr := cron.ParseStandard(s.repairTriggerCron); cronErr != nil {
		return errors.New("metadata-property-repair-trigger-cron is not a valid cron expression")
	}
	return nil
}

// PreRun initializes the server.
func (s *Server) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	s.lfs = fs.NewLocalFileSystemWithLoggerAndLimit(s.l, s.pm.GetLimit())
	location := filepath.Join(s.root, "property", metadataSubDir, storage.DataDir)
	s.lfs.MkdirIfNotExist(location, storage.DirPerm)
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	if s.propertyService == nil {
		return errors.New("property service is not set")
	}
	_ = s.omr.With(metadataScope.ConstLabels(meter.LabelPairs{"server": metadataServiceName}))
	lockPath := filepath.Join(location, lockFilename)
	var lockErr error
	s.lock, lockErr = s.lfs.CreateLockFile(lockPath, storage.FilePerm)
	if lockErr != nil {
		s.l.Error().Err(lockErr).Msg("failed to create lock file")
		return lockErr
	}

	// Initialize repair scheduler
	var repairErr error
	s.repairScheduler, repairErr = newRepairScheduler(s, s.repairTriggerCron, s.repairEnabled, s.l)
	if repairErr != nil {
		return repairErr
	}
	s.metadataSvc.RegisterHandler("metadata-node-property", schema.KindNode, s.repairScheduler)

	// Initialize snapshot manager
	snapshotDir := filepath.Join(s.root, "property", metadataSubDir, "snapshots")
	s.lfs.MkdirIfNotExist(snapshotDir, storage.DirPerm)
	s.snapshotManager = newSnapshotManager(s, snapshotDir, s.l)

	s.l.Info().Str("path", location).Msg("metadata property server initialized")
	return nil
}

// Serve starts the server.
func (s *Server) Serve() run.StopNotify {
	if startErr := s.repairScheduler.Start(); startErr != nil {
		s.l.Error().Err(startErr).Msg("failed to start repair scheduler")
	}
	return s.close
}

// GracefulStop stops the server gracefully.
func (s *Server) GracefulStop() {
	if s.closed.Swap(true) {
		return
	}
	close(s.close)
	if s.repairScheduler != nil {
		s.repairScheduler.Stop()
	}
	if s.lock != nil {
		s.lock.Close()
	}
}

// SchemaManagementService returns the schema management service handler.
func (s *Server) SchemaManagementService() schemav1.SchemaManagementServiceServer {
	return &schemaManagementServer{
		server: s,
		l:      s.l,
	}
}

// SchemaUpdateService returns the schema update service handler.
func (s *Server) SchemaUpdateService() schemav1.SchemaUpdateServiceServer {
	return &schemaUpdateServer{
		server: s,
		l:      s.l,
	}
}

// RegisterGRPCServices registers schema management services to gRPC server.
func (s *Server) RegisterGRPCServices(grpcServer *grpc.Server) {
	schemav1.RegisterSchemaManagementServiceServer(grpcServer, s.SchemaManagementService())
	schemav1.RegisterSchemaUpdateServiceServer(grpcServer, s.SchemaUpdateService())
	s.l.Info().Msg("registered metadata property gRPC services")
}

// CreateSnapshot creates a snapshot of all metadata properties.
func (s *Server) CreateSnapshot(ctx context.Context, name string) (string, error) {
	return s.snapshotManager.CreateSnapshot(ctx, name)
}

// RestoreSnapshot restores metadata properties from a snapshot file.
func (s *Server) RestoreSnapshot(ctx context.Context, path string) error {
	return s.snapshotManager.RestoreSnapshot(ctx, path)
}

// insert inserts a schema property.
func (s *Server) insert(ctx context.Context, prop *propertyv1.Property) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errors.New("server is closed")
	}
	id := property.GetPropertyID(prop)
	return s.propertyService.DirectUpdate(ctx, prop.Metadata.Group, 0, id, prop)
}

// update updates a schema property.
func (s *Server) update(ctx context.Context, prop *propertyv1.Property) error {
	return s.insert(ctx, prop)
}

// delete deletes a schema property.
func (s *Server) delete(ctx context.Context, group, name, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return false, errors.New("server is closed")
	}
	prop, getErr := s.propertyService.DirectGet(ctx, group, name, id)
	if getErr != nil {
		return false, getErr
	}
	if prop == nil {
		return false, nil
	}
	docID := property.GetPropertyID(prop)
	deleteErr := s.propertyService.DirectDelete(ctx, [][]byte{docID})
	return deleteErr == nil, deleteErr
}

// get retrieves a single schema property.
func (s *Server) get(ctx context.Context, group, name, id string) (*propertyv1.Property, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return nil, errors.New("server is closed")
	}
	return s.propertyService.DirectGet(ctx, group, name, id)
}

// list lists schema properties with delete time info.
func (s *Server) list(ctx context.Context, req *propertyv1.QueryRequest) ([]*property.PropertyWithDeleteTime, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return nil, errors.New("server is closed")
	}
	return s.propertyService.DirectQuery(ctx, req)
}
