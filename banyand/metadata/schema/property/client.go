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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/common"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const defaultSyncInterval = 30 * time.Second

var errNoMetadataServers = errors.New("no metadata servers available")

// cacheEntry stores cached schema information for change detection.
type cacheEntry struct {
	valueHash   uint64
	modRevision int64
	kind        schema.Kind
	group       string
	name        string
}

// schemaCache manages local schema cache.
type schemaCache struct {
	entries     map[string]cacheEntry
	maxRevision int64
	mu          sync.RWMutex
}

func newSchemaCache() *schemaCache {
	return &schemaCache{
		entries: make(map[string]cacheEntry),
	}
}

// Update updates the cache entry and returns true if it was changed.
func (c *schemaCache) Update(propID string, entry cacheEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, exists := c.entries[propID]
	if exists {
		if existing.modRevision > entry.modRevision {
			return false
		}
		if existing.modRevision == entry.modRevision && existing.valueHash == entry.valueHash {
			return false
		}
	}
	c.entries[propID] = entry
	if entry.modRevision > c.maxRevision {
		c.maxRevision = entry.modRevision
	}
	return true
}

// Delete removes an entry from the cache and returns true if it existed.
func (c *schemaCache) Delete(propID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[propID]; exists {
		delete(c.entries, propID)
		return true
	}
	return false
}

// GetMaxRevision returns the maximum revision seen.
func (c *schemaCache) GetMaxRevision() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxRevision
}

// GetAllEntries returns a copy of all cache entries.
func (c *schemaCache) GetAllEntries() map[string]cacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]cacheEntry, len(c.entries))
	for k, v := range c.entries {
		result[k] = v
	}
	return result
}

// GetEntriesByKind returns entries matching the specified kind.
func (c *schemaCache) GetEntriesByKind(kind schema.Kind) map[string]cacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]cacheEntry)
	for propID, entry := range c.entries {
		if entry.kind == kind {
			result[propID] = entry
		}
	}
	return result
}

// GetCachedKinds returns the unique kinds that have cached entries.
func (c *schemaCache) GetCachedKinds() []schema.Kind {
	c.mu.RLock()
	defer c.mu.RUnlock()
	kindSet := make(map[schema.Kind]struct{})
	for _, entry := range c.entries {
		kindSet[entry.kind] = struct{}{}
	}
	kinds := make([]schema.Kind, 0, len(kindSet))
	for kind := range kindSet {
		kinds = append(kinds, kind)
	}
	return kinds
}

// nodeConnection represents a connection to a metadata node.
type nodeConnection struct {
	mgrClient    schemav1.SchemaManagementServiceClient
	updateClient schemav1.SchemaUpdateServiceClient
	conn         *grpc.ClientConn
}

// SchemaRegistry implements schema.Registry interface using property-based storage.
type SchemaRegistry struct {
	dialOptsPrv  common.GRPCDialOptionsProvider
	handlers     map[schema.Kind][]schema.EventHandler
	nodeConns    map[string]*nodeConnection
	cache        *schemaCache
	closer       *run.Closer
	syncCloser   *run.Closer
	l            *logger.Logger
	mu           sync.RWMutex
	connMu       sync.RWMutex
	grpcTimeout  time.Duration
	syncInterval time.Duration
}

// NewSchemaRegistryClient creates a new property schema registry client.
// It accepts a NodeDiscovery service to dynamically discover and connect to metadata nodes.
func NewSchemaRegistryClient(dialOptsProvider common.GRPCDialOptionsProvider, grpcTimeout time.Duration) *SchemaRegistry {
	r := &SchemaRegistry{
		dialOptsPrv:  dialOptsProvider,
		nodeConns:    make(map[string]*nodeConnection),
		handlers:     make(map[schema.Kind][]schema.EventHandler),
		cache:        newSchemaCache(),
		closer:       run.NewCloser(1),
		l:            logger.GetLogger("property-schema-registry"),
		grpcTimeout:  grpcTimeout,
		syncInterval: defaultSyncInterval,
	}
	return r
}

// SetSyncInterval sets the sync interval for polling. This should be called before StartWatcher.
// This is primarily intended for testing purposes.
func (r *SchemaRegistry) SetSyncInterval(interval time.Duration) {
	r.syncInterval = interval
}

func (r *SchemaRegistry) OnInit(_ []schema.Kind) (bool, []int64) {
	return false, nil
}

func (r *SchemaRegistry) OnAddOrUpdate(m schema.Metadata) {
	if m.Kind != schema.KindNode {
		return
	}
	node, ok := m.Spec.(*databasev1.Node)
	if !ok {
		return
	}
	containsMetadata := false
	for _, role := range node.Roles {
		if role == databasev1.Role_ROLE_META {
			containsMetadata = true
			break
		}
	}
	if !containsMetadata {
		return
	}
	r.addNodeConnection(node)
}

func (r *SchemaRegistry) OnDelete(m schema.Metadata) {
	if m.Kind != schema.KindNode {
		return
	}
	node, ok := m.Spec.(*databasev1.Node)
	if !ok {
		return
	}
	r.removeNodeConnection(node.GetGrpcAddress())
}

func (r *SchemaRegistry) addNodeConnection(node *databasev1.Node) {
	address := node.GetGrpcAddress()
	if address == "" {
		return
	}
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if _, exists := r.nodeConns[address]; exists {
		return
	}
	var dialOpts []grpc.DialOption
	if r.dialOptsPrv != nil {
		var optsErr error
		dialOpts, optsErr = r.dialOptsPrv.GetDialOptions(address)
		if optsErr != nil {
			r.l.Warn().Err(optsErr).Str("address", address).Msg("failed to get dial options")
			return
		}
	}
	conn, connErr := grpchelper.Conn(address, r.grpcTimeout, dialOpts...)
	if connErr != nil {
		r.l.Warn().Err(connErr).Str("address", address).Msg("failed to connect to metadata node")
		return
	}
	schemaMgrClient := schemav1.NewSchemaManagementServiceClient(conn)
	schemaUpdateClient := schemav1.NewSchemaUpdateServiceClient(conn)
	nodeConn := &nodeConnection{
		mgrClient:    schemaMgrClient,
		updateClient: schemaUpdateClient,
		conn:         conn,
	}
	r.nodeConns[address] = nodeConn
	r.l.Info().Str("address", address).Str("node", node.GetMetadata().GetName()).Msg("connected to metadata node")
	go r.initializeFromNode(nodeConn)
}

func (r *SchemaRegistry) removeNodeConnection(address string) {
	if address == "" {
		return
	}
	r.connMu.Lock()
	defer r.connMu.Unlock()
	nc, exists := r.nodeConns[address]
	if !exists {
		return
	}
	if nc.conn != nil {
		if closeErr := nc.conn.Close(); closeErr != nil {
			r.l.Warn().Err(closeErr).Str("address", address).Msg("failed to close connection")
		}
	}
	delete(r.nodeConns, address)
	r.l.Info().Str("address", address).Msg("disconnected from metadata node")
}

func (r *SchemaRegistry) getClients() []schemav1.SchemaManagementServiceClient {
	r.connMu.RLock()
	defer r.connMu.RUnlock()
	clients := make([]schemav1.SchemaManagementServiceClient, 0, len(r.nodeConns))
	for _, nc := range r.nodeConns {
		clients = append(clients, nc.mgrClient)
	}
	return clients
}

// Close closes the registry.
func (r *SchemaRegistry) Close() error {
	r.closer.Done()
	r.closer.CloseThenWait()
	if r.syncCloser != nil {
		r.syncCloser.Done()
		r.syncCloser.CloseThenWait()
	}
	r.connMu.Lock()
	defer r.connMu.Unlock()
	for addr, nc := range r.nodeConns {
		if nc.conn != nil {
			if closeErr := nc.conn.Close(); closeErr != nil {
				r.l.Warn().Err(closeErr).Str("address", addr).Msg("failed to close connection")
			}
		}
	}
	r.nodeConns = make(map[string]*nodeConnection)
	return nil
}

// RegisterHandler registers an event handler for a schema kind.
func (r *SchemaRegistry) RegisterHandler(name string, kind schema.Kind, handler schema.EventHandler) {
	// Validate kind
	if kind&schema.KindMask != kind {
		panic(fmt.Sprintf("invalid kind %d", kind))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var kinds []schema.Kind
	for i := 0; i < schema.KindSize; i++ {
		ki := schema.Kind(1 << i)
		if kind&ki > 0 {
			kinds = append(kinds, ki)
		}
	}
	r.l.Info().Str("name", name).Interface("kinds", kinds).Msg("registering handler")
	handler.OnInit(kinds)
	for _, ki := range kinds {
		r.addHandler(ki, handler)
	}
}

// Register registers a metadata entry.
func (r *SchemaRegistry) Register(ctx context.Context, metadata schema.Metadata, _ bool) error {
	prop, convErr := SchemaToProperty(metadata.Kind, metadata.Spec.(proto.Message))
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

// Compact is not supported in property mode.
func (r *SchemaRegistry) Compact(_ context.Context, _ int64) error {
	return nil
}

// StartWatcher starts the global sync mechanism.
func (r *SchemaRegistry) StartWatcher() {
	r.syncCloser = run.NewCloser(1)
	go r.globalSync()
}

// Stream methods.

// GetStream retrieves a stream by metadata.
func (r *SchemaRegistry) GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
	prop, getErr := r.getSchema(ctx, schema.KindStream, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindStream, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.Stream), nil
}

// ListStream lists streams in a group.
func (r *SchemaRegistry) ListStream(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Stream, error) {
	if opt.Group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, schema.KindStream, opt.Group)
	if listErr != nil {
		return nil, listErr
	}
	streams := make([]*databasev1.Stream, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindStream, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to stream")
			continue
		}
		streams = append(streams, md.Spec.(*databasev1.Stream))
	}
	return streams, nil
}

// CreateStream creates a new stream.
func (r *SchemaRegistry) CreateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	prop, convErr := SchemaToProperty(schema.KindStream, stream)
	if convErr != nil {
		return 0, convErr
	}
	if insertErr := r.insertToAllServers(ctx, prop); insertErr != nil {
		return 0, insertErr
	}
	return time.Now().UnixNano(), nil
}

// UpdateStream updates an existing stream.
func (r *SchemaRegistry) UpdateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	prop, convErr := SchemaToProperty(schema.KindStream, stream)
	if convErr != nil {
		return 0, convErr
	}
	if updateErr := r.updateToAllServers(ctx, prop); updateErr != nil {
		return 0, updateErr
	}
	return time.Now().UnixNano(), nil
}

// DeleteStream deletes a stream.
func (r *SchemaRegistry) DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindStream, metadata.GetGroup(), metadata.GetName())
}

// Measure methods.

// GetMeasure retrieves a measure by metadata.
func (r *SchemaRegistry) GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error) {
	prop, getErr := r.getSchema(ctx, schema.KindMeasure, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindMeasure, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.Measure), nil
}

// ListMeasure lists measures in a group.
func (r *SchemaRegistry) ListMeasure(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Measure, error) {
	if opt.Group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, schema.KindMeasure, opt.Group)
	if listErr != nil {
		return nil, listErr
	}
	measures := make([]*databasev1.Measure, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindMeasure, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to measure")
			continue
		}
		measures = append(measures, md.Spec.(*databasev1.Measure))
	}
	return measures, nil
}

// CreateMeasure creates a new measure.
func (r *SchemaRegistry) CreateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	prop, convErr := SchemaToProperty(schema.KindMeasure, measure)
	if convErr != nil {
		return 0, convErr
	}
	if insertErr := r.insertToAllServers(ctx, prop); insertErr != nil {
		return 0, insertErr
	}
	return time.Now().UnixNano(), nil
}

// UpdateMeasure updates an existing measure.
func (r *SchemaRegistry) UpdateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	prop, convErr := SchemaToProperty(schema.KindMeasure, measure)
	if convErr != nil {
		return 0, convErr
	}
	if updateErr := r.updateToAllServers(ctx, prop); updateErr != nil {
		return 0, updateErr
	}
	return time.Now().UnixNano(), nil
}

// DeleteMeasure deletes a measure.
func (r *SchemaRegistry) DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindMeasure, metadata.GetGroup(), metadata.GetName())
}

// TopNAggregations returns top-N aggregations for a measure.
func (r *SchemaRegistry) TopNAggregations(ctx context.Context, metadata *commonv1.Metadata) ([]*databasev1.TopNAggregation, error) {
	aggregations, listErr := r.ListTopNAggregation(ctx, schema.ListOpt{Group: metadata.GetGroup()})
	if listErr != nil {
		return nil, listErr
	}
	var result []*databasev1.TopNAggregation
	for _, aggrDef := range aggregations {
		if aggrDef.GetSourceMeasure().GetName() == metadata.GetName() {
			result = append(result, aggrDef)
		}
	}
	return result, nil
}

// Trace methods.

// GetTrace retrieves a trace by metadata.
func (r *SchemaRegistry) GetTrace(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Trace, error) {
	prop, getErr := r.getSchema(ctx, schema.KindTrace, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindTrace, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.Trace), nil
}

// ListTrace lists traces in a group.
func (r *SchemaRegistry) ListTrace(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Trace, error) {
	if opt.Group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, schema.KindTrace, opt.Group)
	if listErr != nil {
		return nil, listErr
	}
	traces := make([]*databasev1.Trace, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindTrace, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to trace")
			continue
		}
		traces = append(traces, md.Spec.(*databasev1.Trace))
	}
	return traces, nil
}

// CreateTrace creates a new trace.
func (r *SchemaRegistry) CreateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	prop, convErr := SchemaToProperty(schema.KindTrace, trace)
	if convErr != nil {
		return 0, convErr
	}
	if insertErr := r.insertToAllServers(ctx, prop); insertErr != nil {
		return 0, insertErr
	}
	return time.Now().UnixNano(), nil
}

// UpdateTrace updates an existing trace.
func (r *SchemaRegistry) UpdateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	prop, convErr := SchemaToProperty(schema.KindTrace, trace)
	if convErr != nil {
		return 0, convErr
	}
	if updateErr := r.updateToAllServers(ctx, prop); updateErr != nil {
		return 0, updateErr
	}
	return time.Now().UnixNano(), nil
}

// DeleteTrace deletes a trace.
func (r *SchemaRegistry) DeleteTrace(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindTrace, metadata.GetGroup(), metadata.GetName())
}

// Group methods.

// GetGroup retrieves a group by name.
func (r *SchemaRegistry) GetGroup(ctx context.Context, group string) (*commonv1.Group, error) {
	prop, getErr := r.getSchema(ctx, schema.KindGroup, "", group)
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindGroup, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*commonv1.Group), nil
}

// ListGroup lists all groups.
func (r *SchemaRegistry) ListGroup(ctx context.Context) ([]*commonv1.Group, error) {
	props, listErr := r.listSchemas(ctx, schema.KindGroup, "")
	if listErr != nil {
		return nil, listErr
	}
	groups := make([]*commonv1.Group, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindGroup, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to group")
			continue
		}
		groups = append(groups, md.Spec.(*commonv1.Group))
	}
	return groups, nil
}

// CreateGroup creates a new group.
func (r *SchemaRegistry) CreateGroup(ctx context.Context, group *commonv1.Group) error {
	prop, convErr := SchemaToProperty(schema.KindGroup, group)
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

// UpdateGroup updates an existing group.
func (r *SchemaRegistry) UpdateGroup(ctx context.Context, group *commonv1.Group) error {
	prop, convErr := SchemaToProperty(schema.KindGroup, group)
	if convErr != nil {
		return convErr
	}
	return r.updateToAllServers(ctx, prop)
}

// DeleteGroup deletes a group.
func (r *SchemaRegistry) DeleteGroup(ctx context.Context, group string) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindGroup, "", group)
}

// IndexRule methods.

// GetIndexRule retrieves an index rule by metadata.
func (r *SchemaRegistry) GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	prop, getErr := r.getSchema(ctx, schema.KindIndexRule, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindIndexRule, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.IndexRule), nil
}

// ListIndexRule lists index rules in a group.
func (r *SchemaRegistry) ListIndexRule(ctx context.Context, opt schema.ListOpt) ([]*databasev1.IndexRule, error) {
	if opt.Group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, schema.KindIndexRule, opt.Group)
	if listErr != nil {
		return nil, listErr
	}
	rules := make([]*databasev1.IndexRule, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindIndexRule, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to index rule")
			continue
		}
		rules = append(rules, md.Spec.(*databasev1.IndexRule))
	}
	return rules, nil
}

// CreateIndexRule creates a new index rule.
func (r *SchemaRegistry) CreateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	prop, convErr := SchemaToProperty(schema.KindIndexRule, indexRule)
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

// UpdateIndexRule updates an existing index rule.
func (r *SchemaRegistry) UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	prop, convErr := SchemaToProperty(schema.KindIndexRule, indexRule)
	if convErr != nil {
		return convErr
	}
	return r.updateToAllServers(ctx, prop)
}

// DeleteIndexRule deletes an index rule.
func (r *SchemaRegistry) DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindIndexRule, metadata.GetGroup(), metadata.GetName())
}

// IndexRuleBinding methods.

// GetIndexRuleBinding retrieves an index rule binding by metadata.
func (r *SchemaRegistry) GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	prop, getErr := r.getSchema(ctx, schema.KindIndexRuleBinding, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindIndexRuleBinding, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.IndexRuleBinding), nil
}

// ListIndexRuleBinding lists index rule bindings in a group.
func (r *SchemaRegistry) ListIndexRuleBinding(ctx context.Context, opt schema.ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	if opt.Group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, schema.KindIndexRuleBinding, opt.Group)
	if listErr != nil {
		return nil, listErr
	}
	bindings := make([]*databasev1.IndexRuleBinding, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindIndexRuleBinding, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to index rule binding")
			continue
		}
		bindings = append(bindings, md.Spec.(*databasev1.IndexRuleBinding))
	}
	return bindings, nil
}

// CreateIndexRuleBinding creates a new index rule binding.
func (r *SchemaRegistry) CreateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	prop, convErr := SchemaToProperty(schema.KindIndexRuleBinding, indexRuleBinding)
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

// UpdateIndexRuleBinding updates an existing index rule binding.
func (r *SchemaRegistry) UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	prop, convErr := SchemaToProperty(schema.KindIndexRuleBinding, indexRuleBinding)
	if convErr != nil {
		return convErr
	}
	return r.updateToAllServers(ctx, prop)
}

// DeleteIndexRuleBinding deletes an index rule binding.
func (r *SchemaRegistry) DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindIndexRuleBinding, metadata.GetGroup(), metadata.GetName())
}

// TopNAggregation methods.

// GetTopNAggregation retrieves a top-N aggregation by metadata.
func (r *SchemaRegistry) GetTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.TopNAggregation, error) {
	prop, getErr := r.getSchema(ctx, schema.KindTopNAggregation, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindTopNAggregation, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.TopNAggregation), nil
}

// ListTopNAggregation lists top-N aggregations in a group.
func (r *SchemaRegistry) ListTopNAggregation(ctx context.Context, opt schema.ListOpt) ([]*databasev1.TopNAggregation, error) {
	if opt.Group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, schema.KindTopNAggregation, opt.Group)
	if listErr != nil {
		return nil, listErr
	}
	aggregations := make([]*databasev1.TopNAggregation, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindTopNAggregation, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to top-N aggregation")
			continue
		}
		aggregations = append(aggregations, md.Spec.(*databasev1.TopNAggregation))
	}
	return aggregations, nil
}

// CreateTopNAggregation creates a new top-N aggregation.
func (r *SchemaRegistry) CreateTopNAggregation(ctx context.Context, topN *databasev1.TopNAggregation) error {
	prop, convErr := SchemaToProperty(schema.KindTopNAggregation, topN)
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

// UpdateTopNAggregation updates an existing top-N aggregation.
func (r *SchemaRegistry) UpdateTopNAggregation(ctx context.Context, topN *databasev1.TopNAggregation) error {
	prop, convErr := SchemaToProperty(schema.KindTopNAggregation, topN)
	if convErr != nil {
		return convErr
	}
	return r.updateToAllServers(ctx, prop)
}

// DeleteTopNAggregation deletes a top-N aggregation.
func (r *SchemaRegistry) DeleteTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindTopNAggregation, metadata.GetGroup(), metadata.GetName())
}

// Node methods.

// ListNode lists nodes by role.
func (r *SchemaRegistry) ListNode(ctx context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	if role == databasev1.Role_ROLE_UNSPECIFIED {
		return nil, schema.BadRequest("role", "role should not be unspecified")
	}
	props, listErr := r.listSchemas(ctx, schema.KindNode, "")
	if listErr != nil {
		return nil, listErr
	}
	nodes := make([]*databasev1.Node, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindNode, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to node")
			continue
		}
		node := md.Spec.(*databasev1.Node)
		for _, nodeRole := range node.Roles {
			if nodeRole == role {
				nodes = append(nodes, node)
				break
			}
		}
	}
	return nodes, nil
}

// RegisterNode registers a node.
func (r *SchemaRegistry) RegisterNode(ctx context.Context, node *databasev1.Node, _ bool) error {
	prop, convErr := SchemaToProperty(schema.KindNode, node)
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

// GetNode retrieves a node by name.
func (r *SchemaRegistry) GetNode(ctx context.Context, node string) (*databasev1.Node, error) {
	prop, getErr := r.getSchema(ctx, schema.KindNode, "", node)
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindNode, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.Node), nil
}

// UpdateNode updates a node.
func (r *SchemaRegistry) UpdateNode(ctx context.Context, node *databasev1.Node) error {
	prop, convErr := SchemaToProperty(schema.KindNode, node)
	if convErr != nil {
		return convErr
	}
	return r.updateToAllServers(ctx, prop)
}

// Property methods.

// GetProperty retrieves a property by metadata.
func (r *SchemaRegistry) GetProperty(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Property, error) {
	prop, getErr := r.getSchema(ctx, schema.KindProperty, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindProperty, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*databasev1.Property), nil
}

// ListProperty lists properties in a group.
func (r *SchemaRegistry) ListProperty(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Property, error) {
	if opt.Group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, schema.KindProperty, opt.Group)
	if listErr != nil {
		return nil, listErr
	}
	properties := make([]*databasev1.Property, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindProperty, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to database property")
			continue
		}
		properties = append(properties, md.Spec.(*databasev1.Property))
	}
	return properties, nil
}

// CreateProperty creates a new property.
func (r *SchemaRegistry) CreateProperty(ctx context.Context, property *databasev1.Property) error {
	prop, convErr := SchemaToProperty(schema.KindProperty, property)
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

// UpdateProperty updates an existing property.
func (r *SchemaRegistry) UpdateProperty(ctx context.Context, property *databasev1.Property) error {
	prop, convErr := SchemaToProperty(schema.KindProperty, property)
	if convErr != nil {
		return convErr
	}
	return r.updateToAllServers(ctx, prop)
}

// DeleteProperty deletes a property.
func (r *SchemaRegistry) DeleteProperty(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindProperty, metadata.GetGroup(), metadata.GetName())
}

// Internal helper methods.

func (r *SchemaRegistry) getSchema(ctx context.Context, kind schema.Kind, group, name string) (*propertyv1.Property, error) {
	clients := r.getClients()
	if len(clients) == 0 {
		return nil, errNoMetadataServers
	}
	propID := BuildPropertyID(kind, &commonv1.Metadata{Group: group, Name: name})
	req := &schemav1.GetSchemaRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{SchemaGroup},
			Name:   kind.String(),
			Ids:    []string{propID},
		},
	}
	resp, getErr := clients[0].GetSchema(ctx, req)
	if getErr != nil {
		return nil, getErr
	}
	return resp.Properties, nil
}

// schemaWithDeleteTime holds a property with its delete time for cross-node sync.
type schemaWithDeleteTime struct {
	property   *propertyv1.Property
	deleteTime int64
}

// propInfo tracks the best schema and node-specific state for aggregation.
type propInfo struct {
	nodeRev     map[string]int64
	nodeDelTime map[string]int64
	best        *schemaWithDeleteTime
	bestAddr    string
}

func (r *SchemaRegistry) listSchemas(ctx context.Context, kind schema.Kind, group string) ([]*propertyv1.Property, error) {
	nodeConns := r.getNodeConnectionsWithAddr()
	if len(nodeConns) == 0 {
		return nil, errNoMetadataServers
	}
	type nodeResult struct {
		schemas []*schemaWithDeleteTime
		err     error
		addr    string
	}
	resultCh := make(chan nodeResult, len(nodeConns))
	for addr, nc := range nodeConns {
		go func(address string, client schemav1.SchemaManagementServiceClient) {
			schemas, queryErr := r.listSchemasFromClientWithDeleteTime(ctx, client, kind, group)
			resultCh <- nodeResult{addr: address, schemas: schemas, err: queryErr}
		}(addr, nc.mgrClient)
	}
	propMap := make(map[string]*propInfo)
	for range len(nodeConns) {
		res := <-resultCh
		if res.err != nil {
			r.l.Warn().Err(res.err).Str("node", res.addr).Msg("failed to query node")
			continue
		}
		for _, s := range res.schemas {
			propID := s.property.Id
			rev := s.property.Metadata.GetModRevision()
			info, exists := propMap[propID]
			if !exists {
				info = &propInfo{
					nodeRev:     make(map[string]int64),
					nodeDelTime: make(map[string]int64),
				}
				propMap[propID] = info
			}
			info.nodeRev[res.addr] = rev
			info.nodeDelTime[res.addr] = s.deleteTime
			if info.best == nil || rev > info.best.property.Metadata.GetModRevision() {
				info.best = s
				info.bestAddr = res.addr
			}
		}
	}
	r.repairInconsistentNodes(ctx, nodeConns, propMap)
	result := make([]*propertyv1.Property, 0, len(propMap))
	for _, info := range propMap {
		if info.best != nil && info.best.deleteTime == 0 {
			result = append(result, info.best.property)
		}
	}
	return result, nil
}

func (r *SchemaRegistry) getNodeConnectionsWithAddr() map[string]*nodeConnection {
	r.connMu.RLock()
	defer r.connMu.RUnlock()
	result := make(map[string]*nodeConnection, len(r.nodeConns))
	for addr, nc := range r.nodeConns {
		result[addr] = nc
	}
	return result
}

func (r *SchemaRegistry) listSchemasFromClientWithDeleteTime(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	kind schema.Kind, group string,
) ([]*schemaWithDeleteTime, error) {
	req := &schemav1.ListSchemasRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{SchemaGroup},
			Name:   kind.String(),
			Limit:  10000,
		},
	}
	resp, listErr := client.ListSchemas(ctx, req)
	if listErr != nil {
		return nil, listErr
	}
	results := make([]*schemaWithDeleteTime, 0, len(resp.Properties))
	for idx, prop := range resp.Properties {
		var deleteTime int64
		if idx < len(resp.DeleteTimes) {
			deleteTime = resp.DeleteTimes[idx]
		}
		if group != "" {
			propGroup := getGroupFromTags(prop.Tags)
			if propGroup != group {
				continue
			}
		}
		results = append(results, &schemaWithDeleteTime{
			property:   prop,
			deleteTime: deleteTime,
		})
	}
	return results, nil
}

func (r *SchemaRegistry) repairInconsistentNodes(ctx context.Context, nodeConns map[string]*nodeConnection, propMap map[string]*propInfo) {
	for propID, info := range propMap {
		if info.best == nil {
			continue
		}
		bestRev := info.best.property.Metadata.GetModRevision()
		var nodesToRepair []string
		for addr := range nodeConns {
			nodeRev, exists := info.nodeRev[addr]
			if !exists || nodeRev < bestRev {
				nodesToRepair = append(nodesToRepair, addr)
			}
		}
		if len(nodesToRepair) == 0 {
			continue
		}
		r.l.Info().Str("propID", propID).Int64("bestRev", bestRev).
			Int64("deleteTime", info.best.deleteTime).
			Strs("nodesToRepair", nodesToRepair).Msg("repairing schema inconsistency")
		var wg sync.WaitGroup
		for _, addr := range nodesToRepair {
			nc := nodeConns[addr]
			wg.Add(1)
			go func(nodeAddr string, client schemav1.SchemaManagementServiceClient) {
				defer wg.Done()
				_, repairErr := client.RepairSchema(ctx, &schemav1.RepairSchemaRequest{
					Property:   info.best.property,
					DeleteTime: info.best.deleteTime,
				})
				if repairErr != nil {
					r.l.Warn().Err(repairErr).Str("propID", propID).Str("node", nodeAddr).Msg("repair failed")
				} else {
					r.l.Info().Str("propID", propID).Str("node", nodeAddr).Msg("repaired successfully")
				}
			}(addr, nc.mgrClient)
		}
		wg.Wait()
	}
}

func (r *SchemaRegistry) insertToAllServers(ctx context.Context, prop *propertyv1.Property) error {
	clients := r.getClients()
	if len(clients) == 0 {
		return errNoMetadataServers
	}
	var firstErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(c schemav1.SchemaManagementServiceClient) {
			defer wg.Done()
			_, callErr := c.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: prop})
			if callErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = callErr
				}
				mu.Unlock()
			}
		}(client)
	}
	wg.Wait()
	return firstErr
}

func (r *SchemaRegistry) updateToAllServers(ctx context.Context, prop *propertyv1.Property) error {
	clients := r.getClients()
	if len(clients) == 0 {
		return errNoMetadataServers
	}
	var firstErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(c schemav1.SchemaManagementServiceClient) {
			defer wg.Done()
			_, callErr := c.UpdateSchema(ctx, &schemav1.UpdateSchemaRequest{Property: prop})
			if callErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = callErr
				}
				mu.Unlock()
			}
		}(client)
	}
	wg.Wait()
	return firstErr
}

func (r *SchemaRegistry) deleteFromAllServers(ctx context.Context, kind schema.Kind, group, name string) (bool, error) {
	clients := r.getClients()
	if len(clients) == 0 {
		return false, errNoMetadataServers
	}
	propID := BuildPropertyID(kind, &commonv1.Metadata{Group: group, Name: name})
	var firstErr error
	var found bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(c schemav1.SchemaManagementServiceClient) {
			defer wg.Done()
			resp, callErr := c.DeleteSchema(ctx, &schemav1.DeleteSchemaRequest{
				Delete: &propertyv1.DeleteRequest{
					Group: SchemaGroup,
					Name:  kind.String(),
					Id:    propID,
				},
			})
			mu.Lock()
			if callErr != nil && firstErr == nil {
				firstErr = callErr
			}
			if resp != nil && resp.Found {
				found = true
			}
			mu.Unlock()
		}(client)
	}
	wg.Wait()
	return found, firstErr
}

func (r *SchemaRegistry) initializeFromNode(nodeConn *nodeConnection) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r.l.Info().Str("address", nodeConn.conn.Target()).Msg("initializing resources from metadata node")
	groups, listErr := r.listGroupFromClient(ctx, nodeConn.mgrClient)
	if listErr != nil {
		r.l.Warn().Err(listErr).Msg("failed to list groups during initialization")
		return
	}
	var maxRevision int64
	for _, group := range groups {
		maxRevision = r.processInitialResource(schema.KindGroup, group, maxRevision)
	}
	for _, group := range groups {
		groupName := group.GetMetadata().GetName()
		catalog := group.GetCatalog()
		switch catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			r.initializeStreamResourcesFromClient(ctx, nodeConn.mgrClient, groupName, &maxRevision)
		case commonv1.Catalog_CATALOG_MEASURE:
			r.initializeMeasureResourcesFromClient(ctx, nodeConn.mgrClient, groupName, &maxRevision)
		case commonv1.Catalog_CATALOG_TRACE:
			r.initializeTraceResourcesFromClient(ctx, nodeConn.mgrClient, groupName, &maxRevision)
		case commonv1.Catalog_CATALOG_PROPERTY:
			r.initializePropertyResourcesFromClient(ctx, nodeConn.mgrClient, groupName, &maxRevision)
		}
		if catalog != commonv1.Catalog_CATALOG_PROPERTY {
			r.initializeIndexResourcesFromClient(ctx, nodeConn.mgrClient, groupName, &maxRevision)
		}
	}
	r.l.Info().Str("address", nodeConn.conn.Target()).
		Int64("maxRevision", maxRevision).Msg("completed resource initialization")
}

func (r *SchemaRegistry) initializeStreamResources(ctx context.Context, group string, maxRevision *int64) {
	streams, listErr := r.ListStream(ctx, schema.ListOpt{Group: group})
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list streams")
		return
	}
	for _, stream := range streams {
		*maxRevision = r.processInitialResource(schema.KindStream, stream, *maxRevision)
	}
}

func (r *SchemaRegistry) initializeMeasureResources(ctx context.Context, group string, maxRevision *int64) {
	measures, listErr := r.ListMeasure(ctx, schema.ListOpt{Group: group})
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list measures")
		return
	}
	for _, measure := range measures {
		*maxRevision = r.processInitialResource(schema.KindMeasure, measure, *maxRevision)
	}
	topNs, topNErr := r.ListTopNAggregation(ctx, schema.ListOpt{Group: group})
	if topNErr != nil {
		r.l.Warn().Err(topNErr).Str("group", group).Msg("failed to list topN aggregations")
		return
	}
	for _, topN := range topNs {
		*maxRevision = r.processInitialResource(schema.KindTopNAggregation, topN, *maxRevision)
	}
}

func (r *SchemaRegistry) initializeTraceResources(ctx context.Context, group string, maxRevision *int64) {
	traces, listErr := r.ListTrace(ctx, schema.ListOpt{Group: group})
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list traces")
		return
	}
	for _, trace := range traces {
		*maxRevision = r.processInitialResource(schema.KindTrace, trace, *maxRevision)
	}
}

func (r *SchemaRegistry) initializePropertyResources(ctx context.Context, group string, maxRevision *int64) {
	props, listErr := r.ListProperty(ctx, schema.ListOpt{Group: group})
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list properties")
		return
	}
	for _, prop := range props {
		*maxRevision = r.processInitialResource(schema.KindProperty, prop, *maxRevision)
	}
}

func (r *SchemaRegistry) initializeIndexResources(ctx context.Context, group string, maxRevision *int64) {
	rules, rulesErr := r.ListIndexRule(ctx, schema.ListOpt{Group: group})
	if rulesErr != nil {
		r.l.Warn().Err(rulesErr).Str("group", group).Msg("failed to list index rules")
	} else {
		for _, rule := range rules {
			*maxRevision = r.processInitialResource(schema.KindIndexRule, rule, *maxRevision)
		}
	}
	bindings, bindingsErr := r.ListIndexRuleBinding(ctx, schema.ListOpt{Group: group})
	if bindingsErr != nil {
		r.l.Warn().Err(bindingsErr).Str("group", group).Msg("failed to list index rule bindings")
	} else {
		for _, binding := range bindings {
			*maxRevision = r.processInitialResource(schema.KindIndexRuleBinding, binding, *maxRevision)
		}
	}
}

func (r *SchemaRegistry) processInitialResource(kind schema.Kind, spec proto.Message, currentMax int64) int64 {
	prop, convErr := SchemaToProperty(kind, spec)
	if convErr != nil {
		r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("failed to convert to property")
		return currentMax
	}
	revision := prop.Metadata.GetModRevision()
	entry := cacheEntry{
		valueHash:   computePropertyHash(prop),
		modRevision: revision,
		kind:        kind,
		group:       getGroupFromTags(prop.Tags),
		name:        getNameFromTags(prop.Tags),
	}
	if r.cache.Update(prop.Id, entry) {
		md := schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind:        kind,
				Name:        entry.name,
				Group:       entry.group,
				ModRevision: revision,
			},
			Spec: spec,
		}
		r.notifyHandlers(kind, md, false)
	}
	if revision > currentMax {
		return revision
	}
	return currentMax
}

func computePropertyHash(prop *propertyv1.Property) uint64 {
	data, _ := proto.Marshal(prop)
	return convert.Hash(data)
}

func getGroupFromTags(tags []*modelv1.Tag) string {
	for _, tag := range tags {
		if tag.Key == TagKeyGroup {
			return tag.Value.GetStr().GetValue()
		}
	}
	return ""
}

func getNameFromTags(tags []*modelv1.Tag) string {
	for _, tag := range tags {
		if tag.Key == TagKeyName {
			return tag.Value.GetStr().GetValue()
		}
	}
	return ""
}

func (r *SchemaRegistry) notifyHandlers(kind schema.Kind, md schema.Metadata, isDelete bool) {
	r.mu.RLock()
	handlers := r.handlers[kind]
	r.mu.RUnlock()
	for _, h := range handlers {
		if isDelete {
			h.OnDelete(md)
		} else {
			h.OnAddOrUpdate(md)
		}
	}
}

func (r *SchemaRegistry) addHandler(kind schema.Kind, handler schema.EventHandler) {
	if r.handlers[kind] == nil {
		r.handlers[kind] = make([]schema.EventHandler, 0)
	}
	r.handlers[kind] = append(r.handlers[kind], handler)
}

// parsePropertyID parses property ID to get kind, group, name.
// Format: "kind_group/name" or "kind_name" (for Group/Node).
func parsePropertyID(propID string) (schema.Kind, string, string) {
	underscoreIdx := strings.Index(propID, "_")
	if underscoreIdx == -1 {
		return 0, "", ""
	}
	kindStr := propID[:underscoreIdx]
	rest := propID[underscoreIdx+1:]
	kind, kindErr := KindFromString(kindStr)
	if kindErr != nil {
		return 0, "", ""
	}
	if kind == schema.KindGroup || kind == schema.KindNode {
		return kind, "", rest
	}
	slashIdx := strings.Index(rest, "/")
	if slashIdx == -1 {
		return kind, "", rest
	}
	return kind, rest[:slashIdx], rest[slashIdx+1:]
}

func (r *SchemaRegistry) globalSync() {
	if !r.syncCloser.AddRunning() {
		return
	}
	defer r.syncCloser.Done()
	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.syncCloser.CloseNotify():
			return
		case <-ticker.C:
			r.performSync()
		}
	}
}

func (r *SchemaRegistry) performSync() {
	r.l.Info().Msg("performing global schema sync")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clients := r.getSchemaUpdateClients()
	for _, c := range clients {
		sinceRevision := r.cache.GetMaxRevision()
		updatedNames := r.queryUpdatedSchemas(ctx, c.updateClient, sinceRevision)
		syncedKinds := make(map[schema.Kind]struct{})
		if len(updatedNames) > 0 {
			r.l.Info().Str("connection", c.conn.Target()).
				Int("count", len(updatedNames)).
				Int64("sinceRevision", sinceRevision).
				Msg("detected schema updates")
			for _, name := range updatedNames {
				kind, kindErr := KindFromString(name)
				if kindErr != nil {
					r.l.Warn().Str("kind_name", name).Err(kindErr).Msg("failed to parse kind from name")
					continue
				}
				syncedKinds[kind] = struct{}{}
				if kind == schema.KindGroup {
					r.syncGroupResources(ctx, c.mgrClient)
					continue
				}
				r.syncAllResourcesOfKind(ctx, c.mgrClient, kind, "")
			}
		}
		cachedKinds := r.cache.GetCachedKinds()
		for _, kind := range cachedKinds {
			if _, synced := syncedKinds[kind]; synced {
				continue
			}
			if kind == schema.KindGroup {
				r.syncGroupResources(ctx, c.mgrClient)
			} else {
				r.syncAllResourcesOfKind(ctx, c.mgrClient, kind, "")
			}
		}
	}
}

func (r *SchemaRegistry) queryUpdatedSchemas(ctx context.Context, c schemav1.SchemaUpdateServiceClient, reversion int64) []string {
	req := &schemav1.AggregateSchemaUpdatesRequest{
		Query: &propertyv1.QueryRequest{
			Groups:   []string{SchemaGroup},
			Criteria: buildModRevisionCriteria(reversion),
			Limit:    10000,
		},
	}
	resp, queryErr := c.AggregateSchemaUpdates(ctx, req)
	if queryErr != nil {
		r.l.Warn().Err(queryErr).Msg("failed to query schema updates")
		return nil
	}
	return resp.Names
}

func (r *SchemaRegistry) getSchemaUpdateClients() []*nodeConnection {
	r.connMu.RLock()
	defer r.connMu.RUnlock()
	clients := make([]*nodeConnection, 0, len(r.nodeConns))
	for _, nc := range r.nodeConns {
		clients = append(clients, nc)
	}
	return clients
}

func (r *SchemaRegistry) syncGroupResources(ctx context.Context, client schemav1.SchemaManagementServiceClient) {
	cachedGroupEntries := r.cache.GetEntriesByKind(schema.KindGroup)
	groups, listErr := r.listGroupFromClient(ctx, client)
	if listErr != nil {
		r.l.Warn().Err(listErr).Msg("failed to list groups during sync")
		return
	}
	currentGroupPropIDs := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		propID := BuildPropertyID(schema.KindGroup, group.GetMetadata())
		currentGroupPropIDs[propID] = struct{}{}
		r.processInitialResource(schema.KindGroup, group, 0)
		groupName := group.GetMetadata().GetName()
		catalog := group.GetCatalog()
		switch catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			r.initializeStreamResourcesFromClient(ctx, client, groupName, new(int64))
			r.initializeIndexResourcesFromClient(ctx, client, groupName, new(int64))
		case commonv1.Catalog_CATALOG_MEASURE:
			r.initializeMeasureResourcesFromClient(ctx, client, groupName, new(int64))
			r.initializeIndexResourcesFromClient(ctx, client, groupName, new(int64))
		case commonv1.Catalog_CATALOG_TRACE:
			r.initializeTraceResourcesFromClient(ctx, client, groupName, new(int64))
			r.initializeIndexResourcesFromClient(ctx, client, groupName, new(int64))
		case commonv1.Catalog_CATALOG_PROPERTY:
			r.initializePropertyResourcesFromClient(ctx, client, groupName, new(int64))
		}
	}
	for cachedPropID, cachedEntry := range cachedGroupEntries {
		if _, exists := currentGroupPropIDs[cachedPropID]; !exists {
			r.handleDeletion(schema.KindGroup, cachedPropID, cachedEntry)
		}
	}
}

func (r *SchemaRegistry) syncAllResourcesOfKind(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	kind schema.Kind, group string,
) {
	cachedEntries := r.cache.GetEntriesByKind(kind)
	if group != "" {
		filteredEntries := make(map[string]cacheEntry)
		for propID, entry := range cachedEntries {
			if entry.group == group {
				filteredEntries[propID] = entry
			}
		}
		cachedEntries = filteredEntries
	}
	props, listErr := r.listSchemasFromClient(ctx, client, kind, group)
	if listErr != nil {
		r.l.Warn().Err(listErr).Stringer("kind", kind).Msg("failed to list resources during sync")
		return
	}
	currentPropIDs := make(map[string]struct{}, len(props))
	for _, prop := range props {
		currentPropIDs[prop.Id] = struct{}{}
		md, convErr := ToSchema(kind, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("failed to convert property to schema")
			continue
		}
		spec, ok := md.Spec.(proto.Message)
		if !ok {
			r.l.Warn().Stringer("kind", kind).Msg("spec does not implement proto.Message")
			continue
		}
		r.processInitialResource(kind, spec, 0)
	}
	for cachedPropID, cachedEntry := range cachedEntries {
		if _, exists := currentPropIDs[cachedPropID]; !exists {
			r.handleDeletion(kind, cachedPropID, cachedEntry)
		}
	}
}

func (r *SchemaRegistry) handleDeletion(kind schema.Kind, propID string, entry cacheEntry) {
	if !r.cache.Delete(propID) {
		return
	}
	md := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:        kind,
			Name:        entry.name,
			Group:       entry.group,
			ModRevision: entry.modRevision,
		},
		Spec: nil,
	}
	r.l.Info().Stringer("kind", kind).Str("group", entry.group).Str("name", entry.name).Msg("detected resource deletion during sync")
	r.notifyHandlers(kind, md, true)
}

func buildModRevisionCriteria(sinceRevision int64) *modelv1.Criteria {
	if sinceRevision <= 0 {
		return nil
	}
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: TagKeyModRevision,
				Op:   modelv1.Condition_BINARY_OP_GT,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{
						Int: &modelv1.Int{Value: sinceRevision},
					},
				},
			},
		},
	}
}

func (r *SchemaRegistry) getSchemaFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	kind schema.Kind, group, name string,
) (*propertyv1.Property, error) {
	propID := BuildPropertyID(kind, &commonv1.Metadata{Group: group, Name: name})
	req := &schemav1.GetSchemaRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{SchemaGroup},
			Name:   kind.String(),
			Ids:    []string{propID},
		},
	}
	resp, getErr := client.GetSchema(ctx, req)
	if getErr != nil {
		return nil, getErr
	}
	return resp.Properties, nil
}

func (r *SchemaRegistry) listSchemasFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	kind schema.Kind, group string,
) ([]*propertyv1.Property, error) {
	req := &schemav1.ListSchemasRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{SchemaGroup},
			Name:   kind.String(),
			Limit:  10000,
		},
	}
	resp, listErr := client.ListSchemas(ctx, req)
	if listErr != nil {
		return nil, listErr
	}
	if group == "" {
		return resp.Properties, nil
	}
	filtered := make([]*propertyv1.Property, 0, len(resp.Properties))
	for _, prop := range resp.Properties {
		for _, tag := range prop.Tags {
			if tag.Key == TagKeyGroup && tag.Value.GetStr().GetValue() == group {
				filtered = append(filtered, prop)
				break
			}
		}
	}
	return filtered, nil
}

func (r *SchemaRegistry) listGroupFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient) ([]*commonv1.Group, error) {
	props, listErr := r.listSchemasFromClient(ctx, client, schema.KindGroup, "")
	if listErr != nil {
		return nil, listErr
	}
	groups := make([]*commonv1.Group, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindGroup, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to group")
			continue
		}
		groups = append(groups, md.Spec.(*commonv1.Group))
	}
	return groups, nil
}

func (r *SchemaRegistry) getGroupFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient, groupName string) (*commonv1.Group, error) {
	prop, getErr := r.getSchemaFromClient(ctx, client, schema.KindGroup, "", groupName)
	if getErr != nil {
		return nil, getErr
	}
	if prop == nil {
		return nil, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(schema.KindGroup, prop)
	if convErr != nil {
		return nil, convErr
	}
	return md.Spec.(*commonv1.Group), nil
}

func (r *SchemaRegistry) initializeStreamResourcesFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	group string, maxRevision *int64,
) {
	props, listErr := r.listSchemasFromClient(ctx, client, schema.KindStream, group)
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list streams")
		return
	}
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindStream, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to stream")
			continue
		}
		*maxRevision = r.processInitialResource(schema.KindStream, md.Spec.(*databasev1.Stream), *maxRevision)
	}
}

func (r *SchemaRegistry) initializeMeasureResourcesFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	group string, maxRevision *int64,
) {
	props, listErr := r.listSchemasFromClient(ctx, client, schema.KindMeasure, group)
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list measures")
		return
	}
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindMeasure, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to measure")
			continue
		}
		*maxRevision = r.processInitialResource(schema.KindMeasure, md.Spec.(*databasev1.Measure), *maxRevision)
	}
	topNProps, topNErr := r.listSchemasFromClient(ctx, client, schema.KindTopNAggregation, group)
	if topNErr != nil {
		r.l.Warn().Err(topNErr).Str("group", group).Msg("failed to list topN aggregations")
		return
	}
	for _, prop := range topNProps {
		md, convErr := ToSchema(schema.KindTopNAggregation, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to topN aggregation")
			continue
		}
		*maxRevision = r.processInitialResource(schema.KindTopNAggregation, md.Spec.(*databasev1.TopNAggregation), *maxRevision)
	}
}

func (r *SchemaRegistry) initializeTraceResourcesFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	group string, maxRevision *int64,
) {
	props, listErr := r.listSchemasFromClient(ctx, client, schema.KindTrace, group)
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list traces")
		return
	}
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindTrace, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to trace")
			continue
		}
		*maxRevision = r.processInitialResource(schema.KindTrace, md.Spec.(*databasev1.Trace), *maxRevision)
	}
}

func (r *SchemaRegistry) initializePropertyResourcesFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	group string, maxRevision *int64,
) {
	props, listErr := r.listSchemasFromClient(ctx, client, schema.KindProperty, group)
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list properties")
		return
	}
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindProperty, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to database property")
			continue
		}
		*maxRevision = r.processInitialResource(schema.KindProperty, md.Spec.(*databasev1.Property), *maxRevision)
	}
}

func (r *SchemaRegistry) initializeIndexResourcesFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	group string, maxRevision *int64,
) {
	ruleProps, rulesErr := r.listSchemasFromClient(ctx, client, schema.KindIndexRule, group)
	if rulesErr != nil {
		r.l.Warn().Err(rulesErr).Str("group", group).Msg("failed to list index rules")
	} else {
		for _, prop := range ruleProps {
			md, convErr := ToSchema(schema.KindIndexRule, prop)
			if convErr != nil {
				r.l.Warn().Err(convErr).Msg("failed to convert property to index rule")
				continue
			}
			*maxRevision = r.processInitialResource(schema.KindIndexRule, md.Spec.(*databasev1.IndexRule), *maxRevision)
		}
	}
	bindingProps, bindingsErr := r.listSchemasFromClient(ctx, client, schema.KindIndexRuleBinding, group)
	if bindingsErr != nil {
		r.l.Warn().Err(bindingsErr).Str("group", group).Msg("failed to list index rule bindings")
	} else {
		for _, prop := range bindingProps {
			md, convErr := ToSchema(schema.KindIndexRuleBinding, prop)
			if convErr != nil {
				r.l.Warn().Err(convErr).Msg("failed to convert property to index rule binding")
				continue
			}
			*maxRevision = r.processInitialResource(schema.KindIndexRuleBinding, md.Spec.(*databasev1.IndexRuleBinding), *maxRevision)
		}
	}
}
