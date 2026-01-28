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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestSchemaCache_NewSchemaCache(t *testing.T) {
	cache := newSchemaCache()
	assert.NotNil(t, cache)
	assert.NotNil(t, cache.entries)
	assert.Equal(t, int64(0), cache.maxRevision)
}

func TestSchemaCache_Update(t *testing.T) {
	cache := newSchemaCache()

	entry1 := cacheEntry{
		valueHash:   123456,
		modRevision: 100,
		kind:        schema.KindStream,
		group:       "test-group",
		name:        "test-stream",
	}

	changed := cache.Update("stream_test-group/test-stream", entry1)
	assert.True(t, changed, "First update should return true")
	assert.Equal(t, int64(100), cache.GetMaxRevision())

	changed = cache.Update("stream_test-group/test-stream", entry1)
	assert.False(t, changed, "Same entry update should return false")

	entry2 := cacheEntry{
		valueHash:   789012,
		modRevision: 150,
		kind:        schema.KindStream,
		group:       "test-group",
		name:        "test-stream",
	}
	changed = cache.Update("stream_test-group/test-stream", entry2)
	assert.True(t, changed, "Updated entry should return true")
	assert.Equal(t, int64(150), cache.GetMaxRevision())

	oldEntry := cacheEntry{
		valueHash:   111111,
		modRevision: 50,
		kind:        schema.KindStream,
		group:       "test-group",
		name:        "test-stream",
	}
	changed = cache.Update("stream_test-group/test-stream", oldEntry)
	assert.False(t, changed, "Older revision should return false")
	assert.Equal(t, int64(150), cache.GetMaxRevision())
}

func TestSchemaCache_Delete(t *testing.T) {
	cache := newSchemaCache()

	entry := cacheEntry{
		valueHash:   123456,
		modRevision: 100,
		kind:        schema.KindStream,
		group:       "test-group",
		name:        "test-stream",
	}
	cache.Update("stream_test-group/test-stream", entry)

	deleted := cache.Delete("stream_test-group/test-stream")
	assert.True(t, deleted, "Deleting existing entry should return true")

	deleted = cache.Delete("stream_test-group/test-stream")
	assert.False(t, deleted, "Deleting non-existing entry should return false")

	deleted = cache.Delete("non-existent-key")
	assert.False(t, deleted, "Deleting non-existent key should return false")
}

func TestSchemaCache_GetMaxRevision(t *testing.T) {
	cache := newSchemaCache()
	assert.Equal(t, int64(0), cache.GetMaxRevision())

	cache.Update("key1", cacheEntry{modRevision: 100})
	assert.Equal(t, int64(100), cache.GetMaxRevision())

	cache.Update("key2", cacheEntry{modRevision: 200})
	assert.Equal(t, int64(200), cache.GetMaxRevision())

	cache.Update("key3", cacheEntry{modRevision: 150})
	assert.Equal(t, int64(200), cache.GetMaxRevision())
}

func TestSchemaCache_GetAllEntries(t *testing.T) {
	cache := newSchemaCache()

	entries := cache.GetAllEntries()
	assert.Empty(t, entries)

	cache.Update("key1", cacheEntry{modRevision: 100, kind: schema.KindStream})
	cache.Update("key2", cacheEntry{modRevision: 200, kind: schema.KindMeasure})

	entries = cache.GetAllEntries()
	assert.Len(t, entries, 2)
	assert.Contains(t, entries, "key1")
	assert.Contains(t, entries, "key2")

	entries["key3"] = cacheEntry{}
	originalEntries := cache.GetAllEntries()
	assert.Len(t, originalEntries, 2)
}

func TestSchemaCache_Concurrency(t *testing.T) {
	cache := newSchemaCache()
	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				entry := cacheEntry{
					valueHash:   uint64(idx*1000 + j),
					modRevision: int64(idx*1000 + j),
					kind:        schema.KindStream,
				}
				cache.Update("concurrent-key", entry)
				cache.GetMaxRevision()
				cache.GetAllEntries()
			}
		}(i)
	}
	wg.Wait()

	assert.True(t, cache.GetMaxRevision() > 0)
}

func TestSchemaCache_GetEntriesByKind(t *testing.T) {
	cache := newSchemaCache()

	entries := cache.GetEntriesByKind(schema.KindStream)
	assert.Empty(t, entries)

	cache.Update("stream_group1/stream1", cacheEntry{modRevision: 100, kind: schema.KindStream, group: "group1", name: "stream1"})
	cache.Update("stream_group1/stream2", cacheEntry{modRevision: 101, kind: schema.KindStream, group: "group1", name: "stream2"})
	cache.Update("measure_group1/measure1", cacheEntry{modRevision: 102, kind: schema.KindMeasure, group: "group1", name: "measure1"})
	cache.Update("group_group1", cacheEntry{modRevision: 103, kind: schema.KindGroup, group: "", name: "group1"})

	streamEntries := cache.GetEntriesByKind(schema.KindStream)
	assert.Len(t, streamEntries, 2)
	assert.Contains(t, streamEntries, "stream_group1/stream1")
	assert.Contains(t, streamEntries, "stream_group1/stream2")

	measureEntries := cache.GetEntriesByKind(schema.KindMeasure)
	assert.Len(t, measureEntries, 1)
	assert.Contains(t, measureEntries, "measure_group1/measure1")

	groupEntries := cache.GetEntriesByKind(schema.KindGroup)
	assert.Len(t, groupEntries, 1)
	assert.Contains(t, groupEntries, "group_group1")

	traceEntries := cache.GetEntriesByKind(schema.KindTrace)
	assert.Empty(t, traceEntries)

	streamEntries["new_key"] = cacheEntry{}
	originalStreamEntries := cache.GetEntriesByKind(schema.KindStream)
	assert.Len(t, originalStreamEntries, 2)
}

func TestSchemaRegistry_HandleDeletion(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	handler := newMockEventHandler()
	registry.addHandler(schema.KindStream, handler)

	entry := cacheEntry{
		valueHash:   123456,
		modRevision: 100,
		kind:        schema.KindStream,
		group:       "test-group",
		name:        "test-stream",
	}
	propID := "stream_test-group/test-stream"
	registry.cache.Update(propID, entry)

	registry.handleDeletion(schema.KindStream, propID, entry)

	assert.Equal(t, int32(1), handler.deleteCalled.Load())
	handler.mu.Lock()
	assert.Equal(t, "test-stream", handler.lastDeleteMeta.Name)
	assert.Equal(t, "test-group", handler.lastDeleteMeta.Group)
	assert.Equal(t, schema.KindStream, handler.lastDeleteMeta.Kind)
	assert.Equal(t, int64(100), handler.lastDeleteMeta.ModRevision)
	assert.Nil(t, handler.lastDeleteMeta.Spec)
	handler.mu.Unlock()

	allEntries := registry.cache.GetAllEntries()
	assert.NotContains(t, allEntries, propID)
}

func TestSchemaRegistry_HandleDeletion_NotInCache(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	handler := newMockEventHandler()
	registry.addHandler(schema.KindStream, handler)

	entry := cacheEntry{
		valueHash:   123456,
		modRevision: 100,
		kind:        schema.KindStream,
		group:       "test-group",
		name:        "test-stream",
	}
	propID := "stream_test-group/test-stream"

	registry.handleDeletion(schema.KindStream, propID, entry)

	assert.Equal(t, int32(0), handler.deleteCalled.Load())
}

func TestParsePropertyID(t *testing.T) {
	tests := []struct {
		name          string
		propID        string
		expectedKind  schema.Kind
		expectedGroup string
		expectedName  string
	}{
		{
			name:          "stream with group",
			propID:        "stream_mygroup/mystream",
			expectedKind:  schema.KindStream,
			expectedGroup: "mygroup",
			expectedName:  "mystream",
		},
		{
			name:          "measure with group",
			propID:        "measure_metrics/cpu",
			expectedKind:  schema.KindMeasure,
			expectedGroup: "metrics",
			expectedName:  "cpu",
		},
		{
			name:          "group without group field",
			propID:        "group_mygroup",
			expectedKind:  schema.KindGroup,
			expectedGroup: "",
			expectedName:  "mygroup",
		},
		{
			name:          "node without group field",
			propID:        "node_node-1",
			expectedKind:  schema.KindNode,
			expectedGroup: "",
			expectedName:  "node-1",
		},
		{
			name:          "indexRule with group",
			propID:        "indexRule_default/rule1",
			expectedKind:  schema.KindIndexRule,
			expectedGroup: "default",
			expectedName:  "rule1",
		},
		{
			name:          "invalid - no underscore",
			propID:        "invalidformat",
			expectedKind:  0,
			expectedGroup: "",
			expectedName:  "",
		},
		{
			name:          "invalid - unknown kind",
			propID:        "unknown_group/name",
			expectedKind:  0,
			expectedGroup: "",
			expectedName:  "",
		},
		{
			name:          "empty string",
			propID:        "",
			expectedKind:  0,
			expectedGroup: "",
			expectedName:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, group, name := parsePropertyID(tt.propID)
			assert.Equal(t, tt.expectedKind, kind)
			assert.Equal(t, tt.expectedGroup, group)
			assert.Equal(t, tt.expectedName, name)
		})
	}
}

func TestBuildModRevisionCriteria(t *testing.T) {
	criteria := buildModRevisionCriteria(0)
	assert.Nil(t, criteria, "sinceRevision 0 should return nil")

	criteria = buildModRevisionCriteria(-1)
	assert.Nil(t, criteria, "sinceRevision -1 should return nil")

	criteria = buildModRevisionCriteria(100)
	require.NotNil(t, criteria)
	condition := criteria.GetCondition()
	require.NotNil(t, condition)
	assert.Equal(t, TagKeyModRevision, condition.Name)
	assert.Equal(t, modelv1.Condition_BINARY_OP_GT, condition.Op)
	assert.Equal(t, int64(100), condition.Value.GetInt().GetValue())

	criteria = buildModRevisionCriteria(999999)
	require.NotNil(t, criteria)
	assert.Equal(t, int64(999999), criteria.GetCondition().Value.GetInt().GetValue())
}

func TestGetGroupFromTags(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-group"}}}},
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-name"}}}},
	}
	group := getGroupFromTags(tags)
	assert.Equal(t, "test-group", group)

	tags = []*modelv1.Tag{
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-name"}}}},
	}
	group = getGroupFromTags(tags)
	assert.Equal(t, "", group)

	group = getGroupFromTags(nil)
	assert.Equal(t, "", group)

	group = getGroupFromTags([]*modelv1.Tag{})
	assert.Equal(t, "", group)
}

func TestGetNameFromTags(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-group"}}}},
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-name"}}}},
	}
	name := getNameFromTags(tags)
	assert.Equal(t, "test-name", name)

	tags = []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-group"}}}},
	}
	name = getNameFromTags(tags)
	assert.Equal(t, "", name)

	name = getNameFromTags(nil)
	assert.Equal(t, "", name)
}

func TestNewSchemaRegistry(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	require.NotNil(t, registry)
	assert.NotNil(t, registry.nodeConns)
	assert.NotNil(t, registry.handlers)
	assert.NotNil(t, registry.cache)
	assert.NotNil(t, registry.closer)
	assert.Equal(t, defaultSyncInterval, registry.syncInterval)

	closeErr := registry.Close()
	assert.NoError(t, closeErr)
}

type mockEventHandler struct {
	initCalled          *atomic.Int32
	addOrUpdateCalled   *atomic.Int32
	deleteCalled        *atomic.Int32
	lastAddOrUpdateMeta schema.Metadata
	lastDeleteMeta      schema.Metadata
	mu                  sync.Mutex
}

func newMockEventHandler() *mockEventHandler {
	return &mockEventHandler{
		initCalled:        &atomic.Int32{},
		addOrUpdateCalled: &atomic.Int32{},
		deleteCalled:      &atomic.Int32{},
	}
}

func (m *mockEventHandler) OnInit(_ []schema.Kind) (bool, []int64) {
	m.initCalled.Add(1)
	return false, nil
}

func (m *mockEventHandler) OnAddOrUpdate(md schema.Metadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addOrUpdateCalled.Add(1)
	m.lastAddOrUpdateMeta = md
}

func (m *mockEventHandler) OnDelete(md schema.Metadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalled.Add(1)
	m.lastDeleteMeta = md
}

func TestSchemaRegistry_RegisterHandler(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	handler := newMockEventHandler()

	registry.RegisterHandler("test", schema.KindStream, handler)
	assert.Equal(t, int32(1), handler.initCalled.Load())

	registry.mu.RLock()
	handlers := registry.handlers[schema.KindStream]
	registry.mu.RUnlock()
	assert.Len(t, handlers, 1)
}

func TestSchemaRegistry_RegisterHandler_MultipleKinds(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	handler := newMockEventHandler()

	registry.RegisterHandler("test", schema.KindStream|schema.KindMeasure, handler)
	assert.Equal(t, int32(1), handler.initCalled.Load())

	registry.mu.RLock()
	streamHandlers := registry.handlers[schema.KindStream]
	measureHandlers := registry.handlers[schema.KindMeasure]
	registry.mu.RUnlock()
	assert.Len(t, streamHandlers, 1)
	assert.Len(t, measureHandlers, 1)
}

func TestSchemaRegistry_OnInit(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	ok, revisions := registry.OnInit([]schema.Kind{schema.KindStream, schema.KindMeasure})
	assert.False(t, ok)
	assert.Nil(t, revisions)
}

func TestSchemaRegistry_AddHandler(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	handler1 := newMockEventHandler()
	handler2 := newMockEventHandler()

	registry.addHandler(schema.KindStream, handler1)
	registry.addHandler(schema.KindStream, handler2)

	registry.mu.RLock()
	handlers := registry.handlers[schema.KindStream]
	registry.mu.RUnlock()
	assert.Len(t, handlers, 2)
}

func TestSchemaRegistry_NotifyHandlers(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	handler := newMockEventHandler()
	registry.addHandler(schema.KindStream, handler)

	md := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:  schema.KindStream,
			Name:  "test-stream",
			Group: "test-group",
		},
	}

	registry.notifyHandlers(schema.KindStream, md, false)
	assert.Equal(t, int32(1), handler.addOrUpdateCalled.Load())
	handler.mu.Lock()
	assert.Equal(t, "test-stream", handler.lastAddOrUpdateMeta.Name)
	handler.mu.Unlock()

	registry.notifyHandlers(schema.KindStream, md, true)
	assert.Equal(t, int32(1), handler.deleteCalled.Load())
	handler.mu.Lock()
	assert.Equal(t, "test-stream", handler.lastDeleteMeta.Name)
	handler.mu.Unlock()
}

func TestSchemaRegistry_NotifyHandlers_NoHandlers(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)
	defer registry.Close()

	md := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:  schema.KindStream,
			Name:  "test-stream",
			Group: "test-group",
		},
	}

	registry.notifyHandlers(schema.KindStream, md, false)
}

func TestSchemaRegistry_Close(t *testing.T) {
	registry := NewSchemaRegistryClient(nil, 5*time.Second)

	closeErr := registry.Close()
	assert.NoError(t, closeErr)
}

func TestComputePropertyHash(t *testing.T) {
	prop1 := createTestProperty("id1", "group1", "name1")
	prop2 := createTestProperty("id1", "group1", "name1")
	prop3 := createTestProperty("id2", "group2", "name2")

	hash1 := computePropertyHash(prop1)
	hash2 := computePropertyHash(prop2)
	hash3 := computePropertyHash(prop3)

	assert.Equal(t, hash1, hash2, "Same properties should have same hash")
	assert.NotEqual(t, hash1, hash3, "Different properties should have different hash")
	assert.NotZero(t, hash1)
}

func TestSchemaCache_Update_HashChange(t *testing.T) {
	cache := newSchemaCache()

	entry1 := cacheEntry{
		valueHash:   100,
		modRevision: 50,
		kind:        schema.KindStream,
	}
	cache.Update("key1", entry1)

	entry2 := cacheEntry{
		valueHash:   200,
		modRevision: 50,
		kind:        schema.KindStream,
	}
	changed := cache.Update("key1", entry2)
	assert.True(t, changed, "Hash change should trigger update")
}

func createTestProperty(id, group, name string) *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group: group,
			Name:  name,
		},
		Id: id,
		Tags: []*modelv1.Tag{
			{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: group}}}},
			{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: name}}}},
		},
	}
}
