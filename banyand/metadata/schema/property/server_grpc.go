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

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type schemaManagementServer struct {
	schemav1.UnimplementedSchemaManagementServiceServer
	server *Server
	l      *logger.Logger
}

// InsertSchema inserts a new schema.
func (s *schemaManagementServer) InsertSchema(ctx context.Context, req *schemav1.InsertSchemaRequest) (*schemav1.InsertSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	if insertErr := s.server.insert(ctx, req.Property); insertErr != nil {
		s.l.Error().Err(insertErr).Msg("failed to insert schema")
		return nil, insertErr
	}
	return &schemav1.InsertSchemaResponse{}, nil
}

// UpdateSchema updates an existing schema.
func (s *schemaManagementServer) UpdateSchema(ctx context.Context, req *schemav1.UpdateSchemaRequest) (*schemav1.UpdateSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	if updateErr := s.server.update(ctx, req.Property); updateErr != nil {
		s.l.Error().Err(updateErr).Msg("failed to update schema")
		return nil, updateErr
	}
	return &schemav1.UpdateSchemaResponse{}, nil
}

// ListSchemas lists schemas based on the query request.
func (s *schemaManagementServer) ListSchemas(ctx context.Context, req *schemav1.ListSchemasRequest) (*schemav1.ListSchemasResponse, error) {
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	results, listErr := s.server.list(ctx, req.Query)
	if listErr != nil {
		s.l.Error().Err(listErr).Msg("failed to list schemas")
		return nil, listErr
	}
	props := make([]*propertyv1.Property, 0, len(results))
	deleteTimes := make([]int64, 0, len(results))
	for _, r := range results {
		props = append(props, r.Property)
		deleteTimes = append(deleteTimes, r.DeleteTime)
	}
	return &schemav1.ListSchemasResponse{Properties: props, DeleteTimes: deleteTimes}, nil
}

// GetSchema retrieves a single schema.
func (s *schemaManagementServer) GetSchema(ctx context.Context, req *schemav1.GetSchemaRequest) (*schemav1.GetSchemaResponse, error) {
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	if len(req.Query.Groups) == 0 {
		return nil, errInvalidRequest("groups is required")
	}
	if len(req.Query.Ids) == 0 {
		return nil, errInvalidRequest("ids is required")
	}
	prop, getErr := s.server.get(ctx, req.Query.Groups[0], req.Query.Name, req.Query.Ids[0])
	if getErr != nil {
		s.l.Error().Err(getErr).Msg("failed to get schema")
		return nil, getErr
	}
	return &schemav1.GetSchemaResponse{Properties: prop}, nil
}

// DeleteSchema deletes a schema.
func (s *schemaManagementServer) DeleteSchema(ctx context.Context, req *schemav1.DeleteSchemaRequest) (*schemav1.DeleteSchemaResponse, error) {
	if req.Delete == nil {
		return nil, errInvalidRequest("delete request is required")
	}
	found, deleteErr := s.server.delete(ctx, req.Delete.Group, req.Delete.Name, req.Delete.Id)
	if deleteErr != nil {
		s.l.Error().Err(deleteErr).Msg("failed to delete schema")
		return nil, deleteErr
	}
	return &schemav1.DeleteSchemaResponse{Found: found}, nil
}

// RepairSchema repairs a schema property on this node with specified deleteTime.
func (s *schemaManagementServer) RepairSchema(ctx context.Context, req *schemav1.RepairSchemaRequest) (*schemav1.RepairSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	id := property.GetPropertyID(req.Property)
	repairErr := s.server.propertyService.DirectRepair(ctx, 0, id, req.Property, req.DeleteTime)
	if repairErr != nil {
		s.l.Error().Err(repairErr).Msg("failed to repair schema")
		return nil, repairErr
	}
	return &schemav1.RepairSchemaResponse{}, nil
}

type schemaUpdateServer struct {
	schemav1.UnimplementedSchemaUpdateServiceServer
	server *Server
	l      *logger.Logger
}

// AggregateSchemaUpdates returns kind names of schemas that have been modified since the given revision.
// The criteria in the query should include mod_revision > sinceRevision condition.
func (s *schemaUpdateServer) AggregateSchemaUpdates(ctx context.Context, req *schemav1.AggregateSchemaUpdatesRequest) (*schemav1.AggregateSchemaUpdatesResponse, error) {
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	results, listErr := s.server.list(ctx, req.Query)
	if listErr != nil {
		s.l.Error().Err(listErr).Msg("failed to list schemas for updates")
		return nil, listErr
	}
	names := make([]string, 0, len(results))
	seen := make(map[string]struct{})
	for _, r := range results {
		kindName := r.Property.Metadata.GetName()
		if _, exists := seen[kindName]; !exists {
			names = append(names, kindName)
			seen[kindName] = struct{}{}
		}
	}
	return &schemav1.AggregateSchemaUpdatesResponse{Names: names}, nil
}

func errInvalidRequest(msg string) error {
	return &invalidRequestError{msg: msg}
}

type invalidRequestError struct {
	msg string
}

func (e *invalidRequestError) Error() string {
	return "invalid request: " + e.msg
}
