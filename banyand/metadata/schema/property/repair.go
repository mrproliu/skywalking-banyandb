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
	"fmt"
	"sync"

	"github.com/robfig/cron/v3"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type repairScheduler struct {
	server    *Server
	cron      *cron.Cron
	l         *logger.Logger
	enabled   bool
	cronExpr  string
	mu        sync.Mutex
	nodes     map[string]bool
	nodesLock sync.RWMutex
}

func newRepairScheduler(server *Server, cronExpr string, enabled bool, l *logger.Logger) (*repairScheduler, error) {
	if _, cronErr := cron.ParseStandard(cronExpr); cronErr != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", cronErr)
	}
	return &repairScheduler{
		server:   server,
		cron:     cron.New(),
		cronExpr: cronExpr,
		enabled:  enabled,
		l:        l,
		nodes:    make(map[string]bool),
	}, nil
}

// Start starts the repair scheduler.
func (r *repairScheduler) Start() error {
	if !r.enabled {
		r.l.Info().Msg("repair mechanism is disabled")
		return nil
	}

	_, cronErr := r.cron.AddFunc(r.cronExpr, func() {
		r.performRepair()
	})
	if cronErr != nil {
		return cronErr
	}

	r.cron.Start()
	r.l.Info().Str("cron", r.cronExpr).Msg("repair scheduler started")
	return nil
}

// Stop stops the repair scheduler.
func (r *repairScheduler) Stop() {
	r.cron.Stop()
	r.l.Info().Msg("repair scheduler stopped")
}

func (r *repairScheduler) performRepair() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.l.Info().Msg("starting repair property schema cycle")

	nodes := r.getActivatedNodes()
	messenger := r.server.propertyService.GetGossIPMessenger()
	err := messenger.Propagation(nodes, SchemaGroup, 0)
	if err != nil {
		r.l.Err(err).Msg("failed to propagate schema")
		return
	}
	r.l.Info().Int("node_count", len(nodes)).Msg("propagated schema to nodes success")
}

func (r *repairScheduler) getActivatedNodes() []string {
	r.nodesLock.RLock()
	defer r.nodesLock.RUnlock()
	nodes := make([]string, 0, len(r.nodes))
	for node := range r.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (r *repairScheduler) OnInit([]schema.Kind) (bool, []int64) {
	return false, nil
}

func (r *repairScheduler) OnAddOrUpdate(metadata schema.Metadata) {
	if metadata.Kind != schema.KindNode {
		return
	}
	node, ok := metadata.Spec.(*databasev1.Node)
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
	r.nodesLock.Lock()
	defer r.nodesLock.Unlock()
	r.nodes[node.Metadata.GetName()] = true
}

func (r *repairScheduler) OnDelete(metadata schema.Metadata) {
	if metadata.Kind != schema.KindNode {
		return
	}
	node, ok := metadata.Spec.(*databasev1.Node)
	if !ok {
		return
	}
	r.nodesLock.Lock()
	defer r.nodesLock.Unlock()
	delete(r.nodes, node.Metadata.GetName())
}
