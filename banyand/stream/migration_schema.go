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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// errSchemaPropertyMissing signals "no schema-property/_schema under
// backupDir". Returned by findStreamSchemaPropertyRoot when a caller asks for
// a backup-tree discovery but the snapshot omitted the catalog.
var errStreamSchemaPropertyMissing = errors.New("schema-property catalog not found in backup")

// streamSchemaPropDoc is one decoded doc emitted by walkStreamSchemaPropertyShard;
// the caller decides whether the kind / group matches what it wants.
type streamSchemaPropDoc struct {
	propID     string
	kindName   string
	group      string
	sourceJSON string
	modRev     int64
	deleted    bool
}

// walkStreamSchemaPropertyShard opens one shard of the backup's
// schema-property bluge index and invokes visit() for each doc.
func walkStreamSchemaPropertyShard(shardPath string, visit func(streamSchemaPropDoc) error) error {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(shardPath))
	if err != nil {
		return fmt.Errorf("open bluge reader: %w", err)
	}
	defer func() { _ = reader.Close() }()
	dmi, err := reader.Search(context.Background(),
		bluge.NewTopNSearch(streamSchemaSearchSize, bluge.NewMatchAllQuery()))
	if err != nil {
		return fmt.Errorf("search schema docs: %w", err)
	}
	for {
		next, err := dmi.Next()
		if err != nil {
			return fmt.Errorf("iterate schema docs: %w", err)
		}
		if next == nil {
			return nil
		}
		var sourceBytes []byte
		var deleted bool
		if err := next.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case streamSchemaSourceField:
				sourceBytes = append([]byte(nil), value...)
			case streamSchemaDeletedTag:
				if len(value) > 0 {
					deleted = true
				}
			}
			return true
		}); err != nil {
			return fmt.Errorf("visit schema doc: %w", err)
		}
		if len(sourceBytes) == 0 {
			continue
		}
		var prop propertyv1.Property
		if err := protojson.Unmarshal(sourceBytes, &prop); err != nil {
			return fmt.Errorf("unmarshal property doc in %s (%d source bytes): %w",
				shardPath, len(sourceBytes), err)
		}
		var group, srcJSON string
		for _, t := range prop.GetTags() {
			sv := t.GetValue().GetStr()
			if sv == nil {
				continue
			}
			switch t.GetKey() {
			case "group":
				group = sv.GetValue()
			case "source":
				srcJSON = sv.GetValue()
			}
		}
		if vErr := visit(streamSchemaPropDoc{
			propID:     prop.GetId(),
			kindName:   prop.GetMetadata().GetName(),
			group:      group,
			sourceJSON: srcJSON,
			modRev:     prop.GetMetadata().GetModRevision(),
			deleted:    deleted,
		}); vErr != nil {
			return vErr
		}
	}
}

const (
	streamSchemaSourceField = "_source"
	streamSchemaDeletedTag  = "_deleted"
	streamSchemaShardPrefix = "shard-"
	streamSchemaSearchSize  = 200000
)

// resolveStreamSchemaRoot picks the `_schema` bluge directory to read schemas from.
func resolveStreamSchemaRoot(backupDir, date, schemaPropertyPath string) (string, error) {
	if schemaPropertyPath != "" {
		return schemaPropertyPath, nil
	}
	return findStreamSchemaPropertyRoot(backupDir, date)
}

// findStreamSchemaPropertyRoot walks <backup>/<node>/<date>/ and returns the
// first schema-property/_schema directory it finds.
func findStreamSchemaPropertyRoot(backupDir, date string) (string, error) {
	nodes, err := os.ReadDir(backupDir)
	if err != nil {
		return "", fmt.Errorf("read backup dir %q: %w", backupDir, err)
	}
	tryDate := func(nodeRoot, d string) (string, bool) {
		p := filepath.Join(nodeRoot, d, backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup)
		info, statErr := os.Stat(p)
		return p, statErr == nil && info.IsDir()
	}
	for _, node := range nodes {
		if !node.IsDir() {
			continue
		}
		nodeRoot := filepath.Join(backupDir, node.Name())
		candidates := []string{date}
		if date == "" {
			dates, readErr := os.ReadDir(nodeRoot)
			if readErr != nil {
				continue
			}
			candidates = candidates[:0]
			for _, d := range dates {
				if d.IsDir() {
					candidates = append(candidates, d.Name())
				}
			}
		}
		for _, c := range candidates {
			if p, ok := tryDate(nodeRoot, c); ok {
				return p, nil
			}
		}
	}
	dateClause := ""
	if date != "" {
		dateClause = fmt.Sprintf(" for date %q", date)
	}
	return "", errors.WithMessagef(errStreamSchemaPropertyMissing,
		"no %s/%s directory found under %q%s (only hot/schema-server node backups carry schemas)",
		backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup, backupDir, dateClause)
}

// walkStreamSchemaShards resolves the schema root from backupDir/date/schemaPropertyPath,
// reads its shard-* subdirectories, and invokes fn(shardPath) for each. The caller owns
// all scanning and candidate-merging logic; this helper owns only the repeated
// resolve-readDir-filter boilerplate that would otherwise appear in every loader.
func walkStreamSchemaShards(backupDir, date, schemaPropertyPath string, fn func(shardPath string) error) error {
	schemaRoot, err := resolveStreamSchemaRoot(backupDir, date, schemaPropertyPath)
	if err != nil {
		return err
	}
	shards, err := os.ReadDir(schemaRoot)
	if err != nil {
		return fmt.Errorf("read schema-property root %q: %w", schemaRoot, err)
	}
	for _, sh := range shards {
		if !sh.IsDir() || !strings.HasPrefix(sh.Name(), streamSchemaShardPrefix) {
			continue
		}
		if fnErr := fn(filepath.Join(schemaRoot, sh.Name())); fnErr != nil {
			return fnErr
		}
	}
	return nil
}

// streamSchemaInfo is the subset of a stream's schema MigrationCopy cares about.
type streamSchemaInfo struct {
	Group          string
	Name           string
	EntityTagNames []string
	EntityFamily   string
	TagFamilies    []streamTagFamilyInfo
}

type streamTagFamilyInfo struct {
	Name string
	Tags []string
}

// streamSchemaCandidate captures the best (highest mod_revision) doc seen for
// one propID while iterating the schema-property bluge index.
type streamSchemaCandidate struct {
	info    *streamSchemaInfo
	modRev  int64
	deleted bool
}

// loadStreamSchemasFromSchemaCatalog reads every stream schema under the
// given groups directly from the schema-property bluge index.
// Returns (group -> stream-name -> schema).
func loadStreamSchemasFromSchemaCatalog(backupDir, date, schemaPropertyPath string, groups []string) (map[string]map[string]*streamSchemaInfo, error) {
	if backupDir == "" && schemaPropertyPath == "" {
		return nil, errors.New("either backup-dir or schema-property-path is required")
	}
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	candidates := make(map[string]*streamSchemaCandidate)
	if walkErr := walkStreamSchemaShards(backupDir, date, schemaPropertyPath, func(shardPath string) error {
		if loadErr := loadStreamSchemasFromShard(shardPath, wanted, candidates); loadErr != nil {
			return fmt.Errorf("load schemas from %s: %w", shardPath, loadErr)
		}
		return nil
	}); walkErr != nil {
		return nil, walkErr
	}
	byGroup := make(map[string][]*streamSchemaInfo)
	for _, c := range candidates {
		if c.deleted || c.info == nil {
			continue
		}
		byGroup[c.info.Group] = append(byGroup[c.info.Group], c.info)
	}
	out := make(map[string]map[string]*streamSchemaInfo, len(groups))
	for _, group := range groups {
		list := byGroup[group]
		byName := make(map[string]*streamSchemaInfo, len(list))
		for _, s := range list {
			byName[s.Name] = s
		}
		out[group] = byName
	}
	return out, nil
}

func loadStreamSchemasFromShard(shardPath string, wantedGroups map[string]bool,
	candidates map[string]*streamSchemaCandidate,
) error {
	return walkStreamSchemaPropertyShard(shardPath, func(d streamSchemaPropDoc) error {
		if d.kindName != schema.KindStream.String() {
			return nil
		}
		if len(wantedGroups) > 0 && !wantedGroups[d.group] {
			return nil
		}
		if d.sourceJSON == "" {
			return fmt.Errorf("schema property %q missing source tag", d.propID)
		}
		var stream databasev1.Stream
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &stream); uErr != nil {
			return fmt.Errorf("unmarshal stream %q source: %w", d.propID, uErr)
		}
		if existing, ok := candidates[d.propID]; ok && existing.modRev >= d.modRev {
			return nil
		}
		candidates[d.propID] = &streamSchemaCandidate{
			info:    streamSchemaInfoFromProto(d.group, &stream),
			modRev:  d.modRev,
			deleted: d.deleted,
		}
		return nil
	})
}

func streamSchemaInfoFromProto(group string, s *databasev1.Stream) *streamSchemaInfo {
	si := &streamSchemaInfo{
		Group:          group,
		Name:           s.GetMetadata().GetName(),
		EntityTagNames: append([]string(nil), s.GetEntity().GetTagNames()...),
	}
	for _, tf := range s.GetTagFamilies() {
		tags := make([]string, 0, len(tf.GetTags()))
		for _, t := range tf.GetTags() {
			tags = append(tags, t.GetName())
			for _, en := range si.EntityTagNames {
				if t.GetName() == en {
					si.EntityFamily = tf.GetName()
				}
			}
		}
		si.TagFamilies = append(si.TagFamilies, streamTagFamilyInfo{Name: tf.GetName(), Tags: tags})
	}
	return si
}

// streamIndexLocator bundles everything the element-index rebuild needs for one
// stream: the parsed locators (entity set + per-family tag rules) plus the flat
// list of bound INVERTED index rules. It is built offline from schema-property.
type streamIndexLocator struct {
	Locators   partition.IndexRuleLocator
	IndexRules []*databasev1.IndexRule
	Entity     *databasev1.Entity
	Families   []*databasev1.TagFamilySpec
}

// HasEntityIndexRule reports whether any index rule references an entity tag, so
// the slow-path rebuild knows it must resolve seriesID -> EntityValues.
func (l *streamIndexLocator) HasEntityIndexRule() bool {
	for _, ir := range l.IndexRules {
		for _, tagName := range ir.GetTags() {
			if _, ok := l.Locators.EntitySet[tagName]; ok {
				return true
			}
		}
	}
	return false
}

// streamLocatorCands holds the three candidate maps collected by a schema shard scan.
type streamLocatorCands struct {
	streams  map[string]*streamLocCand
	rules    map[string]*streamRuleCand
	bindings map[string]*streamBindingCand
}

type streamLocCand struct {
	stream *databasev1.Stream
	group  string
	modRev int64
	del    bool
}

type streamRuleCand struct {
	rule   *databasev1.IndexRule
	group  string
	modRev int64
	del    bool
}

type streamBindingCand struct {
	binding *databasev1.IndexRuleBinding
	group   string
	modRev  int64
	del     bool
}

// foldStreamLocatorDoc merges one schema-property doc into the candidate maps.
func foldStreamLocatorDoc(d streamSchemaPropDoc, wanted map[string]bool, c *streamLocatorCands) error {
	if d.sourceJSON == "" {
		return nil
	}
	switch d.kindName {
	case schema.KindStream.String():
		// Only streams are scoped to the requested groups; index rules and
		// bindings are loaded regardless of group so a rule bound to a wanted
		// stream from another group still resolves.
		if len(wanted) > 0 && !wanted[d.group] {
			return nil
		}
		var s databasev1.Stream
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &s); uErr != nil {
			return fmt.Errorf("unmarshal stream %q: %w", d.propID, uErr)
		}
		if cur, ok := c.streams[d.propID]; ok && cur.modRev >= d.modRev {
			return nil
		}
		c.streams[d.propID] = &streamLocCand{stream: &s, group: d.group, modRev: d.modRev, del: d.deleted}
	case schema.KindIndexRule.String():
		var r databasev1.IndexRule
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &r); uErr != nil {
			return fmt.Errorf("unmarshal index rule %q: %w", d.propID, uErr)
		}
		if cur, ok := c.rules[d.propID]; ok && cur.modRev >= d.modRev {
			return nil
		}
		c.rules[d.propID] = &streamRuleCand{rule: &r, group: d.group, modRev: d.modRev, del: d.deleted}
	case schema.KindIndexRuleBinding.String():
		var b databasev1.IndexRuleBinding
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &b); uErr != nil {
			return fmt.Errorf("unmarshal index rule binding %q: %w", d.propID, uErr)
		}
		if cur, ok := c.bindings[d.propID]; ok && cur.modRev >= d.modRev {
			return nil
		}
		c.bindings[d.propID] = &streamBindingCand{binding: &b, group: d.group, modRev: d.modRev, del: d.deleted}
	}
	return nil
}

// buildStreamRulesByGroupName indexes non-deleted rules by (group, ruleName).
func buildStreamRulesByGroupName(rules map[string]*streamRuleCand) map[string]map[string]*databasev1.IndexRule {
	out := map[string]map[string]*databasev1.IndexRule{}
	for _, c := range rules {
		if c.del || c.rule == nil {
			continue
		}
		byName := out[c.group]
		if byName == nil {
			byName = map[string]*databasev1.IndexRule{}
			out[c.group] = byName
		}
		byName[c.rule.GetMetadata().GetName()] = c.rule
	}
	return out
}

// buildStreamBoundRules builds the (group -> stream -> rule-name-set) index from
// non-deleted CATALOG_STREAM bindings.
func buildStreamBoundRules(bindings map[string]*streamBindingCand) map[string]map[string]map[string]struct{} {
	out := map[string]map[string]map[string]struct{}{}
	for _, c := range bindings {
		if c.del || c.binding == nil {
			continue
		}
		subject := c.binding.GetSubject()
		if subject.GetCatalog() != commonv1.Catalog_CATALOG_STREAM {
			continue
		}
		byStream := out[c.group]
		if byStream == nil {
			byStream = map[string]map[string]struct{}{}
			out[c.group] = byStream
		}
		set := byStream[subject.GetName()]
		if set == nil {
			set = map[string]struct{}{}
			byStream[subject.GetName()] = set
		}
		for _, ruleName := range c.binding.GetRules() {
			set[ruleName] = struct{}{}
		}
	}
	return out
}

// resolveStreamLocatorsFromCands builds the final (group -> stream-name -> locator)
// map from the collected candidate maps and the pre-built rule/binding indexes.
func resolveStreamLocatorsFromCands(
	groups []string,
	streams map[string]*streamLocCand,
	rulesByGroupName map[string]map[string]*databasev1.IndexRule,
	boundRules map[string]map[string]map[string]struct{},
) map[string]map[string]*streamIndexLocator {
	out := make(map[string]map[string]*streamIndexLocator, len(groups))
	for _, group := range groups {
		out[group] = map[string]*streamIndexLocator{}
	}
	for _, c := range streams {
		if c.del || c.stream == nil {
			continue
		}
		streamName := c.stream.GetMetadata().GetName()
		var indexRules []*databasev1.IndexRule
		if byStream := boundRules[c.group]; byStream != nil {
			if ruleNames := byStream[streamName]; ruleNames != nil {
				byName := rulesByGroupName[c.group]
				for ruleName := range ruleNames {
					if r, ok := byName[ruleName]; ok {
						indexRules = append(indexRules, r)
					}
				}
			}
		}
		locators, _ := partition.ParseIndexRuleLocators(c.stream.GetEntity(), c.stream.GetTagFamilies(), indexRules, false)
		if out[c.group] == nil {
			out[c.group] = map[string]*streamIndexLocator{}
		}
		out[c.group][streamName] = &streamIndexLocator{
			Locators:   locators,
			IndexRules: indexRules,
			Entity:     c.stream.GetEntity(),
			Families:   c.stream.GetTagFamilies(),
		}
	}
	return out
}

// loadStreamIndexLocators reads, for each requested group, the stream protos plus
// the INVERTED index rules bound to each stream (via IndexRuleBinding whose
// Subject.Catalog==CATALOG_STREAM), and builds one streamIndexLocator per stream.
// Returns (group -> stream-name -> locator). This is the offline analog of the
// indexSchema the write path keeps in memory.
func loadStreamIndexLocators(backupDir, date, schemaPropertyPath string, groups []string) (map[string]map[string]*streamIndexLocator, error) {
	if backupDir == "" && schemaPropertyPath == "" {
		return nil, errors.New("either backup-dir or schema-property-path is required")
	}
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	cands := streamLocatorCands{
		streams:  map[string]*streamLocCand{},
		rules:    map[string]*streamRuleCand{},
		bindings: map[string]*streamBindingCand{},
	}
	if walkErr := walkStreamSchemaShards(backupDir, date, schemaPropertyPath, func(shardPath string) error {
		return walkStreamSchemaPropertyShard(shardPath, func(d streamSchemaPropDoc) error {
			return foldStreamLocatorDoc(d, wanted, &cands)
		})
	}); walkErr != nil {
		return nil, walkErr
	}

	rulesByGroupName := buildStreamRulesByGroupName(cands.rules)
	boundRules := buildStreamBoundRules(cands.bindings)
	return resolveStreamLocatorsFromCands(groups, cands.streams, rulesByGroupName, boundRules), nil
}

// groupOptsStreamCandidate is the analog of schemaCandidate for group
// ResourceOpts: tracks the best mod_revision per propID.
type groupOptsStreamCandidate struct {
	opts    *commonv1.ResourceOpts
	name    string
	modRev  int64
	deleted bool
}

// loadGroupResourceOptsFromStreamSchema scans the schema-property bluge index
// for commonv1.Group docs and returns each requested group's ResourceOpts.
func loadGroupResourceOptsFromStreamSchema(backupDir, date, schemaPropertyPath string, groups []string) (map[string]*commonv1.ResourceOpts, error) {
	if backupDir == "" && schemaPropertyPath == "" {
		return nil, errors.New("either backup-dir or schema-property-path is required")
	}
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	candidates := map[string]*groupOptsStreamCandidate{}
	if walkErr := walkStreamSchemaShards(backupDir, date, schemaPropertyPath, func(shardPath string) error {
		return loadGroupResourceOptsFromStreamShard(shardPath, wanted, candidates)
	}); walkErr != nil {
		return nil, walkErr
	}
	out := map[string]*commonv1.ResourceOpts{}
	for _, c := range candidates {
		if c.deleted || c.opts == nil {
			continue
		}
		out[c.name] = c.opts
	}
	return out, nil
}

func loadGroupResourceOptsFromStreamShard(
	shardPath string,
	wanted map[string]bool,
	candidates map[string]*groupOptsStreamCandidate,
) error {
	return walkStreamSchemaPropertyShard(shardPath, func(d streamSchemaPropDoc) error {
		if d.kindName != schema.KindGroup.String() || d.sourceJSON == "" {
			return nil
		}
		var g commonv1.Group
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &g); uErr != nil {
			return nil
		}
		name := g.GetMetadata().GetName()
		if !wanted[name] {
			return nil
		}
		if cur, ok := candidates[d.propID]; ok && cur.modRev >= d.modRev {
			return nil
		}
		candidates[d.propID] = &groupOptsStreamCandidate{
			opts:    g.GetResourceOpts(),
			name:    name,
			modRev:  d.modRev,
			deleted: d.deleted,
		}
		return nil
	})
}

// groupCatalogCandidate tracks the best-revision Group doc for catalog detection.
type groupCatalogCandidate struct {
	name    string
	modRev  int64
	catalog commonv1.Catalog
	deleted bool
}

// DetectGroupCatalogs reads the schema-property bluge index and returns the
// Catalog type (STREAM / MEASURE / TRACE) for each requested group.
// Used by the CLI to auto-route between measure and stream migration paths.
func DetectGroupCatalogs(backupDir, date, schemaPropertyPath string, groups []string) (map[string]commonv1.Catalog, error) {
	if backupDir == "" && schemaPropertyPath == "" {
		return nil, errors.New("either backup-dir or schema-property-path is required")
	}
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	candidates := map[string]*groupCatalogCandidate{}
	if walkErr := walkStreamSchemaShards(backupDir, date, schemaPropertyPath, func(shardPath string) error {
		return detectGroupCatalogsFromShard(shardPath, wanted, candidates)
	}); walkErr != nil {
		return nil, walkErr
	}
	out := make(map[string]commonv1.Catalog, len(groups))
	for _, c := range candidates {
		if c.deleted {
			continue
		}
		out[c.name] = c.catalog
	}
	return out, nil
}

func detectGroupCatalogsFromShard(shardPath string, wanted map[string]bool, candidates map[string]*groupCatalogCandidate) error {
	return walkStreamSchemaPropertyShard(shardPath, func(d streamSchemaPropDoc) error {
		if d.kindName != schema.KindGroup.String() || d.sourceJSON == "" {
			return nil
		}
		var g commonv1.Group
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &g); uErr != nil {
			return nil
		}
		name := g.GetMetadata().GetName()
		if !wanted[name] {
			return nil
		}
		if cur, ok := candidates[d.propID]; ok && cur.modRev >= d.modRev {
			return nil
		}
		candidates[d.propID] = &groupCatalogCandidate{
			catalog: g.GetCatalog(),
			name:    name,
			modRev:  d.modRev,
			deleted: d.deleted,
		}
		return nil
	})
}
