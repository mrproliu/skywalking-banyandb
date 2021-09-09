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
	"bytes"
	"embed"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

type shardStruct struct {
	id       common.ShardID
	location []string
	elements []string
}

type shardsForTest []shardStruct

func Test_Stream_SelectShard(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(tester)
	defer deferFunc()
	_ = setupQueryData(tester, "multiple_shards.json", s)
	tests := []struct {
		name         string
		entity       tsdb.Entity
		wantShardNum int
		wantErr      bool
	}{
		{
			name:         "all shards",
			wantShardNum: 2,
		},
		{
			name:         "select a shard",
			entity:       tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), convert.Int64ToBytes(0)},
			wantShardNum: 1,
		},
		{
			name:         "select shards",
			entity:       tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.AnyEntry, convert.Int64ToBytes(0)},
			wantShardNum: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shards, err := s.Shards(tt.entity)
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
			tester.Equal(tt.wantShardNum, len(shards))
		})
	}

}

func Test_Stream_Series(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(tester)
	defer deferFunc()
	baseTime := setupQueryData(tester, "multiple_shards.json", s)
	tests := []struct {
		name    string
		args    queryOpts
		want    shardsForTest
		wantErr bool
	}{
		{
			name: "all",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_12243341348514563931", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       0,
					location: []string{"series_1671844747554927007", "data_flow_0"},
					elements: []string{"2"},
				},
				{
					id:       1,
					location: []string{"series_2374367181827824198", "data_flow_0"},
					elements: []string{"5", "3"},
				},
				{
					id:       1,
					location: []string{"series_8429137420168685297", "data_flow_0"},
					elements: []string{"4"},
				},
			},
		},

		{
			name: "time range",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime.Add(1500*time.Millisecond), 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_12243341348514563931", "data_flow_0"},
				},
				{
					id:       0,
					location: []string{"series_1671844747554927007", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_2374367181827824198", "data_flow_0"},
					elements: []string{"5"},
				},
				{
					id:       1,
					location: []string{"series_8429137420168685297", "data_flow_0"},
					elements: []string{"4"},
				},
			},
		},
		{
			name: "find series by service_id and instance_id",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_12243341348514563931", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       1,
					location: []string{"series_2374367181827824198", "data_flow_0"},
					elements: []string{"5", "3"},
				},
			},
		},
		{
			name: "find a series",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), convert.Uint64ToBytes(1)},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       1,
					location: []string{"series_2374367181827824198", "data_flow_0"},
					elements: []string{"5", "3"},
				},
			},
		},
		{
			name: "filter",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					builder.Filter(&databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "endpoint_id",
							Group: "default",
						},
						Tags:     []string{"endpoint_id"},
						Type:     databasev2.IndexRule_TYPE_INVERTED,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}, tsdb.Condition{
						"endpoint_id": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_EQ,
								Values: [][]byte{[]byte("/home_id")},
							},
						},
					})
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_12243341348514563931", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       0,
					location: []string{"series_1671844747554927007", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_2374367181827824198", "data_flow_0"},
					elements: []string{"3"},
				},
				{
					id:       1,
					location: []string{"series_8429137420168685297", "data_flow_0"},
				},
			},
		},
		{
			name: "order by duration",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					builder.OrderByIndex(&databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "duration",
							Group: "default",
						},
						Tags:     []string{"duration"},
						Type:     databasev2.IndexRule_TYPE_TREE,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}, modelv2.QueryOrder_SORT_ASC)
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_12243341348514563931", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       0,
					location: []string{"series_1671844747554927007", "data_flow_0"},
					elements: []string{"2"},
				},
				{
					id:       1,
					location: []string{"series_2374367181827824198", "data_flow_0"},
					elements: []string{"3", "5"},
				},
				{
					id:       1,
					location: []string{"series_8429137420168685297", "data_flow_0"},
					elements: []string{"4"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := queryData(tester, s, tt.args)
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
			sort.SliceStable(got, func(i, j int) bool {
				a := got[i]
				b := got[j]
				if a.id > b.id {
					return false
				}
				for i, al := range a.location {
					bl := b.location[i]
					if bytes.Compare([]byte(al), []byte(bl)) > 0 {
						return false
					}
				}
				return true
			})
			tester.Equal(tt.want, got)
		})
	}

}

func Test_Stream_Global_Index(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(tester)
	defer deferFunc()
	_ = setupQueryData(tester, "global_index.json", s)
	tests := []struct {
		name                string
		traceID             string
		wantTraceSegmentNum int
		wantErr             bool
	}{
		{
			name:                "trace id is 1",
			traceID:             "1",
			wantTraceSegmentNum: 2,
		},
		{
			name:                "trace id is 2",
			traceID:             "2",
			wantTraceSegmentNum: 3,
		},
		{
			name:    "unknown trace id",
			traceID: "foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shards, errShards := s.Shards(nil)
			tester.NoError(errShards)
			err := func() error {
				for _, shard := range shards {
					itemIDs, err := shard.Index().Seek(index.Field{
						Key:  []byte("trace_id"),
						Term: []byte(tt.traceID),
					})
					if err != nil {
						return errors.WithStack(err)
					}
					if len(itemIDs) < 1 {
						continue
					}
					if err != nil {
						return errors.WithStack(err)
					}
					tester.Equal(tt.wantTraceSegmentNum, len(itemIDs))
					for _, itemID := range itemIDs {
						segShard, err := s.Shard(itemID.ShardID)
						if err != nil {
							return errors.WithStack(err)
						}
						series, err := segShard.Series().GetByID(itemID.SeriesID)
						if err != nil {
							return errors.WithStack(err)
						}
						err = func() error {
							item, closer, errInner := series.Get(itemID)
							defer func(closer io.Closer) {
								_ = closer.Close()
							}(closer)
							if errInner != nil {
								return errors.WithStack(errInner)
							}
							tagFamily, errInner := s.ParseTagFamily("searchable", item)
							if errInner != nil {
								return errors.WithStack(errInner)
							}
							for _, tag := range tagFamily.GetTags() {
								if tag.GetKey() == "trace_id" {
									tester.Equal(tt.traceID, tag.GetValue().GetStr().GetValue())
								}
							}
							return nil
						}()
						if err != nil {
							return errors.WithStack(err)
						}

					}
				}
				return nil
			}()
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
		})
	}

}

type queryOpts struct {
	entity    tsdb.Entity
	timeRange tsdb.TimeRange
	buildFn   func(builder tsdb.SeekerBuilder)
}

func queryData(tester *assert.Assertions, s *stream, opts queryOpts) (shardsForTest, error) {
	shards, err := s.Shards(opts.entity)
	tester.NoError(err)
	got := shardsForTest{}
	for _, shard := range shards {
		seriesList, err := shard.Series().List(tsdb.NewPath(opts.entity))
		if err != nil {
			return nil, err
		}
		for _, series := range seriesList {
			got, err = func(g shardsForTest) (shardsForTest, error) {
				sp, errInner := series.Span(opts.timeRange)
				defer func(sp tsdb.SeriesSpan) {
					_ = sp.Close()
				}(sp)
				if errInner != nil {
					return nil, errInner
				}
				builder := sp.SeekerBuilder()
				if opts.buildFn != nil {
					opts.buildFn(builder)
				}
				seeker, errInner := builder.Build()
				if errInner != nil {
					return nil, errInner
				}
				iter, errInner := seeker.Seek()
				if errInner != nil {
					return nil, errInner
				}
				for dataFlowID, iterator := range iter {
					var elements []string
					for iterator.Next() {
						tagFamily, errInner := s.ParseTagFamily("searchable", iterator.Val())
						if errInner != nil {
							return nil, errInner
						}
						for _, tag := range tagFamily.GetTags() {
							if tag.GetKey() == "trace_id" {
								elements = append(elements, tag.GetValue().GetStr().GetValue())
							}
						}
					}
					_ = iterator.Close()
					g = append(g, shardStruct{
						id: shard.ID(),
						location: []string{
							fmt.Sprintf("series_%v", series.ID()),
							"data_flow_" + strconv.Itoa(dataFlowID),
						},
						elements: elements,
					})
				}

				return g, nil
			}(got)
			if err != nil {
				return nil, err
			}
		}
	}
	return got, nil
}

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(t *assert.Assertions, dataFile string, stream *stream) (baseTime time.Time) {
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	t.NoError(err)
	t.NoError(json.Unmarshal(content, &templates))
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, err := json.Marshal(template)
		t.NoError(err)
		searchTagFamily := &streamv2.ElementValue_TagFamily{}
		t.NoError(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily))
		e := &streamv2.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(500 * time.Millisecond * time.Duration(i))),
			TagFamilies: []*streamv2.ElementValue_TagFamily{
				{
					Tags: []*modelv2.TagValue{
						{
							Value: &modelv2.TagValue_BinaryData{
								BinaryData: bb,
							},
						},
					},
				},
			},
		}
		e.TagFamilies = append(e.TagFamilies, searchTagFamily)
		entity, err := stream.buildEntity(e)
		t.NoError(err)
		shardID, err := partition.ShardID(entity.Marshal(), stream.schema.GetShardNum())
		t.NoError(err)
		itemID, err := stream.write(common.ShardID(shardID), e)
		t.NoError(err)
		sa, err := stream.Shards(entity)
		t.NoError(err)
		for _, shard := range sa {
			se, err := shard.Series().Get(entity)
			t.NoError(err)
			for {
				item, closer, _ := se.Get(*itemID)
				rawTagFamily, _ := item.Val("searchable")
				if len(rawTagFamily) > 0 {
					_ = closer.Close()
					break
				}
				_ = closer.Close()
			}

		}
	}
	return baseTime
}