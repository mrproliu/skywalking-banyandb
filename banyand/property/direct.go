package property

import (
	"context"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

// DirectUpdate implements DirectService.DirectUpdate.
func (s *service) DirectUpdate(ctx context.Context, group string, shardID uint32, id []byte, prop *propertyv1.Property) error {
	return s.db.update(ctx, common.ShardID(shardID), id, prop)
}

// DirectDelete implements DirectService.DirectDelete.
func (s *service) DirectDelete(ctx context.Context, ids [][]byte) error {
	return s.db.delete(ctx, ids)
}

// DirectQuery implements DirectService.DirectQuery.
func (s *service) DirectQuery(ctx context.Context, req *propertyv1.QueryRequest) ([]*PropertyWithDeleteTime, error) {
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
func (s *service) DirectGet(ctx context.Context, group, name, id string) (*propertyv1.Property, error) {
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
func (s *service) DirectRepair(ctx context.Context, shardID uint64, id []byte, prop *propertyv1.Property, deleteTime int64) error {
	return s.db.repair(ctx, id, shardID, prop, deleteTime)
}
