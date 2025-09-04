package stable

import (
	"context"
	"encoding/base64"
	"fmt"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

type entityScopeType int

const (
	entityScopeTypeService entityScopeType = iota
	entityScopeTypeServiceInstance
	entityScopeTypeEndpoint
	entityScopeTypeServiceRelation
	entityScopeTypeServiceInstanceRelation
	entityScopeTypeEndpointRelation
)

func (e entityScopeType) isRelation() bool {
	return e == entityScopeTypeServiceRelation || e == entityScopeTypeServiceInstanceRelation ||
		e == entityScopeTypeEndpointRelation
}

type entityRelationFromType int

const (
	entityRelationFromTypeSource entityRelationFromType = iota
	entityRelationFromTypeDest
	entityRelationFromTypeAll // for the entity ID combine the source and dest
	entityRelationFromTypeUnknown
)

type entityValueType int

const (
	entityValueTypeName entityValueType = iota
	entityValueTypeID
)

var (
	entityNameSets = map[string]entityScopeType{
		"service_name":                entityScopeTypeService,
		"source_service_name":         entityScopeTypeServiceRelation,
		"dest_service_name":           entityScopeTypeServiceRelation,
		"local_endpoint_service_name": entityScopeTypeService,
		"source_name":                 entityScopeTypeServiceRelation,
		"dest_name":                   entityScopeTypeServiceRelation,
	}
	entityRelationSets = map[string]entityRelationFromType{
		"source_service_name": entityRelationFromTypeSource,
		"dest_service_name":   entityRelationFromTypeDest,
		"source_service_id":   entityRelationFromTypeSource,
		"dest_service_id":     entityRelationFromTypeDest,
		"source_name":         entityRelationFromTypeSource,
		"dest_name":           entityRelationFromTypeDest,
	}
	entityIDSets = map[string]entityScopeType{
		"service_id":          entityScopeTypeService,
		"source_service_id":   entityScopeTypeServiceRelation,
		"dest_service_id":     entityScopeTypeServiceRelation,
		"service_instance_id": entityScopeTypeServiceInstance,
	}
)

type entityIDFieldType int

const (
	entityIDFieldTypeTag entityIDFieldType = iota
	entityIDFieldTypeField
)

type entityIDField struct {
	index        int
	scope        entityScopeType
	relationType entityRelationFromType // if the scope is relation, indicate its source or dest
	value        entityValueType
	tp           entityIDFieldType
	subIndex     int // when the type is tag, subIndex is the index in the tag family
}

type EntityIDFieldValue struct {
	field *entityIDField
	Value string
}

type schema[T proto.Message] interface {
	GetServiceName(T) ([]string, error)
	GetRelatedFieldValues(T) []*EntityIDFieldValue
	getScope() entityScopeType
	ApplyFieldChange(T, []string, *EntityIDFieldValue)
	GetName() string
	GetType() schemaType
}

type schemaType int

const (
	schemaTypeMeasure schemaType = iota
	schemaTypeStream
)

type baseSchema struct {
	serviceField     *entityIDField
	destServiceField *entityIDField
	name             string
	relatedFields    []*entityIDField
	scope            entityScopeType
}

func (b *baseSchema) initEntities(name string, tp schemaType, tags []*databasev1.TagFamilySpec, fields []*databasev1.FieldSpec) {
	b.name = name
	b.generateEntityScope(name)
	for inx, tag := range tags {
		for subInx, t := range tag.Tags {
			if field := b.buildEntityIDField(tp, entityIDFieldTypeTag, inx, subInx, t.Name); field != nil {
				b.appendField(field, name)
			}
		}
	}

	for i, field := range fields {
		if f := b.buildEntityIDField(tp, entityIDFieldTypeField, i, 0, field.Name); f != nil {
			b.appendField(f, name)
		}
	}
}

func (b *baseSchema) generateEntityScope(name string) {
	var scope entityScopeType
	switch {
	case strings.Contains(name, "service_relation_"),
		strings.HasPrefix(name, "tcp_service_client_"),
		strings.HasPrefix(name, "tcp_service_server_"),
		strings.HasPrefix(name, "service_client_"),
		strings.HasPrefix(name, "service_server_"):
		scope = entityScopeTypeServiceRelation

	case strings.Contains(name, "endpoint_relation_"):
		scope = entityScopeTypeEndpointRelation

	case strings.Contains(name, "instance_relation_"):
		scope = entityScopeTypeServiceInstanceRelation

	case strings.HasPrefix(name, "endpoint_"):
		scope = entityScopeTypeEndpoint

	case strings.HasPrefix(name, "envoy_"),
		strings.HasPrefix(name, "instance_"),
		strings.HasPrefix(name, "k8s_tcp_service_instance"),
		strings.HasPrefix(name, "satellite_service_"),
		strings.HasPrefix(name, "service_instance_"),
		strings.HasPrefix(name, "tcp_service_instance_"):
		scope = entityScopeTypeServiceInstance

	default:
		scope = entityScopeTypeService
	}
	b.scope = scope
}

func (b *baseSchema) getScope() entityScopeType {
	return b.scope
}

func (b *baseSchema) appendField(f *entityIDField, entityName string) {
	if f.scope.isRelation() && !b.scope.isRelation() {
		panic(fmt.Errorf("found wrong schema scope analysis: %s", entityName))
	}
	b.relatedFields = append(b.relatedFields, f)
	if f.scope == entityScopeTypeService {
		b.serviceField = b.settingServiceField(b.serviceField, f)
	} else if f.scope.isRelation() {
		if f.relationType == entityRelationFromTypeSource {
			b.serviceField = b.settingServiceField(b.serviceField, f)
		} else if f.relationType == entityRelationFromTypeDest {
			b.destServiceField = b.settingServiceField(b.destServiceField, f)
		}
	}
}

func (b *baseSchema) settingServiceField(original *entityIDField, updated *entityIDField) *entityIDField {
	if original == nil {
		return updated
	}
	// if the existing service field is entity_id, and the new one is entity_name, replace it
	// service name have more chance to be the entity identifier
	if original.value == entityValueTypeID && updated.value == entityValueTypeName {
		return updated
	}
	return original
}

func (b *baseSchema) generateFieldValue(generatedServiceName []string, fv *EntityIDFieldValue) string {
	switch fv.field.scope {
	case entityScopeTypeService:
		if fv.field.value == entityValueTypeID {
			return base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])) + ".1"
		} else if fv.field.value == entityValueTypeName {
			return generatedServiceName[0]
		}
	case entityScopeTypeServiceInstance:
		if fv.field.value == entityValueTypeID {
			return fmt.Sprintf("%s.1_%s", base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])),
				fv.Value)
		} else {
			return fv.Value
		}
	case entityScopeTypeEndpoint:
		if fv.field.value == entityValueTypeID {
			return fmt.Sprintf("%s.1_%s", base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])),
				fv.Value)
		} else {
			return fv.Value
		}
	case entityScopeTypeServiceRelation:
		if fv.field.value == entityValueTypeID {
			switch fv.field.relationType {
			case entityRelationFromTypeSource:
				return base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])) + ".1"
			case entityRelationFromTypeDest:
				return base64.StdEncoding.EncodeToString([]byte(generatedServiceName[1])) + ".1"
			case entityRelationFromTypeAll:
				// service relation ID should be combined by source and dest
				// format: {base64(source_service_name)}.1-{base64(dest_service_name)}.1
				return fmt.Sprintf("%s.1-%s.1", base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])),
					base64.StdEncoding.EncodeToString([]byte(generatedServiceName[1])))
			default:
				panic(fmt.Sprintf("unexpected relation type: %d in entity: %s", fv.field.relationType, b.name))
			}
		} else {
			switch fv.field.relationType {
			case entityRelationFromTypeSource:
				return generatedServiceName[0]
			case entityRelationFromTypeDest:
				return generatedServiceName[1]
			default:
				panic(fmt.Sprintf("unexpected relation type: %d in entity: %s", fv.field.relationType, b.name))
			}
		}
	default:
		panic(fmt.Sprintf("unexpected scope: %d", fv.field.scope))
	}
	return ""
}

func (b *baseSchema) getRelatedFieldValues(tags []*modelv1.TagFamilyForWrite, fields []*modelv1.FieldValue) []*EntityIDFieldValue {
	result := make([]*EntityIDFieldValue, 0, len(b.relatedFields))
	for _, f := range b.relatedFields {
		var val string
		if f.scope == entityScopeTypeService || f.scope.isRelation() {
			result = append(result, &EntityIDFieldValue{field: f})
			continue
		}

		if f.tp == entityIDFieldTypeTag {
			tag := tags[f.index].Tags[f.subIndex]
			val = tag.Value.(*modelv1.TagValue_Str).Str.Value
		} else if f.tp == entityIDFieldTypeField {
			field := fields[f.index]
			val = field.Value.(*modelv1.FieldValue_Str).Str.Value
		}
		if f.value == entityValueTypeID {
			switch f.scope {
			case entityScopeTypeServiceInstance:
				// for service instance, the format is {serviceId}_{base64(instance_name)}
				_, after, found := strings.Cut(val, "_")
				if !found {
					panic(fmt.Sprintf("invalid entity id field: %s", val))
				}
				val = after
			case entityScopeTypeEndpoint:
				// for endpoint, the format is {serviceId}_{base64(endpoint_name)}
				_, after, found := strings.Cut(val, "_")
				if !found {
					panic(fmt.Sprintf("invalid entity id field: %s", val))
				}
				val = after
			default:
				panic(fmt.Sprintf("unknown field scope: %d", f.scope))
			}
		}
		if val != "" {
			result = append(result, &EntityIDFieldValue{
				field: f,
				Value: val,
			})
		}
	}
	return result
}

func (b *baseSchema) buildEntityIDField(entityType schemaType, tp entityIDFieldType, inx, subIndex int, fName string) *entityIDField {
	if level, ok := entityNameSets[fName]; ok {
		return &entityIDField{
			tp:           tp,
			index:        inx,
			subIndex:     subIndex,
			scope:        level,
			value:        entityValueTypeName,
			relationType: b.handleRelationFrom(fName, level),
		}
	} else if l, ok := entityIDSets[fName]; ok {
		return &entityIDField{
			tp:           tp,
			index:        inx,
			subIndex:     subIndex,
			scope:        l,
			value:        entityValueTypeID,
			relationType: b.handleRelationFrom(fName, l),
		}
	}
	// special case for service_traffic_measure or related metrics
	if fName == "name" && b.scope == entityScopeTypeService && entityType == schemaTypeMeasure {
		return &entityIDField{
			tp:       tp,
			index:    inx,
			subIndex: subIndex,
			scope:    entityScopeTypeService,
			value:    entityValueTypeName,
		}
	}
	// if using entity_id as the entity identifier,
	// then it's dependent on the name of measure/stream
	if fName == "entity_id" {
		field := &entityIDField{
			tp:       tp,
			index:    inx,
			subIndex: subIndex,
			scope:    b.scope,
			value:    entityValueTypeID,
		}
		if b.scope.isRelation() {
			field.relationType = entityRelationFromTypeAll
		}
		return field
	}

	return nil
}

func (b *baseSchema) handleRelationFrom(fName string, scope entityScopeType) entityRelationFromType {
	if scope == entityScopeTypeServiceRelation || scope == entityScopeTypeEndpointRelation || scope == entityScopeTypeServiceInstanceRelation {
		if fromType, found := entityRelationSets[fName]; found {
			return fromType
		}
	}
	return entityRelationFromTypeUnknown
}

func (b *baseSchema) findServiceName(tags []*modelv1.TagFamilyForWrite, fields []*modelv1.FieldValue) ([]string, error) {
	// handling relation
	if b.scope.isRelation() {
		if b.serviceField == nil || b.destServiceField == nil {
			return nil, fmt.Errorf("relation entity should have both source and dest service field: %s", b.name)
		}
		return []string{
			b.fetchValueAsName(b.serviceField, tags, fields),
			b.fetchValueAsName(b.destServiceField, tags, fields),
		}, nil
	}
	if b.serviceField == nil {
		return nil, fmt.Errorf("entity should have both source and dest service field: %s", b.name)
	}
	return []string{b.fetchValueAsName(b.serviceField, tags, fields)}, nil
}

func (b *baseSchema) fetchValueAsName(field *entityIDField, tags []*modelv1.TagFamilyForWrite, fields []*modelv1.FieldValue) string {
	var val string
	if field.tp == entityIDFieldTypeTag {
		tag := tags[field.index].Tags[field.subIndex]
		val = tag.Value.(*modelv1.TagValue_Str).Str.Value
	} else if field.tp == entityIDFieldTypeField {
		f := fields[field.index]
		val = f.Value.(*modelv1.FieldValue_Str).Str.Value
	}

	if field.value == entityValueTypeID {
		serviceNameDecode, _, found := strings.Cut(val, ".")
		if !found {
			panic(fmt.Sprintf("invalid service id format: %s", val))
		}
		decodeString, err := base64.StdEncoding.DecodeString(serviceNameDecode)
		if err != nil {
			panic(fmt.Sprintf("service name base64 decode failed: %s", serviceNameDecode))
		}
		return string(decodeString)
	} else if field.value == entityValueTypeName {
		return val
	}
	return ""
}

type MeasureSchema struct {
	schema *databasev1.Measure
	baseSchema
}

func newMeasureSchema(schema *databasev1.Measure) *MeasureSchema {
	result := &MeasureSchema{
		schema: schema,
	}
	result.initEntities(schema.Metadata.Name, schemaTypeMeasure, schema.TagFamilies, schema.Fields)
	return result
}

func (b *MeasureSchema) GetName() string {
	return b.schema.Metadata.Name
}

func (b *MeasureSchema) GetServiceName(request *measurev1.WriteRequest) ([]string, error) {
	return b.baseSchema.findServiceName(request.DataPoint.TagFamilies, request.DataPoint.Fields)
}

func (b *MeasureSchema) GetType() schemaType {
	return schemaTypeMeasure
}

func (b *MeasureSchema) ApplyFieldChange(request *measurev1.WriteRequest, generatedServiceName []string, fv *EntityIDFieldValue) {
	if fv.field.tp == entityIDFieldTypeTag {
		tag := request.DataPoint.TagFamilies[fv.field.index].Tags[fv.field.subIndex]
		tag.Value = &modelv1.TagValue_Str{Str: &modelv1.Str{Value: b.baseSchema.generateFieldValue(generatedServiceName, fv)}}
	} else if fv.field.tp == entityIDFieldTypeField {
		field := request.DataPoint.Fields[fv.field.index]
		field.Value = &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: b.baseSchema.generateFieldValue(generatedServiceName, fv)}}
	}
}

func (b *MeasureSchema) GetRelatedFieldValues(request *measurev1.WriteRequest) []*EntityIDFieldValue {
	return b.baseSchema.getRelatedFieldValues(request.DataPoint.TagFamilies, request.DataPoint.Fields)
}

type StreamSchema struct {
	schema *databasev1.Stream
	baseSchema
}

func newStreamSchema(schema *databasev1.Stream) *StreamSchema {
	result := &StreamSchema{
		schema: schema,
	}
	result.initEntities(schema.Metadata.Name, schemaTypeStream, schema.TagFamilies, nil)
	return result
}

func (s *StreamSchema) GetName() string {
	return s.schema.Metadata.Name
}

func (s *StreamSchema) GetServiceName(request *streamv1.WriteRequest) ([]string, error) {
	return s.baseSchema.findServiceName(request.Element.TagFamilies, nil)
}

func (s *StreamSchema) GetType() schemaType {
	return schemaTypeStream
}

func (s *StreamSchema) ApplyFieldChange(request *streamv1.WriteRequest, generatedServiceName []string, fv *EntityIDFieldValue) {
	if fv.field.tp == entityIDFieldTypeTag {
		tag := request.Element.TagFamilies[fv.field.index].Tags[fv.field.subIndex]
		tag.Value = &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.generateFieldValue(generatedServiceName, fv)}}
	} else if fv.field.tp == entityIDFieldTypeField {
		panic("stream schema should not have field type entity ID")
	}
}

func (s *StreamSchema) GetRelatedFieldValues(request *streamv1.WriteRequest) []*EntityIDFieldValue {
	return s.baseSchema.getRelatedFieldValues(request.Element.TagFamilies, nil)
}

func InitializeAllSchema(conn *grpc.ClientConn, dataDir string) (map[string]*StreamSchema, map[string]*MeasureSchema, error) {
	// creating all the groups, streams, and measures
	var groupClient databasev1.GroupRegistryServiceClient
	var streamClient databasev1.StreamRegistryServiceClient
	var measureClient databasev1.MeasureRegistryServiceClient
	if conn != nil {
		groupClient = databasev1.NewGroupRegistryServiceClient(conn)
		streamClient = databasev1.NewStreamRegistryServiceClient(conn)
		measureClient = databasev1.NewMeasureRegistryServiceClient(conn)
	}
	groupDir := filepath.Join(dataDir, "groups")
	streamDir := filepath.Join(dataDir, "streams")
	measureDir := filepath.Join(dataDir, "measures")
	groups, err := readingDirProtoList(groupDir, func() *commonv1.Group {
		return &commonv1.Group{}
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read groups: %w", err)
	}
	for _, g := range groups {
		if groupClient != nil {
			_, err = groupClient.Create(context.Background(), &databasev1.GroupRegistryServiceCreateRequest{
				Group: g,
			})
			if err = handlingCreateSchemaResult("Group", g.Metadata.Name, err); err != nil {
				return nil, nil, err
			}
		}
	}

	// streams
	streamSchemas := make(map[string]*StreamSchema)
	streams, err := readingDirProtoList(streamDir, func() *databasev1.Stream {
		return &databasev1.Stream{}
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read streams: %w", err)
	}
	for _, s := range streams {
		if streamClient != nil {
			_, err = streamClient.Create(context.Background(), &databasev1.StreamRegistryServiceCreateRequest{
				Stream: s,
			})
			if err = handlingCreateSchemaResult("Stream", s.Metadata.Name, err); err != nil {
				return nil, nil, err
			}
		}
		streamSchemas[s.Metadata.Name] = newStreamSchema(s)
	}

	// measures
	measureSchemas := make(map[string]*MeasureSchema)
	measures, err := readingDirProtoList(measureDir, func() *databasev1.Measure {
		return &databasev1.Measure{}
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read measures: %w", err)
	}
	for _, m := range measures {
		if measureClient != nil {
			_, err = measureClient.Create(context.Background(), &databasev1.MeasureRegistryServiceCreateRequest{
				Measure: m,
			})
			if err = handlingCreateSchemaResult("Measure", m.Metadata.Name, err); err != nil {
				return nil, nil, err
			}
		}
		measureSchemas[m.Metadata.Name] = newMeasureSchema(m)
	}
	return streamSchemas, measureSchemas, nil
}

func handlingCreateSchemaResult(tp, name string, err error) error {
	if err != nil {
		if strings.Contains(err.Error(), "resource already exists") {
			fmt.Println(tp, name, "already exists, skipping creation")
		} else {
			return err
		}
	} else {
		fmt.Println("Created", tp, name)
	}
	return nil
}

func readingDirProtoList[T proto.Message](dir string, newElem func() T) ([]T, error) {
	var out []T
	list, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, node := range list {
		if node.IsDir() {
			continue
		}
		filePath := filepath.Join(dir, node.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
		}
		elem := newElem()
		if err := protojson.Unmarshal(data, elem); err != nil {
			return nil, fmt.Errorf("json->proto doc: %w", err)
		}
		out = append(out, elem)
	}
	return out, nil
}
