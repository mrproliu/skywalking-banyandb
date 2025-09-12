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
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

type EntityScopeType int

const (
	EntityScopeTypeService EntityScopeType = iota
	EntityScopeTypeServiceInstance
	EntityScopeTypeEndpoint
	entityScopeTypeServiceRelation
	entityScopeTypeServiceInstanceRelation
	entityScopeTypeEndpointRelation
)

func (e EntityScopeType) isRelation() bool {
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
	entityNameSets = map[string]EntityScopeType{
		"service_name":                EntityScopeTypeService,
		"source_service_name":         entityScopeTypeServiceRelation,
		"dest_service_name":           entityScopeTypeServiceRelation,
		"local_endpoint_service_name": EntityScopeTypeService,
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
	entityIDSets = map[string]EntityScopeType{
		"service_id":          EntityScopeTypeService,
		"source_service_id":   entityScopeTypeServiceRelation,
		"dest_service_id":     entityScopeTypeServiceRelation,
		"service_instance_id": EntityScopeTypeServiceInstance,
	}
)

type entityIDFieldType int

const (
	entityIDFieldTypeTag entityIDFieldType = iota
	entityIDFieldTypeField
)

type attrFields struct {
	attrs [6]*entityField
}

func (a *attrFields) applyChanges(tagFamilies []*modelv1.TagFamilyForWrite, service string) {
	name := parseServiceName(service)
	if name.highLevelService || name.unknownService {
		return
	}
	if name.hostnameService {
		tagFamilies[a.attrs[0].index].Tags[a.attrs[0].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: "hostname"}}
		tagFamilies[a.attrs[1].index].Tags[a.attrs[1].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.hostname}}
		tagFamilies[a.attrs[2].index].Tags[a.attrs[2].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.service}}
		tagFamilies[a.attrs[3].index].Tags[a.attrs[3].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.cluster}}
		tagFamilies[a.attrs[4].index].Tags[a.attrs[4].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.env}}
	} else {
		tagFamilies[a.attrs[0].index].Tags[a.attrs[0].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service"}}
		tagFamilies[a.attrs[1].index].Tags[a.attrs[1].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.subset}}
		tagFamilies[a.attrs[2].index].Tags[a.attrs[2].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.service}}
		tagFamilies[a.attrs[3].index].Tags[a.attrs[3].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.namespace}}
		tagFamilies[a.attrs[4].index].Tags[a.attrs[4].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.cluster}}
		tagFamilies[a.attrs[5].index].Tags[a.attrs[5].subIndex].Value =
			&modelv1.TagValue_Str{Str: &modelv1.Str{Value: name.env}}
	}
}

type entityField struct {
	index        int
	scope        EntityScopeType
	relationType entityRelationFromType // if the scope is relation, indicate its source or dest
	value        entityValueType
	tp           entityIDFieldType
	subIndex     int // when the type is tag, subIndex is the index in the tag family
}

type EntityFieldValue struct {
	field *entityField
	Value string
}

type schema[T proto.Message] interface {
	GetServiceName(T) ([]string, error)
	GetRelatedFieldValues(T) (subEntity *EntityFieldValue, all []*EntityFieldValue)
	getScope() EntityScopeType
	ApplyFieldChange(T, []string, string, *EntityFieldValue)
	GetName() string
	GetType() schemaType
	getBaseSchema() *baseSchema
	getAttrFields() *attrFields
}

type schemaType int

const (
	schemaTypeMeasure schemaType = iota
	schemaTypeStream
)

type baseSchema struct {
	serviceFieldInx     int
	destServiceFieldInx int
	name                string
	relatedFields       []*entityField
	scope               EntityScopeType
	attrFields          *attrFields
}

func (b *baseSchema) initEntities(name string, tp schemaType, tags []*databasev1.TagFamilySpec, fields []*databasev1.FieldSpec) {
	b.name = name
	b.serviceFieldInx = -1
	b.destServiceFieldInx = -1
	b.generateEntityScope(name)
	for inx, tag := range tags {
		for subInx, t := range tag.Tags {
			if field := b.buildEntityIDField(tp, entityIDFieldTypeTag, inx, subInx, t.Name); field != nil {
				b.appendField(field, name)
			} else if strings.HasPrefix(t.Name, "attr") {
				if b.attrFields == nil {
					b.attrFields = &attrFields{}
				}
				inxStr, ok := strings.CutPrefix(t.Name, "attr")
				if !ok {
					panic("invalid attr field name: " + t.Name)
				}
				attrInx, err := strconv.ParseInt(inxStr, 0, 64)
				if err != nil || attrInx < 0 || attrInx >= int64(len(b.attrFields.attrs)) {
					panic("invalid attr field name: " + t.Name)
				}
				b.attrFields.attrs[attrInx] = &entityField{
					tp:       entityIDFieldTypeTag,
					index:    inx,
					subIndex: subInx,
				}
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
	var scope EntityScopeType
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
		scope = EntityScopeTypeEndpoint

	case strings.HasPrefix(name, "envoy_"),
		strings.HasPrefix(name, "instance_"),
		strings.HasPrefix(name, "k8s_tcp_service_instance"),
		strings.HasPrefix(name, "satellite_service_"),
		strings.HasPrefix(name, "service_instance_"),
		strings.HasPrefix(name, "tcp_service_instance_"):
		scope = EntityScopeTypeServiceInstance

	default:
		scope = EntityScopeTypeService
	}
	b.scope = scope
}

func (b *baseSchema) getScope() EntityScopeType {
	return b.scope
}

func (b *baseSchema) appendField(f *entityField, entityName string) {
	if f.scope.isRelation() && !b.scope.isRelation() {
		panic(fmt.Errorf("found wrong schema scope analysis: %s", entityName))
	}
	b.relatedFields = append(b.relatedFields, f)
	if f.scope == EntityScopeTypeService {
		b.serviceFieldInx = b.settingServiceField(b.serviceFieldInx, len(b.relatedFields)-1)
	} else if f.scope.isRelation() {
		if f.relationType == entityRelationFromTypeSource {
			b.serviceFieldInx = b.settingServiceField(b.serviceFieldInx, len(b.relatedFields)-1)
		} else if f.relationType == entityRelationFromTypeDest {
			b.destServiceFieldInx = b.settingServiceField(b.destServiceFieldInx, len(b.relatedFields)-1)
		}
	}
}

func (b *baseSchema) settingServiceField(original int, updated int) int {
	if original < 0 {
		return updated
	}
	// if the existing service field is entity_id, and the new one is entity_name, replace it
	// service name have more chance to be the entity identifier
	if b.relatedFields[original].value == entityValueTypeID && b.relatedFields[updated].value == entityValueTypeName {
		return updated
	}
	return original
}

func (b *baseSchema) generateFieldValue(generatedServiceName []string, subEntityName string, fv *EntityFieldValue) string {
	switch fv.field.scope {
	case EntityScopeTypeService:
		if fv.field.value == entityValueTypeID {
			return base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])) + ".1"
		} else if fv.field.value == entityValueTypeName {
			return generatedServiceName[0]
		}
	case EntityScopeTypeServiceInstance:
		if fv.field.value == entityValueTypeID {
			return fmt.Sprintf("%s.1_%s", base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])),
				base64.StdEncoding.EncodeToString([]byte(subEntityName)))
		} else {
			return subEntityName
		}
	case EntityScopeTypeEndpoint:
		if fv.field.value == entityValueTypeID {
			return fmt.Sprintf("%s.1_%s", base64.StdEncoding.EncodeToString([]byte(generatedServiceName[0])),
				base64.StdEncoding.EncodeToString([]byte(subEntityName)))
		} else {
			return subEntityName
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

func (b *baseSchema) getRelatedFieldValues(tags []*modelv1.TagFamilyForWrite, fields []*modelv1.FieldValue) (*EntityFieldValue, []*EntityFieldValue) {
	result := make([]*EntityFieldValue, 0, len(b.relatedFields))
	var subEntity *EntityFieldValue
	for _, f := range b.relatedFields {
		var val string
		if f.scope.isRelation() || f.scope == EntityScopeTypeService {
			// service name and relation(only service relation support)no need to analysis
			result = append(result, &EntityFieldValue{field: f})
			continue
		}

		val = b.findValue(f, tags, fields)
		if f.value == entityValueTypeID {
			switch f.scope {
			case EntityScopeTypeServiceInstance:
				// for service instance, the format is {serviceId}_{base64(instance_name)}
				_, after, found := strings.Cut(val, "_")
				if !found {
					panic(fmt.Sprintf("invalid entity id field: %s", val))
				}
				name, err := base64.StdEncoding.DecodeString(after)
				if err != nil {
					panic(fmt.Sprintf("service instance name base64 decode failed: %s", after))
				}
				val = string(name)
			case EntityScopeTypeEndpoint:
				// for endpoint, the format is {serviceId}_{base64(endpoint_name)}
				_, after, found := strings.Cut(val, "_")
				if !found {
					panic(fmt.Sprintf("invalid entity id field: %s", val))
				}
				name, err := base64.StdEncoding.DecodeString(after)
				if err != nil {
					panic(fmt.Sprintf("service instance name base64 decode failed: %s", after))
				}
				val = string(name)
			default:
				panic(fmt.Sprintf("unknown field scope: %d", f.scope))
			}
		}
		fv := &EntityFieldValue{
			field: f,
			Value: val,
		}
		result = append(result, fv)
		if f.scope == EntityScopeTypeServiceInstance || f.scope == EntityScopeTypeEndpoint {
			subEntity = fv
		}
	}
	return subEntity, result
}

func (b *baseSchema) buildEntityIDField(entityType schemaType, tp entityIDFieldType, inx, subIndex int, fName string) *entityField {
	if level, ok := entityNameSets[fName]; ok {
		return &entityField{
			tp:           tp,
			index:        inx,
			subIndex:     subIndex,
			scope:        level,
			value:        entityValueTypeName,
			relationType: b.handleRelationFrom(fName, level),
		}
	} else if l, ok := entityIDSets[fName]; ok {
		return &entityField{
			tp:           tp,
			index:        inx,
			subIndex:     subIndex,
			scope:        l,
			value:        entityValueTypeID,
			relationType: b.handleRelationFrom(fName, l),
		}
	}
	// special case for service_traffic_measure or related metrics
	if fName == "name" && entityType == schemaTypeMeasure {
		return &entityField{
			tp:       tp,
			index:    inx,
			subIndex: subIndex,
			scope:    b.scope,
			value:    entityValueTypeName,
		}
	}
	// if using entity_id as the entity identifier,
	// then it's dependent on the name of measure/stream
	if fName == "entity_id" {
		field := &entityField{
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

func (b *baseSchema) handleRelationFrom(fName string, scope EntityScopeType) entityRelationFromType {
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
		if b.serviceFieldInx < 0 || b.destServiceFieldInx < 0 {
			return nil, fmt.Errorf("relation entity should have both source and dest service field: %s", b.name)
		}
		return []string{
			b.fetchValueAsName(b.relatedFields[b.serviceFieldInx], tags, fields),
			b.fetchValueAsName(b.relatedFields[b.destServiceFieldInx], tags, fields),
		}, nil
	}
	if b.serviceFieldInx < 0 {
		return nil, fmt.Errorf("entity should have both source and dest service field: %s", b.name)
	}
	return []string{b.fetchValueAsName(b.relatedFields[b.serviceFieldInx], tags, fields)}, nil
}

func (b *baseSchema) fetchValueAsName(field *entityField, tags []*modelv1.TagFamilyForWrite, fields []*modelv1.FieldValue) string {
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

func (b *baseSchema) findValue(f *entityField, tags []*modelv1.TagFamilyForWrite, fields []*modelv1.FieldValue) string {
	if f.tp == entityIDFieldTypeTag {
		tag := tags[f.index].Tags[f.subIndex]
		return tag.Value.(*modelv1.TagValue_Str).Str.Value
	} else if f.tp == entityIDFieldTypeField {
		field := fields[f.index]
		return field.Value.(*modelv1.FieldValue_Str).Str.Value
	}
	panic(fmt.Errorf("invalid entity id format: %d", f.tp))
}

func (b *baseSchema) serviceNameByIndexOfField(idVal string, fieldInx int, tags []*modelv1.TagFamilyForWrite, fields []*modelv1.FieldValue) string {
	if b.relatedFields[fieldInx].value == entityValueTypeName {
		return b.findValue(b.relatedFields[fieldInx], tags, fields)
	}
	// for service, the format is {base64(service_name)}.1
	before, _, found := strings.Cut(idVal, ".")
	if !found {
		panic(fmt.Errorf("invalid service id format: %s", idVal))
	}
	decodeString, err := base64.StdEncoding.DecodeString(before)
	if err != nil {
		panic(fmt.Sprintf("service name base64 decode failed: %s", before))
	}
	return string(decodeString)
}

func (b *baseSchema) generateServiceName(baseNames []*serviceName, scales *scalerCounts) [][]string {
	result := make([][]string, 0, scales.cluster*scales.service)
	for clusterInx := range scales.cluster {
		for serviceInx := range scales.service {
			serviceNames := make([]string, 0, len(baseNames))
			for _, service := range baseNames {
				clusterName := fmt.Sprintf("%s-%d", service.cluster, clusterInx)
				if service.cluster == "*" {
					clusterName = service.cluster
				}
				serviceNames = append(serviceNames, service.toService(
					service.subset,
					clusterName,
					fmt.Sprintf("%s-%d", service.service, serviceInx),
					service.env,
				))
			}
			result = append(result, serviceNames)
		}
	}
	return result
}

func (b *baseSchema) generateInstanceName(_ string, instanceBaseName string, sequence int) string {
	return fmt.Sprintf("%s-%d", instanceBaseName, sequence)
}

func (b *baseSchema) generateEndpointName(_ string, endpointBaseName string, sequence int) string {
	return fmt.Sprintf("%s-%d", endpointBaseName, sequence)
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

func (b *MeasureSchema) ApplyFieldChange(request *measurev1.WriteRequest, generatedServiceName []string, subEntity string, fv *EntityFieldValue) {
	if fv.field.tp == entityIDFieldTypeTag {
		tag := request.DataPoint.TagFamilies[fv.field.index].Tags[fv.field.subIndex]
		tag.Value = &modelv1.TagValue_Str{Str: &modelv1.Str{Value: b.baseSchema.generateFieldValue(generatedServiceName, subEntity, fv)}}
	} else if fv.field.tp == entityIDFieldTypeField {
		field := request.DataPoint.Fields[fv.field.index]
		field.Value = &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: b.baseSchema.generateFieldValue(generatedServiceName, subEntity, fv)}}
	}

	if (b.scope == EntityScopeTypeService || b.scope == EntityScopeTypeEndpoint) && b.getAttrFields() != nil {
		b.getAttrFields().applyChanges(request.DataPoint.TagFamilies, generatedServiceName[0])
	}
}

func (b *MeasureSchema) GetRelatedFieldValues(request *measurev1.WriteRequest) (*EntityFieldValue, []*EntityFieldValue) {
	return b.baseSchema.getRelatedFieldValues(request.DataPoint.TagFamilies, request.DataPoint.Fields)
}

func (b *MeasureSchema) GetScope() EntityScopeType {
	return b.scope
}

func (b *MeasureSchema) getBaseSchema() *baseSchema {
	return &b.baseSchema
}

func (b *MeasureSchema) getAttrFields() *attrFields {
	return b.attrFields
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

func (s *StreamSchema) ApplyFieldChange(request *streamv1.WriteRequest, generatedServiceName []string, subEntity string, fv *EntityFieldValue) {
	if fv.field.tp == entityIDFieldTypeTag {
		tag := request.Element.TagFamilies[fv.field.index].Tags[fv.field.subIndex]
		tag.Value = &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.generateFieldValue(generatedServiceName, subEntity, fv)}}
	} else if fv.field.tp == entityIDFieldTypeField {
		panic("stream schema should not have field type entity ID")
	}
}

func (s *StreamSchema) GetRelatedFieldValues(request *streamv1.WriteRequest) (*EntityFieldValue, []*EntityFieldValue) {
	return s.baseSchema.getRelatedFieldValues(request.Element.TagFamilies, nil)
}

func (s *StreamSchema) getBaseSchema() *baseSchema {
	return &s.baseSchema
}

func (s *StreamSchema) getAttrFields() *attrFields {
	return nil
}

func InitializeAllMetadata(conn *grpc.ClientConn, dataDir string) (map[string]*StreamSchema, map[string]*MeasureSchema, error) {
	streamSchema, measureSchema, err := initializeAllSchema(conn, filepath.Join(dataDir, "schema"))
	if err != nil {
		return nil, nil, err
	}
	err = initializeIndexWithTopN(conn, dataDir)
	if err != nil {
		return nil, nil, err
	}
	return streamSchema, measureSchema, nil
}

func initializeAllSchema(conn *grpc.ClientConn, schemaDir string) (map[string]*StreamSchema, map[string]*MeasureSchema, error) {
	// creating all the groups, streams, and measures
	var groupClient databasev1.GroupRegistryServiceClient
	var streamClient databasev1.StreamRegistryServiceClient
	var measureClient databasev1.MeasureRegistryServiceClient
	if conn != nil {
		groupClient = databasev1.NewGroupRegistryServiceClient(conn)
		streamClient = databasev1.NewStreamRegistryServiceClient(conn)
		measureClient = databasev1.NewMeasureRegistryServiceClient(conn)
	}
	groupDir := filepath.Join(schemaDir, "groups")
	streamDir := filepath.Join(schemaDir, "streams")
	measureDir := filepath.Join(schemaDir, "measures")
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

func initializeIndexWithTopN(conn *grpc.ClientConn, dataDir string) error {
	var indexRuleClient databasev1.IndexRuleRegistryServiceClient
	var indexRuleBindingClient databasev1.IndexRuleBindingRegistryServiceClient
	var topNClient databasev1.TopNAggregationRegistryServiceClient
	if conn != nil {
		indexRuleClient = databasev1.NewIndexRuleRegistryServiceClient(conn)
		indexRuleBindingClient = databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
		topNClient = databasev1.NewTopNAggregationRegistryServiceClient(conn)
	}
	indexRuleDir := filepath.Join(dataDir, "index_rules")
	indexRuleBindingDir := filepath.Join(dataDir, "index_rules_binding")
	topNDir := filepath.Join(dataDir, "topn")

	// index rules
	indexRules, err := readingGroupDirProtoList(indexRuleDir, func() *databasev1.IndexRule {
		return &databasev1.IndexRule{}
	})
	if err != nil {
		return fmt.Errorf("failed to read index rules: %w", err)
	}
	for group, rules := range indexRules {
		for _, r := range rules {
			if indexRuleClient != nil {
				_, err = indexRuleClient.Create(context.Background(), &databasev1.IndexRuleRegistryServiceCreateRequest{
					IndexRule: r,
				})
				if err = handlingCreateSchemaResult("IndexRule", fmt.Sprintf("%s/%s", group, r.Metadata.Name), err); err != nil {
					return err
				}
			}
		}
	}

	// index rule bindings
	indexRuleBindings, err := readingGroupDirProtoList(indexRuleBindingDir, func() *databasev1.IndexRuleBinding {
		return &databasev1.IndexRuleBinding{}
	})
	if err != nil {
		return fmt.Errorf("failed to read index rule bindings: %w", err)
	}
	for group, bindings := range indexRuleBindings {
		for _, b := range bindings {
			if indexRuleBindingClient != nil {
				_, err = indexRuleBindingClient.Create(context.Background(), &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
					IndexRuleBinding: b,
				})
				if err = handlingCreateSchemaResult("IndexRuleBinding", fmt.Sprintf("%s/%s", group, b.Metadata.Name), err); err != nil {
					return err
				}
			}
		}
	}

	// TopN aggregations
	topNs, err := readingGroupDirProtoList(topNDir, func() *databasev1.TopNAggregation {
		return &databasev1.TopNAggregation{}
	})
	if err != nil {
		return fmt.Errorf("failed to read TopN aggregations: %w", err)
	}
	for group, tops := range topNs {
		for _, t := range tops {
			if topNClient != nil {
				_, err = topNClient.Create(context.Background(), &databasev1.TopNAggregationRegistryServiceCreateRequest{
					TopNAggregation: t,
				})
				if err = handlingCreateSchemaResult("TopNAggregation", fmt.Sprintf("%s/%s", group, t.Metadata.Name), err); err != nil {
					return err
				}
			}
		}
	}

	return nil
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

func readingGroupDirProtoList[T proto.Message](dir string, newElem func() T) (map[string][]T, error) {
	out := make(map[string][]T)
	list, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, node := range list {
		if !node.IsDir() {
			continue
		}
		groupName := node.Name()
		groupDir := filepath.Join(dir, groupName)
		schemas, err := readingDirProtoList(groupDir, newElem)
		if err != nil {
			return nil, fmt.Errorf("failed to read group dir %s: %w", groupDir, err)
		}
		out[groupName] = schemas
	}
	return out, nil
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
			return nil, fmt.Errorf("json->proto err: file: %s, data: %s, %w", filePath, data, err)
		}
		out = append(out, elem)
	}
	return out, nil
}

const (
	serviceNameSplit   = "|"
	serviceNameAny     = "*"
	serviceNameUnknown = "UNKNOWN"
)

type serviceName struct {
	subset    string
	service   string
	namespace string
	cluster   string
	env       string
	hostname  string

	hostnameService  bool
	unknownService   bool
	highLevelService bool

	original string
}

func parseServiceName(name string) *serviceName {
	parts := strings.Split(name, serviceNameSplit)
	l := len(parts)

	s := &serviceName{original: name}

	switch l {
	case 5:
		// subset|service|namespace|cluster|env
		s.subset = parts[0]
		s.service = parts[1]
		s.namespace = parts[2]
		s.cluster = parts[3]
		s.env = parts[4]
		s.unknownService = s.subset == serviceNameAny &&
			s.service == serviceNameUnknown &&
			s.namespace == serviceNameAny
	case 4:
		// hostname|service|cluster|env
		s.hostname = parts[0]
		s.service = parts[1]
		s.cluster = parts[2]
		s.env = parts[3]

		s.hostnameService = true
		s.unknownService = s.service == serviceNameUnknown

	case 3:
		// service|namespace|cluster
		s.service = parts[0]
		s.namespace = parts[1]
		s.cluster = parts[2]

		s.highLevelService = true
		s.unknownService = s.service == serviceNameUnknown && s.namespace == serviceNameAny
	default:
		panic(fmt.Sprintf("invalid serviceName: %s", name))
	}

	return s
}

func (s *serviceName) serviceID() string {
	return base64.StdEncoding.EncodeToString([]byte(s.original)) + ".1"
}

func (s *serviceName) toService(subset, cluster, service, env string) string {
	if s.hostnameService {
		return strings.Join([]string{s.hostname, service, cluster, env}, serviceNameSplit)
	} else if s.highLevelService {
		return strings.Join([]string{service, s.namespace, cluster}, serviceNameSplit)
	}
	return strings.Join([]string{subset, service, s.namespace, cluster, env}, serviceNameSplit)
}

func serviceNameToID(n string) string {
	return base64.StdEncoding.EncodeToString([]byte(n)) + ".1"
}
