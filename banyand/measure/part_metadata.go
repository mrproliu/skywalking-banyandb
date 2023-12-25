// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"encoding/json"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type partMetadata struct {
	CompressedSizeBytes   uint64
	UncompressedSizeBytes uint64
	TotalCount            uint64
	BlocksCount           uint64
	MinTimestamp          int64
	MaxTimestamp          int64
	ID                    uint64
}

func (pm *partMetadata) reset() {
	pm.CompressedSizeBytes = 0
	pm.UncompressedSizeBytes = 0
	pm.TotalCount = 0
	pm.BlocksCount = 0
	pm.MinTimestamp = 0
	pm.MaxTimestamp = 0
	pm.ID = 0
}

func (pm *partMetadata) mustReadMetadata(fileSystem fs.FileSystem, partPath string) {
	pm.reset()

	metadataPath := filepath.Join(partPath, metadataFilename)
	metadata, err := fileSystem.Read(metadataPath)
	if err != nil {
		logger.Panicf("cannot read %s", err)
		return
	}
	if err := json.Unmarshal(metadata, pm); err != nil {
		logger.Panicf("cannot parse %q: %s", metadataPath, err)
		return
	}

	if pm.MinTimestamp > pm.MaxTimestamp {
		logger.Panicf("MinTimestamp cannot exceed MaxTimestamp; got %d vs %d", pm.MinTimestamp, pm.MaxTimestamp)
	}
}

func (pm *partMetadata) mustWriteMetadata(fileSystem fs.FileSystem, partPath string) {
	metadata, err := json.Marshal(pm)
	if err != nil {
		logger.Panicf("cannot marshal metadata: %s", err)
		return
	}
	metadataPath := filepath.Join(partPath, metadataFilename)
	n, err := fileSystem.Write(metadata, metadataPath, filePermission)
	if err != nil {
		logger.Panicf("cannot write metadata: %s", err)
		return
	}
	if n != len(metadata) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", metadataPath, n, len(metadata))
	}
}