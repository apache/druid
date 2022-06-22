-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

INSERT INTO druid_config (name, payload) VALUES ('coordinator.compaction.config', '{"compactionConfigs":[{"dataSource":"upgradeTest","taskPriority":25,"inputSegmentSizeBytes":100000000000000,"maxRowsPerSegment":null,"skipOffsetFromLatest":"P1D","tuningConfig":{"maxRowsInMemory":null,"maxBytesInMemory":null,"maxTotalRows":null,"splitHintSpec":null,"partitionsSpec":{"type":"hashed","numShards":null,"partitionDimensions":[],"partitionFunction":"murmur3_32_abs","maxRowsPerSegment":5000000},"indexSpec":null,"indexSpecForIntermediatePersists":null,"maxPendingPersists":null,"pushTimeout":null,"segmentWriteOutMediumFactory":null,"maxNumConcurrentSubTasks":null,"maxRetry":null,"taskStatusCheckPeriodMs":null,"chatHandlerTimeout":null,"chatHandlerNumRetries":null,"maxNumSegmentsToMerge":null,"totalNumMergeTasks":null,"forceGuaranteedRollup":true,"type":"index_parallel"},"taskContext":null}],"compactionTaskSlotRatio":0.1,"maxCompactionTaskSlots":2147483647}');
