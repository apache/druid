/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.joda.time.Interval;

/**
 * This class represents the intermediary data server where the partition of {@link #getInterval()} and
 * {@link #getBucketId()} is stored.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = GenericPartitionLocation.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = GenericPartitionStat.TYPE, value = GenericPartitionLocation.class),
    @JsonSubTypes.Type(name = DeepStoragePartitionStat.TYPE, value = DeepStoragePartitionLocation.class)
})
public interface PartitionLocation
{
  int getBucketId();
  Interval getInterval();
  BuildingShardSpec getShardSpec();
  String getSubTaskId();
}
