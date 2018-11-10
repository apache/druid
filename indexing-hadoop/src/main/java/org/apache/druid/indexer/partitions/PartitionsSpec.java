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

package org.apache.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.Jobby;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = HashedPartitionsSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "dimension", value = SingleDimensionPartitionsSpec.class),
    @JsonSubTypes.Type(name = "hashed", value = HashedPartitionsSpec.class)
})
public interface PartitionsSpec
{
  @JsonIgnore
  Jobby getPartitionJob(HadoopDruidIndexerConfig config);

  @JsonProperty
  long getTargetPartitionSize();

  @JsonProperty
  long getMaxPartitionSize();

  @JsonProperty
  boolean isAssumeGrouped();

  @JsonIgnore
  boolean isDeterminingPartitions();

  @JsonProperty
  int getNumShards();

  @JsonProperty
  List<String> getPartitionDimensions();
}
