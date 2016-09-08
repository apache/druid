/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.Jobby;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = HashedPartitionsSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "dimension", value = SingleDimensionPartitionsSpec.class),
    @JsonSubTypes.Type(name = "hashed", value = HashedPartitionsSpec.class)
})
public interface PartitionsSpec
{
  @JsonIgnore
  public Jobby getPartitionJob(HadoopDruidIndexerConfig config);

  @JsonProperty
  public long getTargetPartitionSize();

  @JsonProperty
  public long getMaxPartitionSize();

  @JsonProperty
  public boolean isAssumeGrouped();

  @JsonIgnore
  public boolean isDeterminingPartitions();

  @JsonProperty
  public int getNumShards();

  @JsonProperty
  public List<String> getPartitionDimensions();
}
