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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.joda.time.Interval;

/**
 * This class represents the intermediary data server where the partition of {@code interval} and {@code shardSpec}
 * is stored.
 */
public class GenericPartitionLocation extends PartitionLocation<BuildingShardSpec>
{
  @JsonCreator
  public GenericPartitionLocation(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("useHttps") boolean useHttps,
      @JsonProperty("subTaskId") String subTaskId,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("shardSpec") BuildingShardSpec shardSpec
  )
  {
    super(host, port, useHttps, subTaskId, interval, shardSpec);
  }

  @JsonIgnore
  @Override
  public int getBucketId()
  {
    return getSecondaryPartition().getBucketId();
  }

  @JsonProperty
  BuildingShardSpec getShardSpec()
  {
    return getSecondaryPartition();
  }
}
