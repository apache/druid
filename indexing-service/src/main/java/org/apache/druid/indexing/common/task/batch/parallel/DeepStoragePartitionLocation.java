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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.joda.time.Interval;

import java.util.Map;
import java.util.Objects;

public class DeepStoragePartitionLocation extends GenericPartitionLocation
{
  private final Map<String, Object> loadSpec;

  @JsonCreator
  public DeepStoragePartitionLocation(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("useHttps") boolean useHttps,
      @JsonProperty("subTaskId") String subTaskId,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("shardSpec") BuildingShardSpec shardSpec,
      @JsonProperty("loadSpec") Map<String, Object> loadSpec
  )
  {
    super(host, port, useHttps, subTaskId, interval, shardSpec);
    this.loadSpec = loadSpec;
  }

  @JsonProperty
  public Map<String, Object> getLoadSpec()
  {
    return loadSpec;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DeepStoragePartitionLocation that = (DeepStoragePartitionLocation) o;
    return loadSpec.equals(that.loadSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), loadSpec);
  }

  @Override
  public String toString()
  {
    return super.toString() + "loadSpec = " + loadSpec.toString();
  }
}
