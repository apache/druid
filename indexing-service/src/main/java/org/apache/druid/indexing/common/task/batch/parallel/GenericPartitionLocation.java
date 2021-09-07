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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.joda.time.Interval;

import java.net.URI;
import java.util.Objects;

/**
 * This class represents the intermediary data server where the partition of {@code interval} and {@code shardSpec}
 * is stored.
 */
public class GenericPartitionLocation implements PartitionLocation
{
  private final String host;
  private final int port;
  private final boolean useHttps;
  private final String subTaskId;
  private final Interval interval;
  private final BuildingShardSpec shardSpec;

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
    this.host = host;
    this.port = port;
    this.useHttps = useHttps;
    this.subTaskId = subTaskId;
    this.interval = interval;
    this.shardSpec = shardSpec;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public boolean isUseHttps()
  {
    return useHttps;
  }

  @JsonProperty
  @Override
  public String getSubTaskId()
  {
    return subTaskId;
  }

  @JsonProperty
  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @JsonIgnore
  @Override
  public int getBucketId()
  {
    return shardSpec.getBucketId();
  }

  @JsonProperty
  @Override
  public BuildingShardSpec getShardSpec()
  {
    return shardSpec;
  }

  final URI toIntermediaryDataServerURI(String supervisorTaskId)
  {
    return URI.create(
        StringUtils.format(
            "%s://%s:%d/druid/worker/v1/shuffle/task/%s/%s/partition?startTime=%s&endTime=%s&bucketId=%d",
            useHttps ? "https" : "http",
            host,
            port,
            StringUtils.urlEncode(supervisorTaskId),
            StringUtils.urlEncode(subTaskId),
            interval.getStart(),
            interval.getEnd(),
            getBucketId()
        )
    );
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
    GenericPartitionLocation that = (GenericPartitionLocation) o;
    return port == that.port
           && useHttps == that.useHttps
           && host.equals(that.host)
           && subTaskId.equals(that.subTaskId)
           && interval.equals(that.interval)
           && shardSpec.equals(that.shardSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, port, useHttps, subTaskId, interval, shardSpec);
  }

  @Override
  public String toString()
  {
    return "GenericPartitionLocation{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", useHttps=" + useHttps +
           ", subTaskId='" + subTaskId + '\'' +
           ", interval=" + interval +
           ", shardSpec=" + shardSpec +
           '}';
  }
}
