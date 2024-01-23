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

package org.apache.druid.server.http;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.jackson.CommaListJoinSerializer;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * {@link DataSegment} to be transmitted externally
 */
@PublicApi
public class DataSegmentDto
{
  private final DataSegment dataSegment;
  private final DateTime createdDate;
  @Nullable
  private final DateTime usedStatusLastUpdatedDate;

  public DataSegmentDto(
      DataSegment dataSegment,
      DateTime createdDate,
      @Nullable DateTime usedStatusLastUpdatedDate
  )
  {
    this.dataSegment = dataSegment;
    this.createdDate = createdDate;
    this.usedStatusLastUpdatedDate = usedStatusLastUpdatedDate;
  }

  @JsonCreator
  public DataSegmentDto(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      // use `Map` *NOT* `LoadSpec` because we want to do lazy materialization to prevent dependency pollution
      @JsonProperty("loadSpec") @Nullable Map<String, Object> loadSpec,
      @JsonProperty("dimensions")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> dimensions,
      @JsonProperty("metrics")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> metrics,
      @JsonProperty("shardSpec") @Nullable ShardSpec shardSpec,
      @JsonProperty("lastCompactionState") @Nullable CompactionState lastCompactionState,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size,
      @JsonProperty("createdDate") @Nullable DateTime createdDate,
      @JsonProperty("createdDate") @Nullable DateTime usedStatusLastUpdatedDate,
      @JacksonInject DataSegment.PruneSpecsHolder pruneSpecsHolder
  )
  {
    this.dataSegment = new DataSegment(
        dataSource,
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        lastCompactionState,
        binaryVersion,
        size,
        pruneSpecsHolder
    );
    this.createdDate = createdDate;
    this.usedStatusLastUpdatedDate = usedStatusLastUpdatedDate;
  }

  /**
   * Get dataSource
   *
   * @return the dataSource
   */
  @JsonProperty
  public String getDataSource()
  {
    return dataSegment.getDataSource();
  }

  @JsonProperty
  public Interval getInterval()
  {
    return dataSegment.getInterval();
  }

  @Nullable
  @JsonProperty
  public Map<String, Object> getLoadSpec()
  {
    return dataSegment.getLoadSpec();
  }

  @JsonProperty("version")
  public String getVersion()
  {
    return dataSegment.getVersion();
  }

  @JsonProperty
  @JsonSerialize(using = CommaListJoinSerializer.class)
  public List<String> getDimensions()
  {
    return dataSegment.getDimensions();
  }

  @JsonProperty
  @JsonSerialize(using = CommaListJoinSerializer.class)
  public List<String> getMetrics()
  {
    return dataSegment.getMetrics();
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return dataSegment.getShardSpec();
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public CompactionState getLastCompactionState()
  {
    return dataSegment.getLastCompactionState();
  }

  @JsonProperty
  public Integer getBinaryVersion()
  {
    return dataSegment.getBinaryVersion();
  }

  @JsonProperty
  public long getSize()
  {
    return dataSegment.getSize();
  }

  @JsonProperty
  public DateTime getCreatedDate()
  {
    return createdDate;
  }

  @Nullable
  @JsonProperty
  public DateTime getUsedStatusLastUpdatedDate()
  {
    return usedStatusLastUpdatedDate;
  }

  // "identifier" for backward compatibility of JSON API
  @JsonProperty(value = "identifier", access = JsonProperty.Access.READ_ONLY)
  public SegmentId getId()
  {
    return dataSegment.getId();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSegmentDto) {
      return getId().equals(((DataSegmentDto) o).getId());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return getId().hashCode();
  }

  @Override
  public String toString()
  {
    return "DataSegment{" +
           "binaryVersion=" + getBinaryVersion() +
           ", id=" + getId() +
           ", loadSpec=" + getLoadSpec() +
           ", dimensions=" + getDimensions() +
           ", metrics=" + getMetrics() +
           ", shardSpec=" + getShardSpec() +
           ", lastCompactionState=" + getLastCompactionState() +
           ", size=" + getSize() +
           '}';
  }
}
