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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data segment including the locations which contain the segment. Used if MSQ needs to fetch the segment from a server
 * instead of from deep storage.
 */
public class DataSegmentWithLocation extends DataSegment
{
  private final Set<DruidServerMetadata> servers;

  @JsonCreator
  public DataSegmentWithLocation(
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
      @JsonProperty("servers") Set<DruidServerMetadata> servers,
      @JacksonInject PruneSpecsHolder pruneSpecsHolder
  )
  {
    super(dataSource, interval, version, loadSpec, dimensions, metrics, shardSpec, lastCompactionState, binaryVersion, size, pruneSpecsHolder);
    this.servers = Preconditions.checkNotNull(servers, "servers");
  }

  public DataSegmentWithLocation(
      DataSegment dataSegment,
      Set<DruidServerMetadata> servers
  )
  {
    super(
        dataSegment.getDataSource(),
        dataSegment.getInterval(),
        dataSegment.getVersion(),
        dataSegment.getLoadSpec(),
        dataSegment.getDimensions(),
        dataSegment.getMetrics(),
        dataSegment.getShardSpec(),
        dataSegment.getBinaryVersion(),
        dataSegment.getSize()
    );
    this.servers = servers;
  }

  @JsonProperty("servers")
  public Set<DruidServerMetadata> getServers()
  {
    return servers;
  }
}
