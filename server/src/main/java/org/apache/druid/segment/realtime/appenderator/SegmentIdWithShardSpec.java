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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

/**
 * {@link SegmentId} with additional {@link ShardSpec} info. {@link #equals}/{@link #hashCode} and {@link
 * #compareTo} don't consider that additinal info.
 *
 * This class is separate from {@link SegmentId} because in a lot of places segment ids are transmitted as "segment id
 * strings" that don't contain enough information to deconstruct the {@link ShardSpec}. Also, even a single extra field
 * in {@link SegmentId} is important, because it adds to the memory footprint considerably.
 */
public final class SegmentIdWithShardSpec implements Comparable<SegmentIdWithShardSpec>
{
  private final SegmentId id;
  private final ShardSpec shardSpec;
  private final String asString;

  @JsonCreator
  public SegmentIdWithShardSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("shardSpec") ShardSpec shardSpec
  )
  {
    this.id = SegmentId.of(dataSource, interval, version, shardSpec.getPartitionNum());
    this.shardSpec = Preconditions.checkNotNull(shardSpec, "shardSpec");
    this.asString = id.toString();
  }

  public SegmentId asSegmentId()
  {
    return id;
  }

  @JsonProperty
  public String getDataSource()
  {
    return id.getDataSource();
  }

  @JsonProperty
  public Interval getInterval()
  {
    return id.getInterval();
  }

  @JsonProperty
  public String getVersion()
  {
    return id.getVersion();
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentIdWithShardSpec)) {
      return false;
    }
    SegmentIdWithShardSpec that = (SegmentIdWithShardSpec) o;
    return id.equals(that.id);
  }

  @Override
  public int hashCode()
  {
    return id.hashCode();
  }

  @Override
  public int compareTo(SegmentIdWithShardSpec o)
  {
    return id.compareTo(o.id);
  }

  @Override
  public String toString()
  {
    return asString;
  }

  public static SegmentIdWithShardSpec fromDataSegment(final DataSegment segment)
  {
    return new SegmentIdWithShardSpec(
        segment.getDataSource(),
        segment.getInterval(),
        segment.getVersion(),
        segment.getShardSpec()
    );
  }
}
