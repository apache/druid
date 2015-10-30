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

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.util.Objects;

public class SegmentIdentifier
{
  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final ShardSpec shardSpec;
  private final String asString;

  @JsonCreator
  public SegmentIdentifier(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("shardSpec") ShardSpec shardSpec
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.version = Preconditions.checkNotNull(version, "version");
    this.shardSpec = Preconditions.checkNotNull(shardSpec, "shardSpec");
    this.asString = DataSegment.makeDataSegmentIdentifier(
        dataSource,
        interval.getStart(),
        interval.getEnd(),
        version,
        shardSpec
    );
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  public String getIdentifierAsString()
  {
    return asString;
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
    SegmentIdentifier that = (SegmentIdentifier) o;
    return Objects.equals(asString, that.asString);
  }

  @Override
  public int hashCode()
  {
    return asString.hashCode();
  }

  @Override
  public String toString()
  {
    return asString;
  }

  public static SegmentIdentifier fromDataSegment(final DataSegment segment)
  {
    return new SegmentIdentifier(
        segment.getDataSource(),
        segment.getInterval(),
        segment.getVersion(),
        segment.getShardSpec()
    );
  }
}
