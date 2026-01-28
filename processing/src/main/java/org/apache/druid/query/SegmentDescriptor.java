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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;

/**
 * This class is a pointer to a time range of a segment to be queried. The three pieces of this class (interval,
 * version, and partition number) are typically passed to {@link VersionedIntervalTimeline#findChunk} to retrieve an
 * actual queryable object.
 *
 * Like {@link SegmentId}, instances of this class reference a segment. However, there are two major differences:
 *
 * 1) The {@link #getInterval()} of this class may be narrower than the {@link SegmentId#getInterval()} of the segment
 * being referenced. This happens when this class is being used to reference a specific time range of a segment, rather
 * than the entire segment.
 *
 * 2) The {@link SegmentId#getDataSource()} field is absent from this class. Whenever this class is used, the datasource
 * is known from some out-of-band information, such as the {@link DataSource} object of a query.
 *
 * Omitting the datasource, and abbreviating JSON field names, helps limit the serialized size of this class.
 */
public class SegmentDescriptor
{
  private final Interval interval;
  private final String version;
  private final int partitionNumber;

  @JsonCreator
  public SegmentDescriptor(
      @JsonProperty("itvl") Interval interval,
      @JsonProperty("ver") String version,
      @JsonProperty("part") int partitionNumber
  )
  {
    this.interval = interval;
    this.version = version;
    this.partitionNumber = partitionNumber;
  }

  @JsonProperty("itvl")
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty("ver")
  public String getVersion()
  {
    return version;
  }

  @JsonProperty("part")
  public int getPartitionNumber()
  {
    return partitionNumber;
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

    SegmentDescriptor that = (SegmentDescriptor) o;

    if (partitionNumber != that.partitionNumber) {
      return false;
    }
    if (interval != null ? !interval.equals(that.interval) : that.interval != null) {
      return false;
    }
    if (version != null ? !version.equals(that.version) : that.version != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = interval != null ? interval.hashCode() : 0;
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + partitionNumber;
    return result;
  }

  @Override
  public String toString()
  {
    return "SegmentDescriptor{" +
           "interval=" + interval +
           ", version='" + version + '\'' +
           ", partitionNumber=" + partitionNumber +
           '}';
  }
}
