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
import org.joda.time.Interval;

/**
 * The difference between this class and {@link org.apache.druid.timeline.SegmentId} is that this class is a "light"
 * version of {@link org.apache.druid.timeline.SegmentId}, that only contains the interval, version, and partition
 * number. It's used where the data source, another essential part of {@link org.apache.druid.timeline.SegmentId}
 * is determined by the context (e. g. in org.apache.druid.client.CachingClusteredClient, where SegmentDescriptor is
 * used when Brokers tell data servers which segments to include for a particular query) and where having lean JSON
 * representations is important, because it's actively transferred detween Druid nodes. It's also for this reason that
 * the JSON field names of SegmentDescriptor are abbreviated.
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
