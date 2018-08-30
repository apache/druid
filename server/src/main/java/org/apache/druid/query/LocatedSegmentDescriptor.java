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
import com.google.common.collect.ImmutableList;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

/**
 * public, evolving
 * <p/>
 * extended version of SegmentDescriptor, which is internal class, with location and size information attached
 */
public class LocatedSegmentDescriptor
{
  private final Interval interval;
  private final String version;
  private final int partitionNumber;
  private final long size;
  private final List<DruidServerMetadata> locations;

  @JsonCreator
  public LocatedSegmentDescriptor(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("partitionNumber") int partitionNumber,
      @JsonProperty("size") long size,
      @JsonProperty("locations") List<DruidServerMetadata> locations
  )
  {
    this.interval = interval;
    this.version = version;
    this.partitionNumber = partitionNumber;
    this.size = size;
    this.locations = locations == null ? ImmutableList.of() : locations;
  }

  public LocatedSegmentDescriptor(SegmentDescriptor descriptor, long size, List<DruidServerMetadata> candidates)
  {
    this(descriptor.getInterval(), descriptor.getVersion(), descriptor.getPartitionNumber(), size, candidates);
  }

  @JsonProperty("interval")
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty("version")
  public String getVersion()
  {
    return version;
  }

  @JsonProperty("partitionNumber")
  public int getPartitionNumber()
  {
    return partitionNumber;
  }

  @JsonProperty("size")
  public long getSize()
  {
    return size;
  }

  @JsonProperty("locations")
  public List<DruidServerMetadata> getLocations()
  {
    return locations;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof LocatedSegmentDescriptor)) {
      return false;
    }

    LocatedSegmentDescriptor other = (LocatedSegmentDescriptor) o;

    if (partitionNumber != other.partitionNumber) {
      return false;
    }
    if (!Objects.equals(interval, other.interval)) {
      return false;
    }
    if (!Objects.equals(version, other.version)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionNumber, interval, version);
  }

  @Override
  public String toString()
  {
    return "LocatedSegmentDescriptor{" +
           "interval=" + interval +
           ", version='" + version + '\'' +
           ", partitionNumber=" + partitionNumber +
           ", size=" + size +
           ", locations=" + locations +
           '}';
  }
}
