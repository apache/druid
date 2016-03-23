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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.druid.server.coordination.DruidServerMetadata;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

/**
 */
public class LocatedSegmentDescriptor extends SegmentDescriptor
{
  private final long size;
  private final List<DruidServerMetadata> locations;

  @JsonCreator
  public LocatedSegmentDescriptor(
      @JsonProperty("itvl") Interval interval,
      @JsonProperty("ver") String version,
      @JsonProperty("part") int partitionNumber,
      @JsonProperty("size") long size,
      @JsonProperty("locations") List<DruidServerMetadata> locations
  )
  {
    super(interval, version, partitionNumber);
    this.size = size;
    this.locations = locations == null ? ImmutableList.<DruidServerMetadata>of() : locations;
  }

  public LocatedSegmentDescriptor(SegmentDescriptor descriptor, long size, List<DruidServerMetadata> candidates)
  {
    this(descriptor.getInterval(), descriptor.getVersion(), descriptor.getPartitionNumber(), size, candidates);
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
    if (!(o instanceof LocatedSegmentDescriptor) || !super.equals(o)) {
      return false;
    }

    LocatedSegmentDescriptor other = (LocatedSegmentDescriptor) o;
    return getHostNames().equals(other.getHostNames());
  }

  private Set<String> getHostNames()
  {
    Set<String> hostNames = Sets.newHashSet();
    for (DruidServerMetadata meta : locations) {
      hostNames.add(meta.getHost());
    }
    return hostNames;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + getHostNames().hashCode();
    return result;
  }
}
