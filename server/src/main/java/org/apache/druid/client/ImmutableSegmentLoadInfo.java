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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

import java.util.Set;

public class ImmutableSegmentLoadInfo
{
  private final DataSegment segment;
  private final ImmutableSet<DruidServerMetadata> servers;

  @JsonCreator
  public ImmutableSegmentLoadInfo(
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("servers") Set<DruidServerMetadata> servers
  )
  {
    Preconditions.checkNotNull(segment, "segment");
    Preconditions.checkNotNull(servers, "servers");
    this.segment = segment;
    this.servers = ImmutableSet.copyOf(servers);
  }

  @JsonProperty("segment")
  public DataSegment getSegment()
  {
    return segment;
  }

  @JsonProperty("servers")
  public ImmutableSet<DruidServerMetadata> getServers()
  {
    return servers;
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

    ImmutableSegmentLoadInfo that = (ImmutableSegmentLoadInfo) o;

    if (!segment.equals(that.segment)) {
      return false;
    }
    return servers.equals(that.servers);

  }

  @Override
  public int hashCode()
  {
    int result = segment.hashCode();
    result = 31 * result + servers.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "SegmentLoadInfo{" +
           "segment=" + segment +
           ", servers=" + servers +
           '}';
  }
}
