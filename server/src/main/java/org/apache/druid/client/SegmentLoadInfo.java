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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Overshadowable;

import java.util.Set;

public class SegmentLoadInfo implements Overshadowable<SegmentLoadInfo>
{
  private final DataSegment segment;
  private final Set<DruidServerMetadata> servers;

  public SegmentLoadInfo(DataSegment segment)
  {
    Preconditions.checkNotNull(segment, "segment");
    this.segment = segment;
    this.servers = Sets.newConcurrentHashSet();
  }

  public boolean addServer(DruidServerMetadata server)
  {
    return servers.add(server);
  }

  public boolean removeServer(DruidServerMetadata server)
  {
    return servers.remove(server);
  }

  public boolean isEmpty()
  {
    return servers.isEmpty();
  }

  public ImmutableSegmentLoadInfo toImmutableSegmentLoadInfo()
  {
    return new ImmutableSegmentLoadInfo(segment, servers);
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

    SegmentLoadInfo that = (SegmentLoadInfo) o;

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

  @Override
  public boolean overshadows(SegmentLoadInfo other)
  {
    return segment.overshadows(other.segment);
  }

  @Override
  public int getStartRootPartitionId()
  {
    return segment.getStartRootPartitionId();
  }

  @Override
  public int getEndRootPartitionId()
  {
    return segment.getEndRootPartitionId();
  }

  @Override
  public String getVersion()
  {
    return segment.getVersion();
  }

  @Override
  public short getMinorVersion()
  {
    return segment.getMinorVersion();
  }

  @Override
  public short getAtomicUpdateGroupSize()
  {
    return segment.getAtomicUpdateGroupSize();
  }
}
