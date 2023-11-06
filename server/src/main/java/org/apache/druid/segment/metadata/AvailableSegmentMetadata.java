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

package org.apache.druid.segment.metadata;

import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

import java.util.Set;

/**
 * Immutable representation of RowSignature and other segment attributes.
 * This class contains the metadata of segments announced by historicals or ingestion tasks.
 */
public class AvailableSegmentMetadata
{
  public static Builder builder(
      DataSegment segment,
      long isRealtime,
      Set<DruidServerMetadata> segmentServers,
      long numRows
  )
  {
    return new Builder(segment, isRealtime, segmentServers, numRows);
  }

  public static Builder from(AvailableSegmentMetadata h)
  {
    return new Builder(
        h.getSegment(),
        h.isRealtime(),
        h.getReplicas(),
        h.getNumRows()
    );
  }

  private final DataSegment segment;
  // Booleans represented as long type, where 1 = true and 0 = false
  // to make it easy to count number of segments which are realtime
  private final long isRealtime;
  // set of servers that contain the segment
  private final Set<DruidServerMetadata> segmentServers;
  private final long numRows;

  private AvailableSegmentMetadata(Builder builder)
  {
    this.isRealtime = builder.isRealtime;
    this.segmentServers = builder.segmentServers;
    this.numRows = builder.numRows;
    this.segment = builder.segment;
  }

  public long isRealtime()
  {
    return isRealtime;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public Set<DruidServerMetadata> getReplicas()
  {
    return segmentServers;
  }

  public long getNumReplicas()
  {
    return segmentServers.size();
  }

  public long getNumRows()
  {
    return numRows;
  }

  public static class Builder
  {
    private final DataSegment segment;

    private long isRealtime;
    private Set<DruidServerMetadata> segmentServers;
    private long numRows;

    private Builder(
        DataSegment segment,
        long isRealtime,
        Set<DruidServerMetadata> servers,
        long numRows
    )
    {
      this.segment = segment;
      this.isRealtime = isRealtime;
      this.segmentServers = servers;
      this.numRows = numRows;
    }

    public Builder withNumRows(long numRows)
    {
      this.numRows = numRows;
      return this;
    }

    public Builder withReplicas(Set<DruidServerMetadata> servers)
    {
      this.segmentServers = servers;
      return this;
    }

    public Builder withRealtime(long isRealtime)
    {
      this.isRealtime = isRealtime;
      return this;
    }

    public AvailableSegmentMetadata build()
    {
      return new AvailableSegmentMetadata(this);
    }
  }
}
