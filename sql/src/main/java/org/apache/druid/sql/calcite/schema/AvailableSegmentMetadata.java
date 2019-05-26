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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Immutable representation of RowSignature and other segment attributes needed by {@link SystemSchema.SegmentsTable}
 * This class contains the metadata of segments announced by historicals or ingestion tasks.
 */
public class AvailableSegmentMetadata
{
  public static Builder builder(
      SegmentId segmentId,
      long isRealtime,
      Set<String> segmentServers,
      RowSignature rowSignature,
      long numRows
  )
  {
    return new Builder(segmentId, isRealtime, segmentServers, rowSignature, numRows);
  }

  public static Builder from(AvailableSegmentMetadata h)
  {
    return new Builder(
        h.getSegmentId(),
        h.isRealtime(),
        h.getReplicas(),
        h.getRowSignature(),
        h.getNumRows()
    );
  }

  private final SegmentId segmentId;
  // Booleans represented as long type, where 1 = true and 0 = false
  // to make it easy to count number of segments which are realtime
  private final long isRealtime;
  // set of servers that contain the segment
  private final Set<String> segmentServers;
  private final long numRows;
  @Nullable
  private final RowSignature rowSignature;

  private AvailableSegmentMetadata(Builder builder)
  {
    this.rowSignature = builder.rowSignature;
    this.isRealtime = builder.isRealtime;
    this.segmentServers = builder.segmentServers;
    this.numRows = builder.numRows;
    this.segmentId = builder.segmentId;
  }

  public long isRealtime()
  {
    return isRealtime;
  }

  public SegmentId getSegmentId()
  {
    return segmentId;
  }

  public Set<String> getReplicas()
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

  @Nullable
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  public static class Builder
  {
    private final SegmentId segmentId;
    private final long isRealtime;

    private Set<String> segmentServers;
    @Nullable
    private RowSignature rowSignature;
    private long numRows;

    private Builder(
        SegmentId segmentId,
        long isRealtime,
        Set<String> servers,
        RowSignature rowSignature,
        long numRows
    )
    {
      this.segmentId = segmentId;
      this.isRealtime = isRealtime;
      this.segmentServers = servers;
      this.rowSignature = rowSignature;
      this.numRows = numRows;
    }

    public Builder withRowSignature(RowSignature rowSignature)
    {
      this.rowSignature = rowSignature;
      return this;
    }

    public Builder withNumRows(long numRows)
    {
      this.numRows = numRows;
      return this;
    }

    public Builder withReplicas(Set<String> servers)
    {
      this.segmentServers = servers;
      return this;
    }

    public AvailableSegmentMetadata build()
    {
      return new AvailableSegmentMetadata(this);
    }
  }

}
