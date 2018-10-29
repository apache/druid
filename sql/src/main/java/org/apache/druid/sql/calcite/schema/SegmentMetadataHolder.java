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

import javax.annotation.Nullable;

/**
 * Immutable representation of RowSignature and other segment attributes needed by {@link SystemSchema.SegmentsTable}
 */
public class SegmentMetadataHolder
{

  // Booleans represented as long type, where 1 = true and 0 = false
  // to make it easy to count number of segments which are
  // published, available or realtime etc.
  private final long isPublished;
  private final long isAvailable;
  private final long isRealtime;
  private final String segmentId;
  private final long numReplicas;
  private final long numRows;
  @Nullable
  private final RowSignature rowSignature;

  private SegmentMetadataHolder(Builder builder)
  {
    this.rowSignature = builder.rowSignature;
    this.isPublished = builder.isPublished;
    this.isAvailable = builder.isAvailable;
    this.isRealtime = builder.isRealtime;
    this.numReplicas = builder.numReplicas;
    this.numRows = builder.numRows;
    this.segmentId = builder.segmentId;
  }

  public long isPublished()
  {
    return isPublished;
  }

  public long isAvailable()
  {
    return isAvailable;
  }

  public long isRealtime()
  {
    return isRealtime;
  }

  public String getSegmentId()
  {
    return segmentId;
  }

  public long getNumReplicas()
  {
    return numReplicas;
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
    private final String segmentId;
    private final long isPublished;
    private final long isAvailable;
    private final long isRealtime;

    private long numReplicas;
    @Nullable
    private RowSignature rowSignature;
    private long numRows;

    public Builder(
        String segmentId,
        long isPublished,
        long isAvailable,
        long isRealtime,
        long numReplicas
    )
    {
      this.segmentId = segmentId;
      this.isPublished = isPublished;
      this.isAvailable = isAvailable;
      this.isRealtime = isRealtime;
      this.numReplicas = numReplicas;
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

    public Builder withNumReplicas(long numReplicas)
    {
      this.numReplicas = numReplicas;
      return this;
    }

    public SegmentMetadataHolder build()
    {
      return new SegmentMetadataHolder(this);
    }
  }

}
