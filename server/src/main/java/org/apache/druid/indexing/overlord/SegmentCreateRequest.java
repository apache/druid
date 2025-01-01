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

package org.apache.druid.indexing.overlord;

import org.apache.druid.timeline.partition.PartialShardSpec;

/**
 * Contains information used by {@link IndexerMetadataStorageCoordinator} for
 * creating a new segment.
 * <p>
 * The {@code sequenceName} and {@code previousSegmentId} fields are meant to
 * make it easy for two independent ingestion tasks to produce the same series
 * of segments.
 */
public class SegmentCreateRequest
{
  // DO NOT IMPLEMENT equals or hashCode for this class as each request must be
  // treated as unique even if it is for the same parameters

  private final String version;
  private final String sequenceName;
  private final String previousSegmentId;
  private final PartialShardSpec partialShardSpec;
  private final String taskAllocatorId;

  public SegmentCreateRequest(
      String sequenceName,
      String previousSegmentId,
      String version,
      PartialShardSpec partialShardSpec,
      String taskAllocatorId
  )
  {
    this.sequenceName = sequenceName;
    this.previousSegmentId = previousSegmentId == null ? "" : previousSegmentId;
    this.version = version;
    this.partialShardSpec = partialShardSpec;
    this.taskAllocatorId = taskAllocatorId;
  }

  /**
   * Represents group of ingestion tasks that produce a segment series.
   */
  public String getSequenceName()
  {
    return sequenceName;
  }

  /**
   * Previous segment id allocated for this sequence.
   *
   * @return Empty string if there is no previous segment in the series.
   */
  public String getPreviousSegmentId()
  {
    return previousSegmentId;
  }

  /**
   * Version of the lock held by the task that has requested the segment allocation.
   * The allocated segment must have a version less than or equal to this version.
   */
  public String getVersion()
  {
    return version;
  }

  public PartialShardSpec getPartialShardSpec()
  {
    return partialShardSpec;
  }

  public String getTaskAllocatorId()
  {
    return taskAllocatorId;
  }
}
