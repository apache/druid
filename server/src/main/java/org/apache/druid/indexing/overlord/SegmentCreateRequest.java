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
  private final String version;
  private final String sequenceName;
  private final String previousSegmentId;
  private final PartialShardSpec partialShardSpec;

  public SegmentCreateRequest(
      String sequenceName,
      String previousSegmentId,
      String version,
      PartialShardSpec partialShardSpec
  )
  {
    this.sequenceName = sequenceName;
    this.previousSegmentId = previousSegmentId == null ? "" : previousSegmentId;
    this.version = version;
    this.partialShardSpec = partialShardSpec;
  }

  public String getUniqueSequenceId()
  {
    return getUniqueSequenceId(sequenceName, previousSegmentId);
  }

  public String getSequenceName()
  {
    return sequenceName;
  }

  /**
   * Non-null previous segment id. This can be used for persisting to the
   * pending segments table in the metadata store.
   */
  public String getPreviousSegmentId()
  {
    return previousSegmentId;
  }

  public String getVersion()
  {
    return version;
  }

  public PartialShardSpec getPartialShardSpec()
  {
    return partialShardSpec;
  }

  /**
   * Returns a String representing (sequenceName + previousSegmentId) used to
   * uniquely identify a segment.
   */
  public static String getUniqueSequenceId(String sequenceName, String previousSegmentId)
  {
    return sequenceName + "####" + previousSegmentId;
  }
}
