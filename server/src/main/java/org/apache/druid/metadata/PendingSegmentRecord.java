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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.sql.ResultSet;

/**
 * Representation of a record in the pending segments table. <br/>
 * Mapping of column in table to field:
 *
 * <ul>
 * <li>  id -> id (Unique identifier for pending segment) <li/>
 * <li>  sequence_name -> sequenceName (sequence name used for segment allocation) <li/>
 * <li>  sequence_prev_id -> sequencePrevId (previous segment id used for segment allocation) <li/>
 * <li>  upgraded_from_segment_id -> upgradedFromSegmentId (Id of the root segment from which this was upgraded) <li/>
 * <li>  task_allocator_id -> taskAllocatorId (Associates a task / task group / replica group with the pending segment) <li/>
 * </ul>
 */
public class PendingSegmentRecord
{
  private final SegmentIdWithShardSpec id;
  private final String sequenceName;
  private final String sequencePrevId;
  private final String upgradedFromSegmentId;
  private final String taskAllocatorId;

  @JsonCreator
  public PendingSegmentRecord(
      @JsonProperty("id") SegmentIdWithShardSpec id,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("sequencePrevId") String sequencePrevId,
      @JsonProperty("upgradedFromSegmentId") @Nullable String upgradedFromSegmentId,
      @JsonProperty("taskAllocatorId") @Nullable String taskAllocatorId
  )
  {
    this.id = id;
    this.sequenceName = sequenceName;
    this.sequencePrevId = sequencePrevId;
    this.upgradedFromSegmentId = upgradedFromSegmentId;
    this.taskAllocatorId = taskAllocatorId;
  }

  @JsonProperty
  public SegmentIdWithShardSpec getId()
  {
    return id;
  }

  @JsonProperty
  public String getSequenceName()
  {
    return sequenceName;
  }

  @JsonProperty
  public String getSequencePrevId()
  {
    return sequencePrevId;
  }

  /**
   * The original pending segment using which this upgraded segment was created.
   * Can be null for pending segments allocated before this column was added or for segments that have not been upgraded.
   */
  @Nullable
  @JsonProperty
  public String getUpgradedFromSegmentId()
  {
    return upgradedFromSegmentId;
  }

  /**
   * task / taskGroup / replica group of task that allocated this segment.
   * Can be null for pending segments allocated before this column was added.
   */
  @Nullable
  @JsonProperty
  public String getTaskAllocatorId()
  {
    return taskAllocatorId;
  }

  @SuppressWarnings("UnstableApiUsage")
  public String computeSequenceNamePrevIdSha1(boolean skipSegmentLineageCheck)
  {
    final Hasher hasher = Hashing.sha1().newHasher()
                                 .putBytes(StringUtils.toUtf8(getSequenceName()))
                                 .putByte((byte) 0xff);

    if (skipSegmentLineageCheck) {
      final Interval interval = getId().getInterval();
      hasher
          .putLong(interval.getStartMillis())
          .putLong(interval.getEndMillis());
    } else {
      hasher
          .putBytes(StringUtils.toUtf8(getSequencePrevId()));
    }

    hasher.putByte((byte) 0xff);
    hasher.putBytes(StringUtils.toUtf8(getId().getVersion()));

    return BaseEncoding.base16().encode(hasher.hash().asBytes());
  }

  public static PendingSegmentRecord fromResultSet(ResultSet resultSet, ObjectMapper jsonMapper)
  {
    try {
      final byte[] payload = resultSet.getBytes("payload");
      return new PendingSegmentRecord(
          jsonMapper.readValue(payload, SegmentIdWithShardSpec.class),
          resultSet.getString("sequence_name"),
          resultSet.getString("sequence_prev_id"),
          resultSet.getString("upgraded_from_segment_id"),
          resultSet.getString("task_allocator_id")
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
