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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.joda.time.DateTime;
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
 * <li>  upgraded_from_segment_id -> upgradedFromSegmentId
 * (ID of the segment which was upgraded to create the current segment.
 * If the former was itself created as a result of an upgrade, then this ID
 * must refer to the original non-upgraded segment in the hierarchy.) <li/>
 * <li>  task_allocator_id -> taskAllocatorId (Associates a task / task group / replica group with the pending segment) <li/>
 * </ul>
 */
public class PendingSegmentRecord
{
  /**
   * Default lock version used by concurrent APPEND tasks.
   */
  public static final String DEFAULT_VERSION_FOR_CONCURRENT_APPEND = DateTimes.EPOCH.toString();

  /**
   * Suffix to use to construct fresh segment versions in the event of a clash.
   * The chosen character {@code S} is just for visual ease so that two versions
   * are not easily confused for each other.
   * {@code 1970-01-01T00:00:00.000Z_1} vs {@code 1970-01-01T00:00:00.000ZS_1}.
   */
  public static final String CONCURRENT_APPEND_VERSION_SUFFIX = "S";

  private final SegmentIdWithShardSpec id;
  private final String sequenceName;
  private final String sequencePrevId;
  private final String upgradedFromSegmentId;
  private final String taskAllocatorId;
  private final DateTime createdDate;

  @JsonCreator
  public static PendingSegmentRecord fromJson(
      @JsonProperty("id") SegmentIdWithShardSpec id,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("sequencePrevId") String sequencePrevId,
      @JsonProperty("upgradedFromSegmentId") @Nullable String upgradedFromSegmentId,
      @JsonProperty("taskAllocatorId") @Nullable String taskAllocatorId
  )
  {
    return new PendingSegmentRecord(
        id,
        sequenceName,
        sequencePrevId,
        upgradedFromSegmentId,
        taskAllocatorId,
        // Tasks don't use the createdDate of the record
        DateTimes.EPOCH
    );
  }

  public PendingSegmentRecord(
      SegmentIdWithShardSpec id,
      String sequenceName,
      String sequencePrevId,
      String upgradedFromSegmentId,
      String taskAllocatorId,
      DateTime createdDate
  )
  {
    this.id = id;
    this.sequenceName = sequenceName;
    this.sequencePrevId = sequencePrevId == null ? "" : sequencePrevId;
    this.upgradedFromSegmentId = upgradedFromSegmentId;
    this.taskAllocatorId = taskAllocatorId;
    this.createdDate = createdDate;
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

  /**
   * Previous segment ID allocated for the same sequence.
   *
   * @return Empty string if there is no previous segment in the sequence.
   */
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

  /**
   * This field is not serialized since tasks do not use it.
   */
  @JsonIgnore
  public DateTime getCreatedDate()
  {
    return createdDate;
  }

  /**
   * Computes a hash for this record to serve as UNIQUE key, ensuring we don't
   * have more than one segment per sequence per interval.
   * A single column is used instead of (sequence_name, sequence_prev_id) as
   * some MySQL storage engines have difficulty with large unique keys
   * (see <a href="https://github.com/apache/druid/issues/2319">#2319</a>)
   */
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

  /**
   * Creates a new record (with the current timestamp) that can be used to create
   * a new entry in the pending segments metadata table.
   *
   * @return A new PendingSegmentRecord with the given parameters and the current
   * time as the created date.
   */
  public static PendingSegmentRecord create(
      SegmentIdWithShardSpec id,
      String sequenceName,
      String sequencePrevId,
      @Nullable String upgradedFromSegmentId,
      @Nullable String taskAllocatorId
  )
  {
    return new PendingSegmentRecord(
        id,
        sequenceName,
        sequencePrevId,
        upgradedFromSegmentId,
        taskAllocatorId,
        DateTimes.nowUtc()
    );
  }

  /**
   * Maps the given result set into a {@link PendingSegmentRecord}.
   * The columns required in the result set are:
   * <ul>
   * <li>{@code payload}</li>
   * <li>{@code sequence_name}</li>
   * <li>{@code sequence_prev_id}</li>
   * <li>{@code upgraded_from_segment_id}</li>
   * <li>{@code task_allocator_id}</li>
   * <li>{@code created_date}</li>
   * </ul>
   */
  public static PendingSegmentRecord fromResultSet(ResultSet resultSet, ObjectMapper jsonMapper)
  {
    try {
      final byte[] payload = resultSet.getBytes("payload");
      return new PendingSegmentRecord(
          jsonMapper.readValue(payload, SegmentIdWithShardSpec.class),
          resultSet.getString("sequence_name"),
          resultSet.getString("sequence_prev_id"),
          resultSet.getString("upgraded_from_segment_id"),
          resultSet.getString("task_allocator_id"),
          DateTimes.of(resultSet.getString("created_date"))
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
