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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A bridge class used when merging {@link SegmentWithOvershadowedStatus} in {@link MetadataSegmentView} and
 * {@link AvailableSegmentMetadata} in {@link DruidSchema}. This class is converted to an object array using
 * {@link #toObjectArray} to serve queries against the segments table in the system schema.
 */
public class SegmentsTableRow
{
  /**
   * Booleans constants represented as long type, where 1 = true and 0 = false or unknown.
   * This makes it easy to count number of segments which are published, available etc.
   */
  private static final long SYSTEM_SCHEMA_FALSE_OR_UNKNOWN = 0L;
  private static final long SYSTEM_SCHEMA_TRUE = 1L;

  private final DataSegment segment;
  private final long numReplicas;
  private final long numRows;
  private final long isPublished;
  private final long isAvailable;
  private final long isRealtime;
  private final long isOvershadowed;

  public static SegmentsTableRow from(AvailableSegmentMetadata availableSegment)
  {
    return new SegmentsTableRow(
        availableSegment.getSegment(),
        availableSegment.getNumReplicas(),
        availableSegment.getNumRows(),
        SYSTEM_SCHEMA_FALSE_OR_UNKNOWN,
        // is_available is assumed to be always true for segments announced by historicals or realtime tasks
        SYSTEM_SCHEMA_TRUE,
        availableSegment.isRealtime(),
        // there is an assumption here that unpublished segments are never overshadowed
        SYSTEM_SCHEMA_FALSE_OR_UNKNOWN
    );
  }

  public static SegmentsTableRow from(SegmentWithOvershadowedStatus publishedSegment)
  {
    return new SegmentsTableRow(
        publishedSegment.getDataSegment(),
        0L,
        0L,
        SYSTEM_SCHEMA_TRUE,
        SYSTEM_SCHEMA_FALSE_OR_UNKNOWN,
        SYSTEM_SCHEMA_FALSE_OR_UNKNOWN,
        publishedSegment.isOvershadowed() ? SYSTEM_SCHEMA_TRUE : SYSTEM_SCHEMA_FALSE_OR_UNKNOWN
    );
  }

  private SegmentsTableRow(
      DataSegment segment,
      long numReplicas,
      long numRows,
      long isPublished,
      long isAvailable,
      long isRealtime,
      long isOvershadowed
  )
  {
    this.segment = segment;
    this.numReplicas = numReplicas;
    this.numRows = numRows;
    this.isPublished = isPublished;
    this.isAvailable = isAvailable;
    this.isRealtime = isRealtime;
    this.isOvershadowed = isOvershadowed;
  }

  public SegmentsTableRow merge(SegmentsTableRow other)
  {
    assert this.segment.equals(other.segment);
    return new SegmentsTableRow(
        this.segment,
        // 0 means no information for these variables, so let's choose whatever the larger.
        Math.max(this.numReplicas, other.numReplicas),
        Math.max(this.numRows, other.numRows),
        Math.max(this.isPublished, other.isPublished),
        Math.max(this.isAvailable, other.isAvailable),
        Math.max(this.isRealtime, other.isRealtime),
        Math.max(this.isOvershadowed, other.isOvershadowed)
    );
  }

  public SegmentsTableRow merge(AvailableSegmentMetadata availableSegmentMetadata)
  {
    assert this.segment.equals(availableSegmentMetadata.getSegment());
    return new SegmentsTableRow(
        this.segment,
        // 0 means no information for these variables, so let's choose whatever the larger.
        Math.max(this.numReplicas, availableSegmentMetadata.getNumReplicas()),
        Math.max(this.numRows, availableSegmentMetadata.getNumRows()),
        this.isPublished,
        SYSTEM_SCHEMA_TRUE,
        Math.max(this.isRealtime, availableSegmentMetadata.isRealtime()),
        this.isOvershadowed
    );
  }

  public SegmentId getSegmentId()
  {
    return segment.getId();
  }

  /**
   * Convert this row into an object array that has {@link SystemSchema#SEGMENTS_SIGNATURE}.
   * All non-primitive type objects must be serialized into JSON strings except for timestamps.
   * Since string conversion and JSON serialization are expensive, this method accepts caches to
   * avoid the conversion or serialization of the same object.
   *
   * @param jsonMapper                 objectMapper for JSON serialization
   * @param timestampStringCache       a map of timestamp to its string representation
   * @param dimensionsStringCache      a map of dimension list to its JSON representation
   * @param metricsStringCache         a map of metric list to its JSON representation
   * @param compactionStateStringCache a map of compaction state to its JSON representation
   */
  public Object[] toObjectArray(
      ObjectMapper jsonMapper,
      ObjectStringCache<DateTime> timestampStringCache,
      ObjectStringCache<List<String>> dimensionsStringCache,
      ObjectStringCache<List<String>> metricsStringCache,
      ObjectStringCache<CompactionState> compactionStateStringCache
  )
  {
    return new Object[]{
        segment.getId(),
        segment.getDataSource(),
        timestampStringCache.add(segment.getInterval().getStart()),
        timestampStringCache.add(segment.getInterval().getEnd()),
        segment.getSize(),
        segment.getVersion(),
        (long) segment.getShardSpec().getPartitionNum(),
        numReplicas,
        numRows,
        isPublished,
        isAvailable,
        isRealtime,
        isOvershadowed,
        // we don't cache shardSpec as each segment likely has a unique shardSpec
        toJsonString(jsonMapper, segment.getShardSpec()),
        dimensionsStringCache.add(segment.getDimensions()),
        metricsStringCache.add(segment.getMetrics()),
        compactionStateStringCache.add(segment.getLastCompactionState())
    };
  }

  @Nullable
  static String toJsonString(ObjectMapper jsonMapper, @Nullable Object o)
  {
    try {
      return o == null ? null : jsonMapper.writeValueAsString(o);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
