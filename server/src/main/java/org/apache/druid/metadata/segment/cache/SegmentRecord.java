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

package org.apache.druid.metadata.segment.cache;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.sql.ResultSet;

/**
 * Represents a single record in the druid_segments table.
 */
class SegmentRecord
{
  private static final Logger log = new Logger(SegmentRecord.class);

  private final SegmentId segmentId;
  private final boolean isUsed;
  private final DateTime lastUpdatedTime;

  SegmentRecord(SegmentId segmentId, boolean isUsed, DateTime lastUpdatedTime)
  {
    this.segmentId = segmentId;
    this.isUsed = isUsed;
    this.lastUpdatedTime = lastUpdatedTime;
  }

  public SegmentId getSegmentId()
  {
    return segmentId;
  }

  public boolean isUsed()
  {
    return isUsed;
  }

  public DateTime getLastUpdatedTime()
  {
    return lastUpdatedTime;
  }

  /**
   * Creates a SegmentRecord from the given result set.
   *
   * @return null if an error occurred while reading the record.
   */
  @Nullable
  static SegmentRecord fromResultSet(ResultSet r)
  {
    String serializedId = null;
    String dataSource = null;
    try {
      serializedId = r.getString("id");
      dataSource = r.getString("dataSource");

      final DateTime lastUpdatedTime = SqlSegmentsMetadataQuery.nullAndEmptySafeDate(r.getString(
          "used_status_last_updated"));

      final SegmentId segmentId = SegmentId.tryParse(dataSource, serializedId);
      if (segmentId == null) {
        log.error("Could not parse Segment ID[%s] of datasource[%s]", serializedId, dataSource);
        return null;
      } else {
        return new SegmentRecord(segmentId, true, lastUpdatedTime);
      }
    }
    catch (Exception e) {
      log.error(
          e,
          "Error occurred while reading Segment ID[%s] of datasource[%s]",
          serializedId, dataSource
      );
      return null;
    }
  }
}
