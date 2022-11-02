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

package org.apache.druid.java.util.emitter.service;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

/**
 * The event that gets generated whenever a segment is committed
 */
public class SegmentMetadataEvent implements Event
{
  public static final String FEED = "feed";
  public static final String DATASOURCE = "dataSource";
  public static final String CREATED_TIME = "createdTime";
  public static final String START_TIME = "startTime";
  public static final String END_TIME = "endTime";
  public static final String VERSION = "version";
  public static final String IS_COMPACTED = "isCompacted";

  /**
   * Time at which the segment metadata event is created
   */
  private final DateTime createdTime;
  /**
   * Datasource for which the segment is committed
   */
  private final String dataSource;
  /**
   * Start interval of the committed segment
   */
  private final DateTime startTime;
  /**
   * End interval of the committed segment
   */
  private final DateTime endTime;
  /**
   * Version of the committed segment
   */
  private final String version;
  /**
   * Is the segment, a compacted segment or not
   */
  private final boolean isCompacted;

  public static SegmentMetadataEvent create(DataSegment segment, DateTime eventTime)
  {
    return new SegmentMetadataEvent(
        segment.getDataSource(),
        eventTime,
        segment.getInterval().getStart(),
        segment.getInterval().getEnd(),
        segment.getVersion(),
        segment.getLastCompactionState() != null
    );
  }

  public SegmentMetadataEvent(
      String dataSource,
      DateTime createdTime,
      DateTime startTime,
      DateTime endTime,
      String version,
      boolean isCompacted
  )
  {
    this.dataSource = dataSource;
    this.createdTime = createdTime;
    this.startTime = startTime;
    this.endTime = endTime;
    this.version = version;
    this.isCompacted = isCompacted;
  }

  @Override
  public String getFeed()
  {
    return "segment_metadata";
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public DateTime getStartTime()
  {
    return startTime;
  }

  public DateTime getEndTime()
  {
    return endTime;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public String getVersion()
  {
    return version;
  }

  public boolean isCompacted()
  {
    return isCompacted;
  }

  @Override
  @JsonValue
  public EventMap toMap()
  {

    return EventMap.builder()
        .put(FEED, getFeed())
        .put(DATASOURCE, dataSource)
        .put(CREATED_TIME, createdTime)
        .put(START_TIME, startTime)
        .put(END_TIME, endTime)
        .put(VERSION, version)
        .put(IS_COMPACTED, isCompacted)
        .build();
  }
}
