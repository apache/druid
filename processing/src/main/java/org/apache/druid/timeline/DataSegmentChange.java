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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.StringUtils;

/**
 * Wrapper over segment to include change in segment lifecycle.
 */
public class DataSegmentChange
{
  private final SegmentStatusInCluster segmentStatusInCluster;
  private final SegmentLifecycleChangeType segmentLifecycleChangeType;

  @JsonCreator
  public DataSegmentChange(
      @JsonProperty("segmentStatusInCluster") SegmentStatusInCluster segmentStatusInCluster,
      @JsonProperty("changeType") SegmentLifecycleChangeType segmentLifecycleChangeType
  )
  {
    this.segmentStatusInCluster = segmentStatusInCluster;
    this.segmentLifecycleChangeType = segmentLifecycleChangeType;
  }

  @JsonProperty
  public SegmentStatusInCluster getSegmentStatusInCluster()
  {
    return segmentStatusInCluster;
  }

  @JsonProperty
  public SegmentLifecycleChangeType getChangeType()
  {
    return segmentLifecycleChangeType;
  }

  @JsonIgnore
  public boolean isRemoved()
  {
    return segmentLifecycleChangeType == SegmentLifecycleChangeType.SEGMENT_REMOVED;
  }

  @Override
  public String toString()
  {
    return "DataSegmentChangeRequest{" +
           ", changeReason=" + segmentLifecycleChangeType +
           ", segmentStatusInCluster=" + segmentStatusInCluster +
           '}';
  }

  /**
   * Enum to represent change in lifecycle of a segment in the system
   */
  public enum SegmentLifecycleChangeType
  {
    /**
     * Segment has been added in the system
     */
    SEGMENT_ADDED,

    /**
     * Segment has been removed from the system
     */
    SEGMENT_REMOVED,

    /**
     * Segment has been overshadowed
     */
    SEGMENT_OVERSHADOWED,

    /**
     * Segment has been loaded by historical
     */
    SEGMENT_HAS_LOADED,

    /**
     * Segment is both loaded by historical and overshadowed
     */
    SEGMENT_OVERSHADOWED_AND_HAS_LOADED;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static SegmentLifecycleChangeType fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }
}
