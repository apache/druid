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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;

/**
 * Input specification for compaction task.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = CompactionIntervalSpec.TYPE, value = CompactionIntervalSpec.class),
    @Type(name = SpecificSegmentsSpec.TYPE, value = SpecificSegmentsSpec.class)
})
public interface CompactionInputSpec
{
  /**
   * Find the umbrella interval containing the specified input.
   */
  Interval findInterval(String dataSource);

  /**
   * Validate the specified input against the most recent published segments.
   * This method is used to check whether the specified input has gone stale.
   *
   * @param lockGranularityInUse {@link LockGranularity} in use
   * @param latestSegments       most recent published segments in the interval returned by {@link #findInterval}
   */
  boolean validateSegments(LockGranularity lockGranularityInUse, List<DataSegment> latestSegments);
}
