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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies a segment granularity for reindexing tasks to configure.
 * <p>
 * This rule controls how time-series data is bucketed into segments during reindexing. For example, changing from
 * 15-minute segments to hourly segments reduces segment count.
 * <p>
 * This is a non-additive rule. Multiple segment granularity rules cannot be applied to the same segment.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *     "id": "daily-30d",
 *     "olderThan": "P30D",
 *     "segmentGranularity": "DAY"
 *     "description": "Compact to daily segments for data older than 30 days"
 * }
 * }</pre>
 */
public class ReindexingSegmentGranularityRule extends AbstractReindexingRule
{
  private final Granularity segmentGranularity;

  @JsonCreator
  public ReindexingSegmentGranularityRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("segmentGranularity") @Nonnull Granularity segmentGranularity
  )
  {
    super(id, description, olderThan);
    this.segmentGranularity = Objects.requireNonNull(segmentGranularity);
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

}
