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

import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Associates a time interval with its segment granularity and optional source rule.
 * Used to pass synthetic timeline information from timeline generation to config building.
 */
public class IntervalGranularityInfo
{
  private final Interval interval;
  private final Granularity granularity;
  private final ReindexingSegmentGranularityRule sourceRule;

  public IntervalGranularityInfo(
      Interval interval,
      Granularity granularity,
      @Nullable ReindexingSegmentGranularityRule sourceRule
  )
  {
    this.interval = interval;
    this.granularity = granularity;
    this.sourceRule = sourceRule;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public Granularity getGranularity()
  {
    return granularity;
  }

  @Nullable
  public ReindexingSegmentGranularityRule getSourceRule()
  {
    return sourceRule;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IntervalGranularityInfo that = (IntervalGranularityInfo) o;
    return Objects.equals(interval, that.interval)
           && Objects.equals(granularity, that.granularity)
           && Objects.equals(sourceRule, that.sourceRule);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, granularity, sourceRule);
  }

  @Override
  public String toString()
  {
    return "IntervalGranularityInfo{"
           + "interval=" + interval
           + ", granularity=" + granularity
           + ", sourceRule=" + (sourceRule != null ? sourceRule.getId() : "null")
           + '}';
  }
}
