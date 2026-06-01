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

import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Associates a time interval with its partitioning information.
 * Used to pass synthetic timeline information from timeline generation to config building.
 */
public class IntervalPartitioningInfo
{
  private final Interval interval;
  private final ReindexingPartitioningRule sourceRule;
  // Whether this info was generated from a synthetic rule (i.e. not directly from a user-defined rule).
  private final boolean isRuleSynthetic;

  public IntervalPartitioningInfo(
      Interval interval,
      ReindexingPartitioningRule sourceRule
  )
  {
    this(interval, sourceRule, false);
  }

  public IntervalPartitioningInfo(
      Interval interval,
      ReindexingPartitioningRule sourceRule,
      boolean isRuleSynthetic
  )
  {
    this.interval = interval;
    InvalidInput.conditionalException(sourceRule != null, "sourceRule cannot be null");
    this.sourceRule = sourceRule;
    this.isRuleSynthetic = isRuleSynthetic;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public Granularity getGranularity()
  {
    return sourceRule.getSegmentGranularity();
  }

  public boolean isRuleSynthetic()
  {
    return isRuleSynthetic;
  }

  public ReindexingPartitioningRule getSourceRule()
  {
    return sourceRule;
  }

  public PartitionsSpec getPartitionsSpec()
  {
    return sourceRule.getPartitionsSpec();
  }

  @Nullable
  public VirtualColumns getVirtualColumns()
  {
    return sourceRule.getVirtualColumns();
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
    IntervalPartitioningInfo that = (IntervalPartitioningInfo) o;
    return Objects.equals(interval, that.interval)
           && isRuleSynthetic == that.isRuleSynthetic
           && Objects.equals(sourceRule, that.sourceRule);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, sourceRule, isRuleSynthetic);
  }

  @Override
  public String toString()
  {
    return "IntervalPartitioningInfo{"
           + "interval=" + interval
           + ", isRuleSynthetic=" + isRuleSynthetic
           + ", sourceRule=" + (sourceRule != null ? sourceRule.getId() : "null")
           + '}';
  }
}
