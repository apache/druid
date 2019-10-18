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

package org.apache.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Dynamically determine partitions in the middle of indexing.
 */
public class DynamicPartitionsSpec implements PartitionsSpec
{
  /**
   * Default maxTotalRows for most task types except compaction task.
   */
  public static final long DEFAULT_MAX_TOTAL_ROWS = 20_000_000;
  static final String NAME = "dynamic";

  private final int maxRowsPerSegment;
  @Nullable
  private final Long maxTotalRows;

  @JsonCreator
  public DynamicPartitionsSpec(
      @JsonProperty(PartitionsSpec.MAX_ROWS_PER_SEGMENT) @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows
  )
  {
    this.maxRowsPerSegment = PartitionsSpec.isEffectivelyNull(maxRowsPerSegment)
                             ? DEFAULT_MAX_ROWS_PER_SEGMENT
                             : maxRowsPerSegment;
    this.maxTotalRows = maxTotalRows;
  }

  @Override
  @JsonProperty
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @Nullable
  @JsonProperty
  public Long getMaxTotalRows()
  {
    return maxTotalRows;
  }

  /**
   * Get the given maxTotalRows or the default.
   * The default can be different depending on the caller.
   */
  public long getMaxTotalRowsOr(long defaultMaxTotalRows)
  {
    return PartitionsSpec.isEffectivelyNull(maxTotalRows) ? defaultMaxTotalRows : maxTotalRows;
  }

  @Override
  public boolean needsDeterminePartitions(boolean useForHadoopTask)
  {
    return false;
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
    DynamicPartitionsSpec that = (DynamicPartitionsSpec) o;
    return maxRowsPerSegment == that.maxRowsPerSegment &&
           Objects.equals(maxTotalRows, that.maxTotalRows);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxRowsPerSegment, maxTotalRows);
  }

  @Override
  public String toString()
  {
    return "DynamicPartitionsSpec{" +
           "maxRowsPerSegment=" + maxRowsPerSegment +
           ", maxTotalRows=" + maxTotalRows +
           '}';
  }
}
