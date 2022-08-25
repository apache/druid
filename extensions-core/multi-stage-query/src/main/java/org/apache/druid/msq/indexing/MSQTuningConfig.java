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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.partitions.PartitionsSpec;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Tuning parameters for multi-stage query tasks: {@link MSQControllerTask} and {@link MSQWorkerTask}.
 */
public class MSQTuningConfig
{
  /**
   * Lower than {@link org.apache.druid.segment.indexing.TuningConfig#DEFAULT_MAX_ROWS_IN_MEMORY} to minimize the
   * impact of per-row overheads that are not accounted for by OnheapIncrementalIndex. For example: overheads
   * related to creating bitmaps during persist.
   *
   * If this value proves to work well, we'll update {@link org.apache.druid.segment.indexing.TuningConfig} to bring
   * the two values in line with each other.
   */
  private static final int DEFAULT_MAX_ROWS_IN_MEMORY = 100000;

  /**
   * One worker task.
   */
  private static final int DEFAULT_MAX_NUM_TASKS = 1;

  @Nullable
  private final Integer maxNumWorkers;

  @Nullable
  private final Integer maxRowsInMemory;

  @Nullable
  private final Integer rowsPerSegment;

  public MSQTuningConfig(
      @JsonProperty("maxNumWorkers") @Nullable final Integer maxNumWorkers,
      @JsonProperty("maxRowsInMemory") @Nullable final Integer maxRowsInMemory,
      @JsonProperty("rowsPerSegment") @Nullable final Integer rowsPerSegment
  )
  {
    this.maxNumWorkers = maxNumWorkers;
    this.maxRowsInMemory = maxRowsInMemory;
    this.rowsPerSegment = rowsPerSegment;
  }

  public static MSQTuningConfig defaultConfig()
  {
    return new MSQTuningConfig(null, null, null);
  }

  @JsonProperty("maxNumWorkers")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer getMaxNumWorkersForSerialization()
  {
    return maxNumWorkers;
  }

  @JsonProperty("maxRowsInMemory")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer getMaxRowsInMemoryForSerialization()
  {
    return maxRowsInMemory;
  }

  @JsonProperty("rowsPerSegment")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer getRowsPerSegmentForSerialization()
  {
    return rowsPerSegment;
  }

  public int getMaxNumWorkers()
  {
    return maxNumWorkers != null ? maxNumWorkers : DEFAULT_MAX_NUM_TASKS;
  }

  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory != null ? maxRowsInMemory : DEFAULT_MAX_ROWS_IN_MEMORY;
  }

  public int getRowsPerSegment()
  {
    return rowsPerSegment != null ? rowsPerSegment : PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT;
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
    MSQTuningConfig that = (MSQTuningConfig) o;
    return Objects.equals(maxNumWorkers, that.maxNumWorkers)
           && Objects.equals(maxRowsInMemory, that.maxRowsInMemory)
           && Objects.equals(rowsPerSegment, that.rowsPerSegment);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxNumWorkers, maxRowsInMemory, rowsPerSegment);
  }

  @Override
  public String toString()
  {
    return "MSQTuningConfig{" +
           "maxNumWorkers=" + maxNumWorkers +
           ", maxRowsInMemory=" + maxRowsInMemory +
           ", rowsPerSegment=" + rowsPerSegment +
           '}';
  }
}
