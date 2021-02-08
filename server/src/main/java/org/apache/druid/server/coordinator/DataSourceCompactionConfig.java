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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class DataSourceCompactionConfig
{
  /** Must be synced with Tasks.DEFAULT_MERGE_TASK_PRIORITY */
  public static final int DEFAULT_COMPACTION_TASK_PRIORITY = 25;
  private static final long DEFAULT_INPUT_SEGMENT_SIZE_BYTES = 400 * 1024 * 1024;
  private static final Period DEFAULT_SKIP_OFFSET_FROM_LATEST = new Period("P1D");

  // Interval score computed by recentDaysForTimeWeight,timeWeight,numWeight,sizeWeight
  // Time weight{timeWeight} vs others{numWeight+sizeWeight} is 1:1
  // Recent days{recentDaysForTimeWeight=1} occupy time weight{timeWeight=1.0f} from old to newest is (0f-1.0f)
  public static final Period DEFAULT_COMPACTION_RECENT_DAYS = new Period("P5D");
  public static final float DEFAULT_COMPACTION_TIME_WEIGHT_FOR_RECENT_DAYS = 1.0f;
  public static final float DEFAULT_COMPACTION_SEGMENT_NUM_WEIGHT = 0.6f;

  private final String dataSource;
  private final int taskPriority;
  private final long inputSegmentSizeBytes;
  private final long minInputSegmentNum;
  private final Period minorCompactRecentDays;
  /**
   * The number of input segments is limited because the byte size of a serialized task spec is limited by
   * org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig.maxZnodeBytes.
   */
  @Nullable
  private final Integer maxRowsPerSegment;
  private final Period skipOffsetFromLatest;
  private final Boolean enableFilterLockedInterval;
  private final UserCompactionTaskQueryTuningConfig tuningConfig;
  private final Map<String, Object> taskContext;

  @JsonCreator
  public DataSourceCompactionConfig(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("minInputSegmentNum") @Nullable Long minInputSegmentNum,
      @JsonProperty("minorCompactRecentDays") @Nullable Period minorCompactRecentDays,
      @JsonProperty("maxRowsPerSegment") @Deprecated @Nullable Integer maxRowsPerSegment,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("enableFilterLockedInterval") @Nullable Boolean enableFilterLockedInterval,
      @JsonProperty("tuningConfig") @Nullable UserCompactionTaskQueryTuningConfig tuningConfig,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.taskPriority = taskPriority == null
                        ? DEFAULT_COMPACTION_TASK_PRIORITY
                        : taskPriority;
    this.inputSegmentSizeBytes = inputSegmentSizeBytes == null
                                 ? DEFAULT_INPUT_SEGMENT_SIZE_BYTES
                                 : inputSegmentSizeBytes;
    this.maxRowsPerSegment = maxRowsPerSegment;
    this.skipOffsetFromLatest = skipOffsetFromLatest == null ? DEFAULT_SKIP_OFFSET_FROM_LATEST : skipOffsetFromLatest;
    this.enableFilterLockedInterval = enableFilterLockedInterval == null ? false : enableFilterLockedInterval;
    this.tuningConfig = tuningConfig;
    this.taskContext = taskContext;
    this.minInputSegmentNum = minInputSegmentNum == null ? 1 : minInputSegmentNum;
    this.minorCompactRecentDays = minorCompactRecentDays == null
                                  ? DEFAULT_COMPACTION_RECENT_DAYS
                                  : minorCompactRecentDays;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public int getTaskPriority()
  {
    return taskPriority;
  }

  @JsonProperty
  public long getInputSegmentSizeBytes()
  {
    return inputSegmentSizeBytes;
  }

  @Deprecated
  @JsonProperty
  @Nullable
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  public Period getSkipOffsetFromLatest()
  {
    return skipOffsetFromLatest;
  }

  @JsonProperty
  public Period getMinorCompactRecentDays()
  {
    return minorCompactRecentDays;
  }

  @JsonProperty
  public long getMinInputSegmentNum()
  {
    return minInputSegmentNum;
  }

  @JsonProperty
  public Boolean getEnableFilterLockedInterval()
  {
    return enableFilterLockedInterval;
  }

  @JsonProperty
  @Nullable
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  @Nullable
  public Map<String, Object> getTaskContext()
  {
    return taskContext;
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
    DataSourceCompactionConfig that = (DataSourceCompactionConfig) o;
    return taskPriority == that.taskPriority &&
           inputSegmentSizeBytes == that.inputSegmentSizeBytes &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(skipOffsetFromLatest, that.skipOffsetFromLatest) &&
           Objects.equals(enableFilterLockedInterval, that.enableFilterLockedInterval) &&
           Objects.equals(tuningConfig, that.tuningConfig) &&
           Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSource,
        taskPriority,
        inputSegmentSizeBytes,
        skipOffsetFromLatest,
        enableFilterLockedInterval,
        tuningConfig,
        taskContext
    );
  }
}
