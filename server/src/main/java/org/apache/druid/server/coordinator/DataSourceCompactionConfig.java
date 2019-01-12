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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.client.indexing.ClientCompactQueryTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class DataSourceCompactionConfig
{
  public static final long DEFAULT_TARGET_COMPACTION_SIZE_BYTES = 400 * 1024 * 1024; // 400MB

  // should be synchronized with Tasks.DEFAULT_MERGE_TASK_PRIORITY
  private static final int DEFAULT_COMPACTION_TASK_PRIORITY = 25;
  private static final boolean DEFAULT_KEEP_SEGMENT_GRANULARITY = true;
  private static final long DEFAULT_INPUT_SEGMENT_SIZE_BYTES = 400 * 1024 * 1024;
  private static final int DEFAULT_NUM_INPUT_SEGMENTS = 150;
  private static final Period DEFAULT_SKIP_OFFSET_FROM_LATEST = new Period("P1D");

  private final String dataSource;
  private final boolean keepSegmentGranularity;
  private final int taskPriority;
  private final long inputSegmentSizeBytes;
  @Nullable
  private final Long targetCompactionSizeBytes;
  // The number of input segments is limited because the byte size of a serialized task spec is limited by
  // RemoteTaskRunnerConfig.maxZnodeBytes.
  @Nullable
  private final Integer maxRowsPerSegment;
  private final int maxNumSegmentsToCompact;
  private final Period skipOffsetFromLatest;
  private final UserCompactTuningConfig tuningConfig;
  private final Map<String, Object> taskContext;

  @JsonCreator
  public DataSourceCompactionConfig(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("keepSegmentGranularity") @Nullable Boolean keepSegmentGranularity,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("targetCompactionSizeBytes") @Nullable Long targetCompactionSizeBytes,
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxNumSegmentsToCompact") @Nullable Integer maxNumSegmentsToCompact,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("tuningConfig") @Nullable UserCompactTuningConfig tuningConfig,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext
  )
  {
    Preconditions.checkArgument(
        targetCompactionSizeBytes == null || maxRowsPerSegment == null,
        "targetCompactionSizeBytes and maxRowsPerSegment in tuningConfig can't be used together"
    );
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.keepSegmentGranularity = keepSegmentGranularity == null
                                  ? DEFAULT_KEEP_SEGMENT_GRANULARITY
                                  : keepSegmentGranularity;
    this.taskPriority = taskPriority == null
                        ? DEFAULT_COMPACTION_TASK_PRIORITY
                        : taskPriority;
    this.inputSegmentSizeBytes = inputSegmentSizeBytes == null
                                 ? DEFAULT_INPUT_SEGMENT_SIZE_BYTES
                                 : inputSegmentSizeBytes;
    if (targetCompactionSizeBytes == null && maxRowsPerSegment == null) {
      this.targetCompactionSizeBytes = DEFAULT_TARGET_COMPACTION_SIZE_BYTES;
    } else {
      this.targetCompactionSizeBytes = targetCompactionSizeBytes;
    }
    this.maxRowsPerSegment = maxRowsPerSegment;
    this.maxNumSegmentsToCompact = maxNumSegmentsToCompact == null
                                   ? DEFAULT_NUM_INPUT_SEGMENTS
                                   : maxNumSegmentsToCompact;
    this.skipOffsetFromLatest = skipOffsetFromLatest == null ? DEFAULT_SKIP_OFFSET_FROM_LATEST : skipOffsetFromLatest;
    this.tuningConfig = tuningConfig;
    this.taskContext = taskContext;

    Preconditions.checkArgument(
        this.maxNumSegmentsToCompact > 1,
        "numTargetCompactionSegments should be larger than 1"
    );
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public boolean isKeepSegmentGranularity()
  {
    return keepSegmentGranularity;
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

  @JsonProperty
  public int getMaxNumSegmentsToCompact()
  {
    return maxNumSegmentsToCompact;
  }

  @JsonProperty
  @Nullable
  public Long getTargetCompactionSizeBytes()
  {
    return targetCompactionSizeBytes;
  }

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
  @Nullable
  public UserCompactTuningConfig getTuningConfig()
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
    return keepSegmentGranularity == that.keepSegmentGranularity &&
           taskPriority == that.taskPriority &&
           inputSegmentSizeBytes == that.inputSegmentSizeBytes &&
           maxNumSegmentsToCompact == that.maxNumSegmentsToCompact &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(targetCompactionSizeBytes, that.targetCompactionSizeBytes) &&
           Objects.equals(skipOffsetFromLatest, that.skipOffsetFromLatest) &&
           Objects.equals(tuningConfig, that.tuningConfig) &&
           Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSource,
        keepSegmentGranularity,
        taskPriority,
        inputSegmentSizeBytes,
        targetCompactionSizeBytes,
        maxNumSegmentsToCompact,
        skipOffsetFromLatest,
        tuningConfig,
        taskContext
    );
  }

  public static class UserCompactTuningConfig extends ClientCompactQueryTuningConfig
  {
    @JsonCreator
    public UserCompactTuningConfig(
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxTotalRows") @Nullable Integer maxTotalRows,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        @JsonProperty("pushTimeout") @Nullable Long pushTimeout
    )
    {
      super(null, maxRowsInMemory, maxTotalRows, indexSpec, maxPendingPersists, pushTimeout);
    }

    @Override
    @Nullable
    @JsonIgnore
    public Integer getMaxRowsPerSegment()
    {
      throw new UnsupportedOperationException();
    }
  }
}
