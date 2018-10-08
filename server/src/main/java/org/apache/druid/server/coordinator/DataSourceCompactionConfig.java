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
import org.apache.druid.client.indexing.ClientCompactQueryTuningConfig;
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
  private final long targetCompactionSizeBytes;
  // The number of input segments is limited because the byte size of a serialized task spec is limited by
  // RemoteTaskRunnerConfig.maxZnodeBytes.
  private final int maxNumSegmentsToCompact;
  private final Period skipOffsetFromLatest;
  private final ClientCompactQueryTuningConfig tuningConfig;
  private final Map<String, Object> taskContext;

  @JsonCreator
  public DataSourceCompactionConfig(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("keepSegmentGranularity") Boolean keepSegmentGranularity,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("targetCompactionSizeBytes") @Nullable Long targetCompactionSizeBytes,
      @JsonProperty("maxNumSegmentsToCompact") @Nullable Integer maxNumSegmentsToCompact,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("tuningConfig") @Nullable ClientCompactQueryTuningConfig tuningConfig,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext
  )
  {
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
    this.targetCompactionSizeBytes = targetCompactionSizeBytes == null
                                     ? DEFAULT_TARGET_COMPACTION_SIZE_BYTES
                                     : targetCompactionSizeBytes;
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
  public long getTargetCompactionSizeBytes()
  {
    return targetCompactionSizeBytes;
  }

  @JsonProperty
  public Period getSkipOffsetFromLatest()
  {
    return skipOffsetFromLatest;
  }

  @JsonProperty
  @Nullable
  public ClientCompactQueryTuningConfig getTuningConfig()
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
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DataSourceCompactionConfig that = (DataSourceCompactionConfig) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }

    if (keepSegmentGranularity != that.keepSegmentGranularity) {
      return false;
    }

    if (taskPriority != that.taskPriority) {
      return false;
    }

    if (inputSegmentSizeBytes != that.inputSegmentSizeBytes) {
      return false;
    }

    if (maxNumSegmentsToCompact != that.maxNumSegmentsToCompact) {
      return false;
    }

    if (targetCompactionSizeBytes != that.targetCompactionSizeBytes) {
      return false;
    }

    if (!skipOffsetFromLatest.equals(that.skipOffsetFromLatest)) {
      return false;
    }

    if (!Objects.equals(tuningConfig, that.tuningConfig)) {
      return false;
    }

    return Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSource,
        keepSegmentGranularity,
        taskPriority,
        inputSegmentSizeBytes,
        maxNumSegmentsToCompact,
        targetCompactionSizeBytes,
        skipOffsetFromLatest,
        tuningConfig,
        taskContext
    );
  }
}
