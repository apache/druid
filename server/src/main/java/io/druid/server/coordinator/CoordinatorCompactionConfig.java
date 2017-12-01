/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.client.indexing.ClientCompactQueryTuningConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class CoordinatorCompactionConfig
{
  // should be synchronized with Tasks.DEFAULT_MERGE_TASK_PRIORITY
  private static final int DEFAULT_COMPACTION_TASK_PRIORITY = 25;
  private static final long DEFAULT_TARGET_COMPACTION_SIZE_BYTES = 800 * 1024 * 1024; // 800MB
  private static final int DEFAULT_NUM_TARGET_COMPACTION_SEGMENTS = 150;
  private static final Period DEFAULT_SKIP_OFFSET_FROM_LATEST = new Period("P1D");

  private final String dataSource;
  private final int taskPriority;
  private final long targetCompactionSizeBytes;
  // The number of compaction segments is limited because the byte size of a serialized task spec is limited by
  // RemoteTaskRunnerConfig.maxZnodeBytes.
  private final int numTargetCompactionSegments;
  private final Period skipOffsetFromLatest;
  private final ClientCompactQueryTuningConfig tuningConfig;
  private final Map<String, Object> taskContext;

  @JsonCreator
  public CoordinatorCompactionConfig(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskPriority") Integer taskPriority,
      @JsonProperty("targetCompactionSizeBytes") Long targetCompactionSizeBytes,
      @JsonProperty("numTargetCompactionSegments") Integer numTargetCompactionSegments,
      @JsonProperty("skipOffsetFromLatest") Period skipOffsetFromLatest,
      @JsonProperty("tuningConfig") ClientCompactQueryTuningConfig tuningConfig,
      @JsonProperty("taskContext") Map<String, Object> taskContext
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.taskPriority = taskPriority == null ?
                        DEFAULT_COMPACTION_TASK_PRIORITY :
                        taskPriority;
    this.targetCompactionSizeBytes = targetCompactionSizeBytes == null ?
                                     DEFAULT_TARGET_COMPACTION_SIZE_BYTES :
                                     targetCompactionSizeBytes;
    this.numTargetCompactionSegments = numTargetCompactionSegments == null ?
                                       DEFAULT_NUM_TARGET_COMPACTION_SEGMENTS :
                                       numTargetCompactionSegments;
    this.skipOffsetFromLatest = skipOffsetFromLatest == null ? DEFAULT_SKIP_OFFSET_FROM_LATEST : skipOffsetFromLatest;
    this.tuningConfig = tuningConfig;
    this.taskContext = taskContext;

    Preconditions.checkArgument(
        this.numTargetCompactionSegments > 1,
        "numTargetCompactionSegments should be larger than 1"
    );
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
  public long getTargetCompactionSizeBytes()
  {
    return targetCompactionSizeBytes;
  }

  @JsonProperty
  public int getNumTargetCompactionSegments()
  {
    return numTargetCompactionSegments;
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

    final CoordinatorCompactionConfig that = (CoordinatorCompactionConfig) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }

    if (taskPriority != that.taskPriority) {
      return false;
    }

    if (targetCompactionSizeBytes != that.targetCompactionSizeBytes) {
      return false;
    }

    if (numTargetCompactionSegments != that.numTargetCompactionSegments) {
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
        taskPriority,
        targetCompactionSizeBytes,
        numTargetCompactionSegments,
        skipOffsetFromLatest,
        tuningConfig,
        taskContext
    );
  }
}
