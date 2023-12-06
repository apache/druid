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

package org.apache.druid.indexing.common.config;

import org.apache.druid.segment.loading.StorageLocationConfig;
import org.joda.time.Period;

import java.util.List;

public class TaskConfigBuilder
{
  private String baseDir;
  private String baseTaskDir;
  private String hadoopWorkingPath;
  private Integer defaultRowFlushBoundary;
  private List<String> defaultHadoopCoordinates;
  private boolean restoreTasksOnRestart;
  private Period gracefulShutdownTimeout;
  private Period directoryLockTimeout;
  private List<StorageLocationConfig> shuffleDataLocations;
  private boolean ignoreTimestampSpecForDruidInputSource;
  private boolean batchMemoryMappedIndex; // deprecated; only set to true to fall back to older behavior
  private String batchProcessingMode;
  private Boolean storeEmptyColumns;
  private boolean enableTaskLevelLogPush;
  private Long tmpStorageBytesPerTask;
  private Boolean enableConcurrentAppendAndReplace;

  public TaskConfigBuilder setBaseDir(String baseDir)
  {
    this.baseDir = baseDir;
    return this;
  }

  public TaskConfigBuilder setBaseTaskDir(String baseTaskDir)
  {
    this.baseTaskDir = baseTaskDir;
    return this;
  }

  public TaskConfigBuilder setHadoopWorkingPath(String hadoopWorkingPath)
  {
    this.hadoopWorkingPath = hadoopWorkingPath;
    return this;
  }

  public TaskConfigBuilder setDefaultRowFlushBoundary(Integer defaultRowFlushBoundary)
  {
    this.defaultRowFlushBoundary = defaultRowFlushBoundary;
    return this;
  }

  public TaskConfigBuilder setDefaultHadoopCoordinates(List<String> defaultHadoopCoordinates)
  {
    this.defaultHadoopCoordinates = defaultHadoopCoordinates;
    return this;
  }

  public TaskConfigBuilder setRestoreTasksOnRestart(boolean restoreTasksOnRestart)
  {
    this.restoreTasksOnRestart = restoreTasksOnRestart;
    return this;
  }

  public TaskConfigBuilder setGracefulShutdownTimeout(Period gracefulShutdownTimeout)
  {
    this.gracefulShutdownTimeout = gracefulShutdownTimeout;
    return this;
  }

  public TaskConfigBuilder setDirectoryLockTimeout(Period directoryLockTimeout)
  {
    this.directoryLockTimeout = directoryLockTimeout;
    return this;
  }

  public TaskConfigBuilder setShuffleDataLocations(List<StorageLocationConfig> shuffleDataLocations)
  {
    this.shuffleDataLocations = shuffleDataLocations;
    return this;
  }

  public TaskConfigBuilder setIgnoreTimestampSpecForDruidInputSource(boolean ignoreTimestampSpecForDruidInputSource)
  {
    this.ignoreTimestampSpecForDruidInputSource = ignoreTimestampSpecForDruidInputSource;
    return this;
  }

  public TaskConfigBuilder setBatchMemoryMappedIndex(boolean batchMemoryMappedIndex)
  {
    this.batchMemoryMappedIndex = batchMemoryMappedIndex;
    return this;
  }

  public TaskConfigBuilder setBatchProcessingMode(String batchProcessingMode)
  {
    this.batchProcessingMode = batchProcessingMode;
    return this;
  }

  public TaskConfigBuilder setStoreEmptyColumns(Boolean storeEmptyColumns)
  {
    this.storeEmptyColumns = storeEmptyColumns;
    return this;
  }

  public TaskConfigBuilder setEnableTaskLevelLogPush(boolean enableTaskLevelLogPush)
  {
    this.enableTaskLevelLogPush = enableTaskLevelLogPush;
    return this;
  }

  public TaskConfigBuilder setTmpStorageBytesPerTask(Long tmpStorageBytesPerTask)
  {
    this.tmpStorageBytesPerTask = tmpStorageBytesPerTask;
    return this;
  }

  public TaskConfigBuilder enableConcurrentAppendAndReplace()
  {
    this.enableConcurrentAppendAndReplace = true;
    return this;
  }

  public TaskConfigBuilder disableConcurrentAppendAndReplace()
  {
    this.enableConcurrentAppendAndReplace = false;
    return this;
  }

  public TaskConfig build()
  {
    return new TaskConfig(
        baseDir,
        baseTaskDir,
        hadoopWorkingPath,
        defaultRowFlushBoundary,
        defaultHadoopCoordinates,
        restoreTasksOnRestart,
        gracefulShutdownTimeout,
        directoryLockTimeout,
        shuffleDataLocations,
        ignoreTimestampSpecForDruidInputSource,
        batchMemoryMappedIndex,
        batchProcessingMode,
        storeEmptyColumns,
        enableTaskLevelLogPush,
        tmpStorageBytesPerTask,
        enableConcurrentAppendAndReplace
    );
  }
}
