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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * Configurations for ingestion tasks. These configurations can be applied per middleManager, indexer, or overlord.
 *
 * See {@link org.apache.druid.indexing.overlord.config.DefaultTaskConfig} if you want to apply the same configuration
 * to all tasks submitted to the overlord.
 */
public class TaskConfig
{
  private static final Logger log = new Logger(TaskConfig.class);
  private static final String HADOOP_LIB_VERSIONS = "hadoop.indexer.libs.version";
  public static final List<String> DEFAULT_DEFAULT_HADOOP_COORDINATES;

  static {
    try {
      DEFAULT_DEFAULT_HADOOP_COORDINATES =
          ImmutableList.copyOf(Lists.newArrayList(IOUtils.toString(
              TaskConfig.class.getResourceAsStream("/"
                                                   + HADOOP_LIB_VERSIONS),
              StandardCharsets.UTF_8
          ).split(",")));

    }
    catch (Exception e) {
      throw new ISE(e, "Unable to read file %s from classpath ", HADOOP_LIB_VERSIONS);
    }
  }

  // This enum controls processing mode of batch ingestion "segment creation" phase (i.e. appenderator logic)
  public enum BatchProcessingMode
  {
    OPEN_SEGMENTS, /* mmap segments, legacy code */
    CLOSED_SEGMENTS, /* Do not mmap segments but keep most other legacy code */
    CLOSED_SEGMENTS_SINKS /* Most aggressive memory optimization, do not mmap segments and eliminate sinks, etc. */
  }

  public static final BatchProcessingMode BATCH_PROCESSING_MODE_DEFAULT = BatchProcessingMode.CLOSED_SEGMENTS;

  private static final Period DEFAULT_DIRECTORY_LOCK_TIMEOUT = new Period("PT10M");
  private static final Period DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT = new Period("PT5M");
  private static final boolean DEFAULT_STORE_EMPTY_COLUMNS = true;

  @JsonProperty
  private final String baseDir;

  @JsonProperty
  private final File baseTaskDir;

  @JsonProperty
  private final String hadoopWorkingPath;

  @JsonProperty
  private final int defaultRowFlushBoundary;

  @JsonProperty
  private final List<String> defaultHadoopCoordinates;

  @JsonProperty
  private final boolean restoreTasksOnRestart;

  @JsonProperty
  private final Period gracefulShutdownTimeout;

  @JsonProperty
  private final Period directoryLockTimeout;

  @JsonProperty
  private final List<StorageLocationConfig> shuffleDataLocations;

  @JsonProperty
  private final boolean ignoreTimestampSpecForDruidInputSource;

  @JsonProperty
  private final boolean batchMemoryMappedIndex;

  @JsonProperty
  private final BatchProcessingMode batchProcessingMode;

  @JsonProperty
  private final boolean storeEmptyColumns;

  @JsonCreator
  public TaskConfig(
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("baseTaskDir") String baseTaskDir,
      @JsonProperty("hadoopWorkingPath") String hadoopWorkingPath,
      @JsonProperty("defaultRowFlushBoundary") Integer defaultRowFlushBoundary,
      @JsonProperty("defaultHadoopCoordinates") List<String> defaultHadoopCoordinates,
      @JsonProperty("restoreTasksOnRestart") boolean restoreTasksOnRestart,
      @JsonProperty("gracefulShutdownTimeout") Period gracefulShutdownTimeout,
      @JsonProperty("directoryLockTimeout") Period directoryLockTimeout,
      @JsonProperty("shuffleDataLocations") List<StorageLocationConfig> shuffleDataLocations,
      @JsonProperty("ignoreTimestampSpecForDruidInputSource") boolean ignoreTimestampSpecForDruidInputSource,
      @JsonProperty("batchMemoryMappedIndex") boolean batchMemoryMappedIndex, // deprecated, only set to true to fall back to older behavior
      @JsonProperty("batchProcessingMode") String batchProcessingMode,
      @JsonProperty("storeEmptyColumns") @Nullable Boolean storeEmptyColumns
  )
  {
    this.baseDir = baseDir == null ? System.getProperty("java.io.tmpdir") : baseDir;
    this.baseTaskDir = new File(defaultDir(baseTaskDir, "persistent/task"));
    // This is usually on HDFS or similar, so we can't use java.io.tmpdir
    this.hadoopWorkingPath = hadoopWorkingPath == null ? "/tmp/druid-indexing" : hadoopWorkingPath;
    this.defaultRowFlushBoundary = defaultRowFlushBoundary == null ? 75000 : defaultRowFlushBoundary;
    this.defaultHadoopCoordinates = defaultHadoopCoordinates == null
                                    ? DEFAULT_DEFAULT_HADOOP_COORDINATES
                                    : defaultHadoopCoordinates;
    this.restoreTasksOnRestart = restoreTasksOnRestart;
    this.gracefulShutdownTimeout = gracefulShutdownTimeout == null
                                   ? DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT
                                   : gracefulShutdownTimeout;
    this.directoryLockTimeout = directoryLockTimeout == null
                                ? DEFAULT_DIRECTORY_LOCK_TIMEOUT
                                : directoryLockTimeout;
    if (shuffleDataLocations == null) {
      this.shuffleDataLocations = Collections.singletonList(
          new StorageLocationConfig(new File(defaultDir(null, "intermediary-segments")), null, null)
      );
    } else {
      this.shuffleDataLocations = shuffleDataLocations;
    }
    this.ignoreTimestampSpecForDruidInputSource = ignoreTimestampSpecForDruidInputSource;

    this.batchMemoryMappedIndex = batchMemoryMappedIndex;
    // Conflict resolution. Assume that if batchMemoryMappedIndex is set (since false is the default) that
    // the user changed it intentionally to use legacy, in this case oveeride batchProcessingMode and also
    // set it to legacy else just use batchProcessingMode and don't pay attention to batchMemoryMappedIndexMode:
    if (batchMemoryMappedIndex) {
      this.batchProcessingMode = BatchProcessingMode.OPEN_SEGMENTS;
    } else if (EnumUtils.isValidEnum(BatchProcessingMode.class, batchProcessingMode)) {
      this.batchProcessingMode = BatchProcessingMode.valueOf(batchProcessingMode);
    } else {
      // batchProcessingMode input string is invalid, log & use the default.
      this.batchProcessingMode = BatchProcessingMode.CLOSED_SEGMENTS; // Default
      log.warn("Batch processing mode argument value is null or not valid:[%s], defaulting to[%s] ",
               batchProcessingMode, this.batchProcessingMode
      );
    }
    log.debug("Batch processing mode:[%s]", this.batchProcessingMode);
    this.storeEmptyColumns = storeEmptyColumns == null ? DEFAULT_STORE_EMPTY_COLUMNS : storeEmptyColumns;
  }

  @JsonProperty
  public String getBaseDir()
  {
    return baseDir;
  }

  @JsonProperty
  public File getBaseTaskDir()
  {
    return baseTaskDir;
  }

  public File getTaskDir(String taskId)
  {
    return new File(baseTaskDir, IdUtils.validateId("task ID", taskId));
  }

  public File getTaskWorkDir(String taskId)
  {
    return new File(getTaskDir(taskId), "work");
  }

  public File getTaskTempDir(String taskId)
  {
    return new File(getTaskDir(taskId), "temp");
  }

  public File getTaskLockFile(String taskId)
  {
    return new File(getTaskDir(taskId), "lock");
  }

  @JsonProperty
  public String getHadoopWorkingPath()
  {
    return hadoopWorkingPath;
  }

  @JsonProperty
  public int getDefaultRowFlushBoundary()
  {
    return defaultRowFlushBoundary;
  }

  @JsonProperty
  public List<String> getDefaultHadoopCoordinates()
  {
    return defaultHadoopCoordinates;
  }

  @JsonProperty
  public boolean isRestoreTasksOnRestart()
  {
    return restoreTasksOnRestart;
  }

  @JsonProperty
  public Period getGracefulShutdownTimeout()
  {
    return gracefulShutdownTimeout;
  }

  @JsonProperty
  public Period getDirectoryLockTimeout()
  {
    return directoryLockTimeout;
  }

  @JsonProperty
  public List<StorageLocationConfig> getShuffleDataLocations()
  {
    return shuffleDataLocations;
  }

  @JsonProperty
  public boolean isIgnoreTimestampSpecForDruidInputSource()
  {
    return ignoreTimestampSpecForDruidInputSource;
  }

  @JsonProperty
  public BatchProcessingMode getBatchProcessingMode()
  {
    return batchProcessingMode;
  }

  /**
   * Do not use in code! use {@link TaskConfig#getBatchProcessingMode() instead}
   */
  @Deprecated
  @JsonProperty
  public boolean getbatchMemoryMappedIndex()
  {
    return batchMemoryMappedIndex;
  }

  @JsonProperty
  public boolean isStoreEmptyColumns()
  {
    return storeEmptyColumns;
  }

  private String defaultDir(@Nullable String configParameter, final String defaultVal)
  {
    if (configParameter == null) {
      return Paths.get(getBaseDir(), defaultVal).toString();
    }

    return configParameter;
  }
}
