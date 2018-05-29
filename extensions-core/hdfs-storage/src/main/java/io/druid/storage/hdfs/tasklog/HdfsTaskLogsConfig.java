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
package io.druid.storage.hdfs.tasklog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;

/**
 * Indexer hdfs task logs configuration.
 */
public class HdfsTaskLogsConfig
{
  @JsonProperty
  @NotNull
  private String directory;

  @JsonProperty
  private boolean useS3Backup = false;

  @JsonProperty
  private String backupS3Bucket = "";

  @JsonProperty
  private String backupS3BaseKey = "";

  public HdfsTaskLogsConfig(String directory)
  {
    this(directory, false, "", "");
  }

  @JsonCreator
  public HdfsTaskLogsConfig(
      @JsonProperty("directory") String directory,
      @JsonProperty("useS3Backup") boolean useS3Backup,
      @JsonProperty("backupS3Bucket") String backupS3Bucket,
      @JsonProperty("backupS3BaseKey") String backupS3BaseKey
  )
  {
    this.directory = directory;
    this.useS3Backup = useS3Backup;
    this.backupS3Bucket = Objects.firstNonNull(backupS3Bucket, "");
    this.backupS3BaseKey = Objects.firstNonNull(backupS3BaseKey, "");

    if (useS3Backup) {
      Preconditions.checkArgument(
          backupS3Bucket != null && backupS3Bucket.length() > 0,
          "must specify backupS3Bucket for task logs"
      );
    }
  }

  public String getDirectory()
  {
    return directory;
  }

  public boolean isUseS3Backup()
  {
    return useS3Backup;
  }

  public String getBackupS3Bucket()
  {
    return backupS3Bucket;
  }

  public String getBackupS3BaseKey()
  {
    return backupS3BaseKey;
  }
}

