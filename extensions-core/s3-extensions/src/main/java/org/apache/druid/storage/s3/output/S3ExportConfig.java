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

package org.apache.druid.storage.s3.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.HumanReadableBytes;

import javax.annotation.Nullable;
import java.util.List;

public class S3ExportConfig
{
  @JsonProperty("tempLocalDir")
  private final String tempLocalDir;
  @JsonProperty("chunkSize")
  private final HumanReadableBytes chunkSize;
  @JsonProperty("maxRetry")
  private final Integer maxRetry;
  @JsonProperty("allowedExportPaths")
  private final List<String> allowedExportPaths;

  @JsonCreator
  public S3ExportConfig(
      @JsonProperty("tempLocalDir") final String tempLocalDir,
      @JsonProperty("chunkSize") @Nullable final HumanReadableBytes chunkSize,
      @JsonProperty("maxRetry") @Nullable final Integer maxRetry,
      @JsonProperty("allowedExportPaths") final List<String> allowedExportPaths)
  {
    this.tempLocalDir = tempLocalDir;
    this.chunkSize = chunkSize;
    this.maxRetry = maxRetry;
    this.allowedExportPaths = allowedExportPaths;
  }

  public String getTempLocalDir()
  {
    return tempLocalDir;
  }

  public HumanReadableBytes getChunkSize()
  {
    return chunkSize;
  }

  public Integer getMaxRetry()
  {
    return maxRetry;
  }

  public List<String> getAllowedExportPaths()
  {
    return allowedExportPaths;
  }
}
