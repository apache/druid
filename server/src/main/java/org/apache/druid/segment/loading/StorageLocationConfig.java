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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.File;

/**
 */
public class StorageLocationConfig
{
  private final File path;
  private final long maxSize;
  @Nullable
  private final Double freeSpacePercent;

  @JsonCreator
  public StorageLocationConfig(
      @JsonProperty("path") File path,
      @JsonProperty("maxSize") @Nullable Long maxSize,
      @JsonProperty("freeSpacePercent") @Nullable Double freeSpacePercent
  )
  {
    this.path = Preconditions.checkNotNull(path, "path");
    this.maxSize = maxSize == null ? Long.MAX_VALUE : maxSize;
    this.freeSpacePercent = freeSpacePercent;
    Preconditions.checkArgument(this.maxSize > 0, "maxSize[%s] should be positive", this.maxSize);
    Preconditions.checkArgument(
        this.freeSpacePercent == null || this.freeSpacePercent >= 0,
        "freeSpacePercent[%s] should be 0 or a positive double",
        this.freeSpacePercent
    );
  }

  @JsonProperty
  public File getPath()
  {
    return path;
  }

  @JsonProperty
  public long getMaxSize()
  {
    return maxSize;
  }

  @JsonProperty
  @Nullable
  public Double getFreeSpacePercent()
  {
    return freeSpacePercent;
  }

  @Override
  public String toString()
  {
    return "StorageLocationConfig{" +
           "path=" + path +
           ", maxSize=" + maxSize +
           ", freeSpacePercent=" + freeSpacePercent +
           '}';
  }
}
