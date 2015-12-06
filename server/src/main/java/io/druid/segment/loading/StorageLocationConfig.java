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

package io.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.File;

/**
 */
public class StorageLocationConfig
{
  @JsonProperty
  @NotNull
  private File path = null;

  @JsonProperty
  @Min(1)
  private long maxSize = Long.MAX_VALUE;

  public File getPath()
  {
    return path;
  }

  public StorageLocationConfig setPath(File path)
  {
    this.path = path;
    return this;
  }

  public long getMaxSize()
  {
    return maxSize;
  }

  public StorageLocationConfig setMaxSize(long maxSize)
  {
    this.maxSize = maxSize;
    return this;
  }

  @Override
  public String toString()
  {
    return "StorageLocationConfig{" +
           "path=" + path +
           ", maxSize=" + maxSize +
           '}';
  }
}
