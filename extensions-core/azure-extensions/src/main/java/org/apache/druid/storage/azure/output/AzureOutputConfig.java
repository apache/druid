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

package org.apache.druid.storage.azure.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.RetryUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class AzureOutputConfig
{
  @JsonProperty
  private final String container;

  @JsonProperty
  private final String prefix;

  @JsonProperty
  private final File tempDir;

  @JsonProperty
  private final HumanReadableBytes chunkSize;

  private static final HumanReadableBytes DEFAULT_CHUNK_SIZE = new HumanReadableBytes("100MiB");

  @JsonProperty
  private final int maxRetry;

  public AzureOutputConfig(
      final String container,
      final String prefix,
      final File tempDir,
      @Nullable final HumanReadableBytes chunkSize,
      @Nullable final Integer maxRetry
  )
  {
    this.container = container;
    this.prefix = prefix;
    this.tempDir = tempDir;
    this.chunkSize = chunkSize != null ? chunkSize : DEFAULT_CHUNK_SIZE;
    this.maxRetry = maxRetry != null ? maxRetry : RetryUtils.DEFAULT_MAX_TRIES;
  }


  public String getContainer()
  {
    return container;
  }

  public String getPrefix()
  {
    return prefix;
  }

  public File getTempDir()
  {
    return tempDir;
  }

  public HumanReadableBytes getChunkSize()
  {
    return chunkSize;
  }

  public int getMaxRetry()
  {
    return maxRetry;
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
    AzureOutputConfig that = (AzureOutputConfig) o;
    return maxRetry == that.maxRetry
           && Objects.equals(container, that.container)
           && Objects.equals(prefix, that.prefix)
           && Objects.equals(tempDir, that.tempDir)
           && Objects.equals(chunkSize, that.chunkSize);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(container, prefix, tempDir, chunkSize, maxRetry);
  }

  @Override
  public String toString()
  {
    return "AzureOutputConfig{" +
           "container='" + container + '\'' +
           ", prefix='" + prefix + '\'' +
           ", tempDir=" + tempDir +
           ", chunkSize=" + chunkSize +
           ", maxRetry=" + maxRetry +
           '}';
  }
}
