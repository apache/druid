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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.RetryUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * Configuration of the Azure storage connector
 */
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

  private static final HumanReadableBytes DEFAULT_CHUNK_SIZE = new HumanReadableBytes("4MiB");

  // Minimum limit is self-imposed, so that chunks are appropriately sized, and we don't spend a lot of time downloading
  // the part of the blobs
  private static final long AZURE_MIN_CHUNK_SIZE_BYTES = new HumanReadableBytes("256KiB").getBytes();

  // Maximum limit is imposed by Azure, on the size of one block blob
  private static final long AZURE_MAX_CHUNK_SIZE_BYTES = new HumanReadableBytes("4000MiB").getBytes();


  @JsonProperty
  private final int maxRetry;

  public AzureOutputConfig(
      @JsonProperty(value = "container", required = true) String container,
      @JsonProperty(value = "prefix", required = true) String prefix,
      @JsonProperty(value = "tempDir", required = true) File tempDir,
      @JsonProperty(value = "chunkSize") @Nullable HumanReadableBytes chunkSize,
      @JsonProperty(value = "maxRetry") @Nullable Integer maxRetry
  )
  {
    this.container = container;
    this.prefix = prefix;
    this.tempDir = tempDir;
    this.chunkSize = chunkSize != null ? chunkSize : DEFAULT_CHUNK_SIZE;
    this.maxRetry = maxRetry != null ? maxRetry : RetryUtils.DEFAULT_MAX_TRIES;
    validateFields();
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

  private void validateFields()
  {
    if (chunkSize.getBytes() < AZURE_MIN_CHUNK_SIZE_BYTES || chunkSize.getBytes() > AZURE_MAX_CHUNK_SIZE_BYTES) {
      throw InvalidInput.exception(
          "'chunkSize' [%d] bytes to the AzureConfig should be between [%d] bytes and [%d] bytes",
          chunkSize.getBytes(),
          AZURE_MIN_CHUNK_SIZE_BYTES,
          AZURE_MAX_CHUNK_SIZE_BYTES
      );
    }

    try {
      FileUtils.mkdirp(tempDir);
    }
    catch (IOException e) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(e, "Unable to create temporary directory [%s]", tempDir.getAbsolutePath());
    }

    if (!tempDir.canRead() || !tempDir.canWrite()) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(
                              "Cannot read or write on the 'tempDir' [%s]. "
                              + "Please provide a different path to store the intermediate contents of AzureStorageConnector",
                              tempDir.getAbsolutePath()
                          );
    }
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
