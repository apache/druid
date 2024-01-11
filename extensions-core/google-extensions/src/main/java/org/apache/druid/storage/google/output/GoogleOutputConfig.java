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

package org.apache.druid.storage.google.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.RetryUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class GoogleOutputConfig
{

  @JsonProperty
  private final String bucket;

  @JsonProperty
  private final String prefix;

  @JsonProperty
  private final File tempDir;

  @JsonProperty
  private HumanReadableBytes chunkSize;

  private static final HumanReadableBytes DEFAULT_CHUNK_SIZE = new HumanReadableBytes("4MiB");

  // GCS imposed minimum chunk size
  private static final long GOOGLE_MIN_CHUNK_SIZE_BYTES = new HumanReadableBytes("256KiB").getBytes();

  // Self-imposed max chunk size since this size is allocated per open file consuming significant memory.
  private static final long GOOGLE_MAX_CHUNK_SIZE_BYTES = new HumanReadableBytes("16MiB").getBytes();


  @JsonProperty
  private int maxRetry;

  public GoogleOutputConfig(
      final String bucket,
      final String prefix,
      final File tempDir,
      @Nullable final HumanReadableBytes chunkSize,
      @Nullable final Integer maxRetry
  )
  {
    this.bucket = bucket;
    this.prefix = prefix;
    this.tempDir = tempDir;
    this.chunkSize = chunkSize != null ? chunkSize : DEFAULT_CHUNK_SIZE;
    this.maxRetry = maxRetry != null ? maxRetry : RetryUtils.DEFAULT_MAX_TRIES;

    validateFields();
  }

  public String getBucket()
  {
    return bucket;
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

  public Integer getMaxRetry()
  {
    return maxRetry;
  }

  private void validateFields()
  {
    if (chunkSize.getBytes() < GOOGLE_MIN_CHUNK_SIZE_BYTES || chunkSize.getBytes() > GOOGLE_MAX_CHUNK_SIZE_BYTES) {
      throw InvalidInput.exception(
          "'chunkSize' [%d] bytes to the GoogleConfig should be between [%d] bytes and [%d] bytes",
          chunkSize.getBytes(),
          GOOGLE_MIN_CHUNK_SIZE_BYTES,
          GOOGLE_MAX_CHUNK_SIZE_BYTES
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
    GoogleOutputConfig that = (GoogleOutputConfig) o;
    return Objects.equals(bucket, that.bucket)
           && Objects.equals(prefix, that.prefix)
           && Objects.equals(tempDir, that.tempDir)
           && Objects.equals(chunkSize, that.chunkSize)
           && Objects.equals(maxRetry, that.maxRetry);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket, prefix, tempDir, chunkSize, maxRetry);
  }

  @Override
  public String toString()
  {
    return "GoogleOutputConfig{" +
           "container='" + bucket + '\'' +
           ", prefix='" + prefix + '\'' +
           ", tempDir=" + tempDir +
           ", chunkSize=" + chunkSize +
           ", maxRetry=" + maxRetry +
           '}';
  }
}
