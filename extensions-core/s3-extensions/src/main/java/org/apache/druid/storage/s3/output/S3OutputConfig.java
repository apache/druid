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
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class S3OutputConfig
{
  public static final long S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES = 5L * 1024 * 1024;
  public static final long S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES = 5L * 1024 * 1024 * 1024L;

  @JsonProperty
  private String bucket;
  @JsonProperty
  private String prefix;

  @JsonProperty
  private File tempDir;

  @Nullable
  @JsonProperty
  private HumanReadableBytes chunkSize = new HumanReadableBytes("100MiB");

  /**
   * Max number of tries for each upload.
   */
  @JsonProperty
  private int maxRetry = RetryUtils.DEFAULT_MAX_TRIES;

  @JsonCreator
  public S3OutputConfig(
      @JsonProperty(value = "bucket", required = true) String bucket,
      @JsonProperty(value = "prefix", required = true) String prefix,
      @JsonProperty(value = "tempDir", required = true) File tempDir,
      @JsonProperty("chunkSize") HumanReadableBytes chunkSize,
      @JsonProperty("maxRetry") Integer maxRetry
  )
  {
    this(bucket, prefix, tempDir, chunkSize, maxRetry, true);
  }

  @VisibleForTesting
  protected S3OutputConfig(
      String bucket,
      String prefix,
      File tempDir,
      @Nullable
      HumanReadableBytes chunkSize,
      @Nullable
      Integer maxRetry,
      boolean validation
  )
  {
    this.bucket = bucket;
    this.prefix = prefix;
    this.tempDir = tempDir;
    if (chunkSize != null) {
      this.chunkSize = chunkSize;
    }
    if (maxRetry != null) {
      this.maxRetry = maxRetry;
    }

    if (validation) {
      validateFields();
    }
  }

  private void validateFields()
  {
    if (chunkSize != null && (chunkSize.getBytes() < S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES
                              || chunkSize.getBytes() > S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES)) {
      throw new IAE(
          "chunkSize[%d] should be >= [%d] and <= [%d] bytes or null",
          chunkSize.getBytes(),
          S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES,
          S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES
      );
    }

    //check results size and chunk size are compatible.
    if (chunkSize != null) {
      validateChunkSize(chunkSize.getBytes());
    }
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

  public Long getChunkSize()
  {
    return chunkSize.getBytes();
  }

  public int getMaxRetry()
  {
    return maxRetry;
  }

  private static void validateChunkSize(long chunkSize)
  {
    if (S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES < chunkSize) {
      throw new IAE(
          "chunkSize[%d] should be smaller than [%d]",
          chunkSize,
          S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES
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
    S3OutputConfig that = (S3OutputConfig) o;
    return maxRetry == that.maxRetry
           && bucket.equals(that.bucket)
           && prefix.equals(that.prefix)
           && tempDir.equals(that.tempDir)
           && Objects.equals(chunkSize, that.chunkSize);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket, prefix, tempDir, chunkSize, maxRetry);
  }

}
