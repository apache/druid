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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.HumanReadableBytesRange;
import org.apache.druid.java.util.common.RetryUtils;

import javax.annotation.Nullable;
import java.io.File;

public class S3OutputConfig
{
  public static final long S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES = 5L * 1024 * 1024;
  public static final long S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES = 5L * 1024 * 1024 * 1024L;
  private static final int S3_MULTIPART_UPLOAD_MAX_NUM_PARTS = 10_000;

  @JsonProperty
  private String bucket;

  @JsonProperty
  private String prefix;

  @JsonProperty
  private File tempDir;

  @Nullable
  @JsonProperty
  @HumanReadableBytesRange(
      min = S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES,
      max = S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES
  ) // limits of s3 multipart upload
  private HumanReadableBytes chunkSize;

  /**
   * Max size for each object. This limit relies on the s3 multipart upload limits.
   * See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html for more details.
   *
   * @see RetryableS3OutputStream
   */
  @JsonProperty
  @HumanReadableBytesRange(min = 5L * 1024 * 1024, max = 5L * 1024 * 1024 * 1024 * 1024)
  private HumanReadableBytes maxResultsSize = new HumanReadableBytes("100MiB");

  /**
   * Max number of tries for each upload.
   */
  @JsonProperty
  private int maxTriesOnTransientErrors = RetryUtils.DEFAULT_MAX_TRIES;

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
    return chunkSize == null ? computeMinChunkSize(getMaxResultsSize()) : chunkSize.getBytes();
  }

  public long getMaxResultsSize()
  {
    return maxResultsSize.getBytes();
  }

  public int getMaxTriesOnTransientError()
  {
    return maxTriesOnTransientErrors;
  }


  public static long computeMinChunkSize(long maxResultsSize)
  {
    return Math.max(
        (long) Math.ceil(maxResultsSize / (double) S3_MULTIPART_UPLOAD_MAX_NUM_PARTS),
        S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES
    );
  }

}
