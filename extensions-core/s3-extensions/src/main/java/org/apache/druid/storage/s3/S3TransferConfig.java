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

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.apache.druid.storage.s3.output.S3OutputConfig;

/**
 */
public class S3TransferConfig
{
  @JsonProperty
  private boolean useTransferManager = true;

  /**
   * Size of each part of a multipart upload except the last, which S3 requires to be between 5MiB and 5GiB. A value
   * outside that range is rejected at configuration time rather than surfacing as an {@code EntityTooSmall} or
   * {@code EntityTooLarge} on every upload.
   */
  @JsonProperty
  @Min(S3OutputConfig.S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES)
  @Max(S3OutputConfig.S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES)
  private long minimumUploadPartSize = 20 * 1024 * 1024L;

  /**
   * Upload size at or above which multipart is used instead of a single PUT. Unlike {@link #minimumUploadPartSize}
   * this has no 5MiB floor: S3 exempts the final part of an upload from the minimum, so an upload just over a small
   * threshold is still valid as a single undersized part.
   */
  @JsonProperty
  @Min(1)
  private long multipartUploadThreshold = 20 * 1024 * 1024L;

  /**
   * Async HTTP client implementation to use with the S3 transfer manager.
   * Accepted values: {@code "crt"} (Amazon CRT, default) or {@code "netty"} (Netty NIO).
   */
  @JsonProperty
  private String asyncHttpClientType = "crt";

  public void setUseTransferManager(boolean useTransferManager)
  {
    this.useTransferManager = useTransferManager;
  }

  public void setMinimumUploadPartSize(long minimumUploadPartSize)
  {
    this.minimumUploadPartSize = minimumUploadPartSize;
  }

  public void setMultipartUploadThreshold(long multipartUploadThreshold)
  {
    this.multipartUploadThreshold = multipartUploadThreshold;
  }

  public void setAsyncHttpClientType(String asyncHttpClientType)
  {
    this.asyncHttpClientType = asyncHttpClientType;
  }

  public boolean isUseTransferManager()
  {
    return useTransferManager;
  }

  public long getMinimumUploadPartSize()
  {
    return minimumUploadPartSize;
  }

  public long getMultipartUploadThreshold()
  {
    return multipartUploadThreshold;
  }

  public String getAsyncHttpClientType()
  {
    return asyncHttpClientType;
  }

}
