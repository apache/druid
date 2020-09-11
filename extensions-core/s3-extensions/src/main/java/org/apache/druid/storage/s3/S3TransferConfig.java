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

import javax.validation.constraints.Min;

/**
 */
public class S3TransferConfig
{
  @JsonProperty
  private boolean useTransferManager = false;

  @JsonProperty
  @Min(1)
  private long minimumUploadPartSize = 5 * 1024 * 1024L;

  @JsonProperty
  @Min(1)
  private long multipartUploadThreshold = 5 * 1024 * 1024L;

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

}
