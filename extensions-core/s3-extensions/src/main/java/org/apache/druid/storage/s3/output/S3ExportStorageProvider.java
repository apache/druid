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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.s3.S3InputSource;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.storage.StorageConfig;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorUtils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nullable;
import java.io.File;

@JsonTypeName(S3ExportStorageProvider.TYPE_NAME)
public class S3ExportStorageProvider implements ExportStorageProvider
{
  public static final String TYPE_NAME = S3InputSource.TYPE_KEY;
  @JsonProperty
  private final String bucket;
  @JsonProperty
  private final String prefix;
  @JsonProperty
  private final String tempSubDir;
  @JsonProperty
  @Nullable
  private final HumanReadableBytes chunkSize;
  @JsonProperty
  @Nullable
  private final Integer maxRetry;

  @JacksonInject
  StorageConfig storageConfig;
  @JacksonInject
  ServerSideEncryptingAmazonS3 s3;

  @JsonCreator
  public S3ExportStorageProvider(
      @JsonProperty(value = "bucket", required = true) String bucket,
      @JsonProperty(value = "prefix", required = true) String prefix,
      @JsonProperty(value = "tempSubDir") @Nullable String tempSubDir,
      @JsonProperty("chunkSize") @Nullable HumanReadableBytes chunkSize,
      @JsonProperty("maxRetry") @Nullable Integer maxRetry
  )
  {
    this.bucket = bucket;
    this.prefix = prefix;
    this.tempSubDir = tempSubDir == null ? "" : tempSubDir;
    this.chunkSize = chunkSize;
    this.maxRetry = maxRetry;
  }

  @Override
  public StorageConnector get()
  {
    final File temporaryDirectory = StorageConnectorUtils.validateAndGetPath(storageConfig.getBaseDir(), tempSubDir);
    final S3OutputConfig s3OutputConfig = new S3OutputConfig(bucket, prefix, temporaryDirectory, chunkSize, maxRetry);
    return new S3StorageConnector(s3OutputConfig, s3);
  }

  @JsonProperty("bucket")
  public String getBucket()
  {
    return bucket;
  }

  @JsonProperty("prefix")
  public String getPrefix()
  {
    return prefix;
  }

  @JsonProperty("tempSubDir")
  public String getTempSubDir()
  {
    return tempSubDir;
  }

  @JsonProperty("chunkSize")
  @Nullable
  public HumanReadableBytes getChunkSize()
  {
    return chunkSize;
  }

  @JsonProperty("maxRetry")
  @Nullable
  public Integer getMaxRetry()
  {
    return maxRetry;
  }

  @Override
  @JsonIgnore
  public String getResourceType()
  {
    return TYPE_NAME;
  }
}
