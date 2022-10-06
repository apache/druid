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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import java.io.File;

@JsonTypeName(S3StorageDruidModule.SCHEME)
public class S3StorageConnectorProvider extends S3OutputConfig implements StorageConnectorProvider
{
  @JacksonInject
  ServerSideEncryptingAmazonS3 s3;

  @JsonCreator
  public S3StorageConnectorProvider(
      @JsonProperty(value = "bucket", required = true) String bucket,
      @JsonProperty(value = "prefix", required = true) String prefix,
      @JsonProperty(value = "tempDir", required = true) File tempDir,
      @JsonProperty("chunkSize") HumanReadableBytes chunkSize,
      @JsonProperty("maxResultsSize") HumanReadableBytes maxResultsSize,
      @JsonProperty("maxRetry") Integer maxRetry
  )
  {
    super(bucket, prefix, tempDir, chunkSize, maxResultsSize, maxRetry);
  }

  @Override
  public StorageConnector get()
  {
    return new S3StorageConnector(this, s3);
  }
}
