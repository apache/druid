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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageDruidModule;

import java.io.File;

@JsonTypeName(GoogleStorageDruidModule.SCHEME)
public class GoogleStorageConnectorProvider extends GoogleOutputConfig implements StorageConnectorProvider
{

  private final GoogleStorage storage;
  private final GoogleInputDataConfig inputDataConfig;

  @JsonCreator
  public GoogleStorageConnectorProvider(
      @JacksonInject GoogleStorage storage,
      @JacksonInject GoogleInputDataConfig inputDataConfig,
      @JsonProperty(value = "bucket", required = true) final String bucket,
      @JsonProperty(value = "prefix", required = true) final String prefix,
      @JsonProperty(value = "tempDir", required = true) final File tempDir,
      @JsonProperty(value = "chunkedDownloads") final Boolean chunkedDownloads,
      @JsonProperty(value = "chunkSize") final HumanReadableBytes chunkSize,
      @JsonProperty(value = "maxRetry") final Integer maxRetry
  )
  {
    super(bucket, prefix, tempDir, chunkedDownloads, chunkSize, maxRetry);
    this.storage = Preconditions.checkNotNull(storage, "google client must be provided");
    this.inputDataConfig = Preconditions.checkNotNull(inputDataConfig, "google input data config must be provided");
  }

  @Override
  public StorageConnector get()
  {
    return new GoogleStorageConnector(storage, this, inputDataConfig);
  }
}
