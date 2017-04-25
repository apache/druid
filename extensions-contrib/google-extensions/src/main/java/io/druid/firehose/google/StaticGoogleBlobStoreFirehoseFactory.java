/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.google;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.impl.PrefetcheableTextFilesFirehoseFactory;
import io.druid.storage.google.GoogleByteSource;
import io.druid.storage.google.GoogleStorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class StaticGoogleBlobStoreFirehoseFactory extends PrefetcheableTextFilesFirehoseFactory<GoogleBlob>
{
  private final GoogleStorage storage;

  @JsonCreator
  public StaticGoogleBlobStoreFirehoseFactory(
      @JacksonInject GoogleStorage storage,
      @JsonProperty("blobs") List<GoogleBlob> blobs,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Integer fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(blobs, maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.storage = storage;
  }

  @JsonProperty
  public List<GoogleBlob> getBlobs() {
    return getObjects();
  }

  @Override
  protected InputStream openStream(GoogleBlob object) throws IOException
  {
    final String bucket = object.getBucket();
    final String path = object.getPath().startsWith("/")
                        ? object.getPath().substring(1)
                        : object.getPath();

    return new GoogleByteSource(storage, bucket, path).openStream();
  }

  @Override
  protected boolean isGzipped(GoogleBlob object)
  {
    return object.getPath().endsWith(".gz");
  }
}

