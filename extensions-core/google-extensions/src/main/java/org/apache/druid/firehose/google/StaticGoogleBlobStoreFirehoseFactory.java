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

package org.apache.druid.firehose.google;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.google.GoogleByteSource;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;
import org.apache.druid.utils.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StaticGoogleBlobStoreFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<GoogleBlob>
{
  private final GoogleStorage storage;
  private final List<GoogleBlob> blobs;

  @JsonCreator
  public StaticGoogleBlobStoreFirehoseFactory(
      @JacksonInject GoogleStorage storage,
      @JsonProperty("blobs") List<GoogleBlob> blobs,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.storage = storage;
    this.blobs = blobs;
  }

  @JsonProperty
  public List<GoogleBlob> getBlobs()
  {
    return blobs;
  }

  @Override
  protected Collection<GoogleBlob> initObjects()
  {
    return blobs;
  }

  @Override
  protected InputStream openObjectStream(GoogleBlob object) throws IOException
  {
    return openObjectStream(object, 0);
  }

  @Override
  protected InputStream openObjectStream(GoogleBlob object, long start) throws IOException
  {
    return createGoogleByteSource(object).openStream(start);
  }

  private GoogleByteSource createGoogleByteSource(GoogleBlob object)
  {
    final String bucket = object.getBucket();
    final String path = StringUtils.maybeRemoveLeadingSlash(object.getPath());

    return new GoogleByteSource(storage, bucket, path);
  }

  @Override
  protected InputStream wrapObjectStream(GoogleBlob object, InputStream stream) throws IOException
  {
    return CompressionUtils.decompress(stream, object.getPath());
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

    final StaticGoogleBlobStoreFirehoseFactory that = (StaticGoogleBlobStoreFirehoseFactory) o;

    return Objects.equals(blobs, that.blobs) &&
           getMaxCacheCapacityBytes() == that.getMaxCacheCapacityBytes() &&
           getMaxFetchCapacityBytes() == that.getMaxFetchCapacityBytes() &&
           getPrefetchTriggerBytes() == that.getPrefetchTriggerBytes() &&
           getFetchTimeout() == that.getFetchTimeout() &&
           getMaxFetchRetry() == that.getMaxFetchRetry();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        blobs,
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }

  @Override
  protected Predicate<Throwable> getRetryCondition()
  {
    return GoogleUtils::isRetryable;
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, GoogleBlob> withSplit(InputSplit<GoogleBlob> split)
  {
    return new StaticGoogleBlobStoreFirehoseFactory(
        storage,
        Collections.singletonList(split.get()),
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }
}

