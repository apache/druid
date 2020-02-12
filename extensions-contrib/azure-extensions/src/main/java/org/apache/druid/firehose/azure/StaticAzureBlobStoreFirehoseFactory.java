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

package org.apache.druid.firehose.azure;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.azure.AzureByteSource;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.AzureUtils;
import org.apache.druid.utils.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This class is heavily inspired by the StaticS3FirehoseFactory class in the org.apache.druid.firehose.s3 package
 *
 * @deprecated as of version 0.18.0 because support for firehose has been discontinued. Please use
 * {@link org.apache.druid.data.input.azure.AzureInputSource} instead.
 */
@Deprecated
public class StaticAzureBlobStoreFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<AzureBlob>
{
  private final AzureStorage azureStorage;
  private final List<AzureBlob> blobs;

  @JsonCreator
  public StaticAzureBlobStoreFirehoseFactory(
      @JacksonInject AzureStorage azureStorage,
      @JsonProperty("blobs") List<AzureBlob> blobs,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.blobs = blobs;
    this.azureStorage = azureStorage;
  }

  @JsonProperty
  public List<AzureBlob> getBlobs()
  {
    return blobs;
  }

  @Override
  protected Collection<AzureBlob> initObjects()
  {
    return blobs;
  }

  @Override
  protected InputStream openObjectStream(AzureBlob object) throws IOException
  {
    return makeByteSource(azureStorage, object).openStream();
  }

  @Override
  protected InputStream openObjectStream(AzureBlob object, long start) throws IOException
  {
    // BlobInputStream.skip() moves the next read offset instead of skipping first 'start' bytes.
    final InputStream in = openObjectStream(object);
    final long skip = in.skip(start);
    Preconditions.checkState(skip == start, "start offset was [%s] but [%s] bytes were skipped", start, skip);
    return in;
  }

  @Override
  protected InputStream wrapObjectStream(AzureBlob object, InputStream stream) throws IOException
  {
    return CompressionUtils.decompress(stream, object.getPath());
  }

  private static AzureByteSource makeByteSource(AzureStorage azureStorage, AzureBlob object)
  {
    final String container = object.getContainer();
    final String path = StringUtils.maybeRemoveLeadingSlash(object.getPath());

    return new AzureByteSource(azureStorage, container, path);
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

    final StaticAzureBlobStoreFirehoseFactory that = (StaticAzureBlobStoreFirehoseFactory) o;

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
    return AzureUtils.AZURE_RETRY;
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, AzureBlob> withSplit(InputSplit<AzureBlob> split)
  {
    return new StaticAzureBlobStoreFirehoseFactory(
        azureStorage,
        Collections.singletonList(split.get()),
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }
}
