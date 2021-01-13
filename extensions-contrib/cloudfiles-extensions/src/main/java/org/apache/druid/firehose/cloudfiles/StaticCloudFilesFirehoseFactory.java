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

package org.apache.druid.firehose.cloudfiles;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.InputSourceSecurityConfig;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.cloudfiles.CloudFilesByteSource;
import org.apache.druid.storage.cloudfiles.CloudFilesObjectApiProxy;
import org.apache.druid.storage.cloudfiles.CloudFilesUtils;
import org.apache.druid.utils.CompressionUtils;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StaticCloudFilesFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<CloudFilesBlob>
{
  private static final Logger log = new Logger(StaticCloudFilesFirehoseFactory.class);

  private final CloudFilesApi cloudFilesApi;
  private final List<CloudFilesBlob> blobs;

  @JsonCreator
  public StaticCloudFilesFirehoseFactory(
      @JacksonInject CloudFilesApi cloudFilesApi,
      @JsonProperty("blobs") List<CloudFilesBlob> blobs,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.cloudFilesApi = cloudFilesApi;
    this.blobs = blobs;
  }

  @JsonProperty
  public List<CloudFilesBlob> getBlobs()
  {
    return blobs;
  }

  @Override
  protected Collection<CloudFilesBlob> initObjects()
  {
    return blobs;
  }

  @Override
  protected InputStream openObjectStream(CloudFilesBlob object) throws IOException
  {
    return openObjectStream(object, 0);
  }

  @Override
  protected InputStream openObjectStream(CloudFilesBlob object, long start) throws IOException
  {
    return createCloudFilesByteSource(object).openStream(start);
  }

  private CloudFilesByteSource createCloudFilesByteSource(CloudFilesBlob object)
  {
    final String region = object.getRegion();
    final String container = object.getContainer();
    final String path = object.getPath();

    log.info("Retrieving file from region[%s], container[%s] and path [%s]",
             region, container, path
    );
    CloudFilesObjectApiProxy objectApi = new CloudFilesObjectApiProxy(cloudFilesApi, region, container);
    return new CloudFilesByteSource(objectApi, path);
  }

  @Override
  protected InputStream wrapObjectStream(CloudFilesBlob object, InputStream stream) throws IOException
  {
    return CompressionUtils.decompress(stream, object.getPath());
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StaticCloudFilesFirehoseFactory that = (StaticCloudFilesFirehoseFactory) o;
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
    return CloudFilesUtils.CLOUDFILESRETRY;
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, CloudFilesBlob> withSplit(InputSplit<CloudFilesBlob> split)
  {
    return new StaticCloudFilesFirehoseFactory(
        cloudFilesApi,
        Collections.singletonList(split.get()),
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }

  @Override
  public void validateAllowDenyPrefixList(InputSourceSecurityConfig securityConfig)
  {
    securityConfig.validateURIAccess(blobs.stream()
                                          .map(blob -> URI.create(
                                              StringUtils.format(
                                                  "%s/%s",
                                                  cloudFilesApi.getCDNApi(blob.getRegion())
                                                               .get(blob.getContainer())
                                                               .getUri(),
                                                  blob.getPath()
                                              )))
                                          .collect(Collectors.toList()));
  }

}
