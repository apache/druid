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

package io.druid.firehose.cloudfiles;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.impl.PrefetchableTextFilesFirehoseFactory;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.storage.cloudfiles.CloudFilesByteSource;
import io.druid.storage.cloudfiles.CloudFilesObjectApiProxy;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

public class StaticCloudFilesFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<CloudFilesBlob>
{
  private static final Logger log = new Logger(StaticCloudFilesFirehoseFactory.class);

  private final CloudFilesApi cloudFilesApi;
  private final List<CloudFilesBlob> blobs;

  @JsonCreator
  public StaticCloudFilesFirehoseFactory(
      @JacksonInject("objectApi") CloudFilesApi cloudFilesApi,
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
    final String region = object.getRegion();
    final String container = object.getContainer();
    final String path = object.getPath();

    log.info("Retrieving file from region[%s], container[%s] and path [%s]",
             region, container, path
    );
    CloudFilesObjectApiProxy objectApi = new CloudFilesObjectApiProxy(
        cloudFilesApi, region, container);
    final CloudFilesByteSource byteSource = new CloudFilesByteSource(objectApi, path);

    return byteSource.openStream();
  }

  @Override
  protected InputStream wrapObjectStream(CloudFilesBlob object, InputStream stream) throws IOException
  {
    return object.getPath().endsWith(".gz") ? CompressionUtils.gzipInputStream(stream) : stream;
  }
}
