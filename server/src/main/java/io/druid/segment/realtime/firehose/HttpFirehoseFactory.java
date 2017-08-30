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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.impl.PrefetchableTextFilesFirehoseFactory;
import io.druid.java.util.common.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;

public class HttpFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<URI>
{
  private final List<URI> uris;

  @JsonCreator
  public HttpFirehoseFactory(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.uris = uris;
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @Override
  protected Collection<URI> initObjects()
  {
    return uris;
  }

  @Override
  protected InputStream openObjectStream(URI object) throws IOException
  {
    return object.toURL().openConnection().getInputStream();
  }

  @Override
  protected InputStream wrapObjectStream(URI object, InputStream stream) throws IOException
  {
    return object.getPath().endsWith(".gz") ? CompressionUtils.gzipInputStream(stream) : stream;
  }
}
