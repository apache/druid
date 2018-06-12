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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.net.HttpHeaders;
import io.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class HttpFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<URI>
{
  private static final Logger log = new Logger(HttpFirehoseFactory.class);
  private final List<URI> uris;
  private final boolean supportContentRange;

  @JsonCreator
  public HttpFirehoseFactory(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  ) throws IOException
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.uris = uris;

    Preconditions.checkArgument(uris.size() > 0, "Empty URIs");
    final URLConnection connection = uris.get(0).toURL().openConnection();
    final String acceptRanges = connection.getHeaderField(HttpHeaders.ACCEPT_RANGES);
    this.supportContentRange = acceptRanges != null && acceptRanges.equalsIgnoreCase("bytes");
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
  protected InputStream openObjectStream(URI object, long start) throws IOException
  {
    if (supportContentRange) {
      final URLConnection connection = object.toURL().openConnection();
      // Set header for range request.
      // Since we need to set only the start offset, the header is "bytes=<range-start>-".
      // See https://tools.ietf.org/html/rfc7233#section-2.1
      connection.addRequestProperty(HttpHeaders.RANGE, StringUtils.format("bytes=%d-", start));
      return connection.getInputStream();
    } else {
      log.warn(
          "Since the input source doesn't support range requests, the object input stream is opened from the start and "
          + "then skipped. This may make the ingestion speed slower. Consider enabling prefetch if you see this message"
          + " a lot."
      );
      final InputStream in = openObjectStream(object);
      in.skip(start);
      return in;
    }
  }

  @Override
  protected InputStream wrapObjectStream(URI object, InputStream stream) throws IOException
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

    final HttpFirehoseFactory that = (HttpFirehoseFactory) o;
    return Objects.equals(uris, that.uris) &&
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
        uris,
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
    return e -> e instanceof IOException;
  }
}
