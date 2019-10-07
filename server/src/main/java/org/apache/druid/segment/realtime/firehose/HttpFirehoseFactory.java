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

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HttpFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<URI>
{
  private static final Logger log = new Logger(HttpFirehoseFactory.class);
  private final List<URI> uris;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;

  @JsonCreator
  public HttpFirehoseFactory(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry,
      @JsonProperty("httpAuthenticationUsername") @Nullable String httpAuthenticationUsername,
      @JsonProperty("httpAuthenticationPassword") @Nullable PasswordProvider httpAuthenticationPasswordProvider
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    Preconditions.checkArgument(uris.size() > 0, "Empty URIs");
    this.uris = uris;
    this.httpAuthenticationUsername = httpAuthenticationUsername;
    this.httpAuthenticationPasswordProvider = httpAuthenticationPasswordProvider;
  }

  @Nullable
  @JsonProperty
  public String getHttpAuthenticationUsername()
  {
    return httpAuthenticationUsername;
  }

  @Nullable
  @JsonProperty("httpAuthenticationPassword")
  public PasswordProvider getHttpAuthenticationPasswordProvider()
  {
    return httpAuthenticationPasswordProvider;
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
    // A negative start value will ensure no bytes of the InputStream are skipped
    return openObjectStream(object, 0);
  }

  @Override
  protected InputStream openObjectStream(URI object, long start) throws IOException
  {
    URLConnection urlConnection = openURLConnection(object);
    final String acceptRanges = urlConnection.getHeaderField(HttpHeaders.ACCEPT_RANGES);
    final boolean withRanges = "bytes".equalsIgnoreCase(acceptRanges);
    if (withRanges && start > 0) {
      // Set header for range request.
      // Since we need to set only the start offset, the header is "bytes=<range-start>-".
      // See https://tools.ietf.org/html/rfc7233#section-2.1
      urlConnection.addRequestProperty(HttpHeaders.RANGE, StringUtils.format("bytes=%d-", start));
      return urlConnection.getInputStream();
    } else {
      if (!withRanges && start > 0) {
        log.warn(
                "Since the input source doesn't support range requests, the object input stream is opened from the start and "
                        + "then skipped. This may make the ingestion speed slower. Consider enabling prefetch if you see this message"
                        + " a lot."
        );
      }
      final InputStream in = urlConnection.getInputStream();
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
           getMaxFetchRetry() == that.getMaxFetchRetry() &&
           Objects.equals(httpAuthenticationUsername, that.getHttpAuthenticationUsername()) &&
           Objects.equals(httpAuthenticationPasswordProvider, that.getHttpAuthenticationPasswordProvider());
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
        getMaxFetchRetry(),
        httpAuthenticationUsername,
        httpAuthenticationPasswordProvider
    );
  }

  @Override
  protected Predicate<Throwable> getRetryCondition()
  {
    return e -> e instanceof IOException;
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, URI> withSplit(InputSplit<URI> split)
  {
    return new HttpFirehoseFactory(
        Collections.singletonList(split.get()),
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry(),
        getHttpAuthenticationUsername(),
        httpAuthenticationPasswordProvider
    );
  }

  private URLConnection openURLConnection(URI object) throws IOException
  {
    URLConnection urlConnection = object.toURL().openConnection();
    if (!Strings.isNullOrEmpty(httpAuthenticationUsername) && httpAuthenticationPasswordProvider != null) {
      String userPass = httpAuthenticationUsername + ":" + httpAuthenticationPasswordProvider.getPassword();
      String basicAuthString = "Basic " + Base64.getEncoder().encodeToString(StringUtils.toUtf8(userPass));
      urlConnection.setRequestProperty("Authorization", basicAuthString);
    }
    return urlConnection;
  }
}
