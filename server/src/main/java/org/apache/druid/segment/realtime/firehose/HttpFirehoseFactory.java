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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.HttpEntity;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HttpFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<URI>
{
  private final List<URI> uris;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;
  private final HttpInputSourceConfig inputSourceConfig;

  @JsonCreator
  public HttpFirehoseFactory(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry,
      @JsonProperty("httpAuthenticationUsername") @Nullable String httpAuthenticationUsername,
      @JsonProperty("httpAuthenticationPassword") @Nullable PasswordProvider httpAuthenticationPasswordProvider,
      @JacksonInject HttpInputSourceConfig inputSourceConfig
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    Preconditions.checkArgument(uris.size() > 0, "Empty URIs");
    HttpInputSource.throwIfInvalidProtocols(inputSourceConfig, uris);
    this.uris = uris;
    this.httpAuthenticationUsername = httpAuthenticationUsername;
    this.httpAuthenticationPasswordProvider = httpAuthenticationPasswordProvider;
    this.inputSourceConfig = inputSourceConfig;
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
    return HttpEntity.openInputStream(object, httpAuthenticationUsername, httpAuthenticationPasswordProvider, start);
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
    HttpFirehoseFactory that = (HttpFirehoseFactory) o;
    return uris.equals(that.uris) &&
           Objects.equals(httpAuthenticationUsername, that.httpAuthenticationUsername) &&
           Objects.equals(httpAuthenticationPasswordProvider, that.httpAuthenticationPasswordProvider) &&
           inputSourceConfig.equals(that.inputSourceConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, httpAuthenticationUsername, httpAuthenticationPasswordProvider, inputSourceConfig);
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
        httpAuthenticationPasswordProvider,
        inputSourceConfig
    );
  }
}
