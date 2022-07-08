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

package org.apache.druid.rpc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.http.client.Request;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Used by {@link ServiceClient} to generate {@link Request} objects for an
 * {@link org.apache.druid.java.util.http.client.HttpClient}.
 */
public class RequestBuilder
{
  @VisibleForTesting
  static final Duration DEFAULT_TIMEOUT = Duration.standardMinutes(2);

  private final HttpMethod method;
  private final String encodedPathAndQueryString;
  private final Multimap<String, String> headers = HashMultimap.create();
  private String contentType = null;
  private byte[] content = null;
  private Duration timeout = DEFAULT_TIMEOUT;

  public RequestBuilder(final HttpMethod method, final String encodedPathAndQueryString)
  {
    this.method = Preconditions.checkNotNull(method, "method");
    this.encodedPathAndQueryString = Preconditions.checkNotNull(encodedPathAndQueryString, "encodedPathAndQueryString");

    if (!encodedPathAndQueryString.startsWith("/")) {
      throw new IAE("Path must start with '/'");
    }
  }

  public RequestBuilder header(final String header, final String value)
  {
    headers.put(header, value);
    return this;
  }

  public RequestBuilder content(final String contentType, final byte[] content)
  {
    this.contentType = Preconditions.checkNotNull(contentType, "contentType");
    this.content = Preconditions.checkNotNull(content, "content");
    return this;
  }

  public RequestBuilder jsonContent(final ObjectMapper jsonMapper, final Object content)
  {
    try {
      this.contentType = MediaType.APPLICATION_JSON;
      this.content = jsonMapper.writeValueAsBytes(Preconditions.checkNotNull(content, "content"));
      return this;
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public RequestBuilder smileContent(final ObjectMapper smileMapper, final Object content)
  {
    try {
      this.contentType = SmileMediaTypes.APPLICATION_JACKSON_SMILE;
      this.content = smileMapper.writeValueAsBytes(Preconditions.checkNotNull(content, "content"));
      return this;
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public RequestBuilder timeout(final Duration timeout)
  {
    this.timeout = Preconditions.checkNotNull(timeout, "timeout");
    return this;
  }

  /**
   * Accessor for request timeout. Provided because the timeout is not part of the request generated
   * by {@link #build(ServiceLocation)}.
   *
   * If there is no timeout, returns an empty Duration.
   */
  public Duration getTimeout()
  {
    return timeout;
  }

  public Request build(ServiceLocation serviceLocation)
  {
    // It's expected that our encodedPathAndQueryString starts with '/' and the service base path doesn't end with one.
    final String path = serviceLocation.getBasePath() + encodedPathAndQueryString;
    final Request request = new Request(method, makeURL(serviceLocation, path));

    for (final Map.Entry<String, String> entry : headers.entries()) {
      request.addHeader(entry.getKey(), entry.getValue());
    }

    if (contentType != null) {
      request.setContent(contentType, content);
    }

    return request;
  }

  private URL makeURL(final ServiceLocation serviceLocation, final String encodedPathAndQueryString)
  {
    final String scheme;
    final int portToUse;

    if (serviceLocation.getTlsPort() > 0) {
      // Prefer HTTPS if available.
      scheme = "https";
      portToUse = serviceLocation.getTlsPort();
    } else {
      scheme = "http";
      portToUse = serviceLocation.getPlaintextPort();
    }

    // Use URL constructor, not URI, since the path is already encoded.
    try {
      return new URL(scheme, serviceLocation.getHost(), portToUse, encodedPathAndQueryString);
    }
    catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
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
    RequestBuilder that = (RequestBuilder) o;
    return Objects.equals(method, that.method)
           && Objects.equals(encodedPathAndQueryString, that.encodedPathAndQueryString)
           && Objects.equals(headers, that.headers)
           && Objects.equals(contentType, that.contentType)
           && Arrays.equals(content, that.content)
           && Objects.equals(timeout, that.timeout);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(method, encodedPathAndQueryString, headers, contentType, timeout);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }
}
