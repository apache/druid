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

package org.apache.druid.java.util.http.client;

import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An HTTP request, which may be sent more than once.
 *
 * The body is held as a plain byte array rather than a Netty {@link io.netty.buffer.ByteBuf} so that a Request has no
 * reference-counted state and therefore needs no release step. Sending does not consume or modify it: the client wraps
 * a fresh outbound buffer around these bytes for each attempt. That is what allows a Request to be resent, as
 * KerberosHttpClient does after a 401.
 */
public class Request
{
  private final HttpMethod method;
  private final URL url;
  private final Multimap<String, String> headers = Multimaps.newListMultimap(
      new HashMap<>(),
      new Supplier<>()
      {
        @Override
        public List<String> get()
        {
          return new ArrayList<>();
        }
      }
  );

  private byte[] content;

  public Request(
      HttpMethod method,
      URL url
  )
  {
    this.method = method;
    this.url = url;
  }

  public HttpMethod getMethod()
  {
    return method;
  }

  public URL getUrl()
  {
    return url;
  }

  public Multimap<String, String> getHeaders()
  {
    return headers;
  }

  public boolean hasContent()
  {
    return content != null;
  }

  /**
   * The request body, or null if there is none. Not copied, and never modified by sending the request.
   */
  public byte[] getContent()
  {
    return content;
  }

  public Request copy()
  {
    Request retVal = new Request(method, url);
    retVal.headers.putAll(this.headers);
    // Shares the body with the original rather than duplicating it, since nothing writes to it in place.
    retVal.content = content;
    return retVal;
  }

  public Request setHeader(String header, String value)
  {
    headers.replaceValues(header, Collections.singletonList(value));
    return this;
  }

  public Request addHeader(String header, String value)
  {
    headers.put(header, value);
    return this;
  }

  public Request addHeaderValues(String header, Iterable<String> value)
  {
    headers.putAll(header, value);
    return this;
  }

  public Request addHeaderValues(Multimap<String, String> inHeaders)
  {
    for (Map.Entry<String, Collection<String>> entry : inHeaders.asMap().entrySet()) {
      this.addHeaderValues(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public Request setContent(byte[] bytes)
  {
    return setContent(null, bytes);
  }

  public Request setContent(String contentType, byte[] bytes)
  {
    return setContent(contentType, bytes, 0, bytes.length);
  }

  public Request setContent(String contentType, byte[] bytes, int offset, int length)
  {
    if (contentType != null) {
      setHeader(HttpHeaderNames.CONTENT_TYPE.toString(), contentType);
    }

    // The whole array is kept as-is; only a partial view has to be extracted.
    this.content = offset == 0 && length == bytes.length
                   ? bytes
                   : Arrays.copyOfRange(bytes, offset, offset + length);

    setHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(length));

    return this;
  }

  public Request setBasicAuthentication(String username, String password)
  {
    setHeader(HttpHeaderNames.AUTHORIZATION.toString(), makeBasicAuthenticationString(username, password));
    return this;
  }

  public static String makeBasicAuthenticationString(String username, String password)
  {
    return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
