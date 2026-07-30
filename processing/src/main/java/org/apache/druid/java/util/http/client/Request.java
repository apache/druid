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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
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

  private ByteBuf content;

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

  public ByteBuf getContent()
  {
    return content;
  }

  public Request copy()
  {
    Request retVal = new Request(method, url);
    retVal.headers.putAll(this.headers);
    retVal.content = content == null ? null : content.copy();
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

  public Request setContent(ByteBuf content)
  {
    return setContent(null, content);
  }

  public Request setContent(String contentType, byte[] bytes)
  {
    return setContent(contentType, bytes, 0, bytes.length);
  }

  public Request setContent(String contentType, byte[] bytes, int offset, int length)
  {
    return setContent(contentType, Unpooled.wrappedBuffer(bytes, offset, length));
  }

  public Request setContent(String contentType, ByteBuf content)
  {
    if (contentType != null) {
      setHeader(HttpHeaderNames.CONTENT_TYPE.toString(), contentType);
    }

    this.content = content;

    setHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(content.writerIndex()));

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
