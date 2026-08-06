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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.segment.TestHelper;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class RequestBuilderTest
{
  @Test
  public void test_constructor_noLeadingSlash()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new RequestBuilder(HttpMethod.GET, "q")
    );

    org.assertj.core.api.Assertions.assertThat(e).hasMessageContaining("Path must start with '/'");
  }

  @Test
  public void test_build_getPlaintext() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.GET, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 8888, -1, ""));

    Assertions.assertEquals(HttpMethod.GET, request.getMethod());
    Assertions.assertEquals(new URI("http://example.com:8888/q").toURL(), request.getUrl());
    Assertions.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assertions.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assertions.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_getTls() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.GET, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assertions.assertEquals(HttpMethod.GET, request.getMethod());
    Assertions.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assertions.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assertions.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assertions.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_getTlsWithBasePath() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.GET, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 9999, 8888, "/base")) /* TLS preferred over plaintext */;

    Assertions.assertEquals(HttpMethod.GET, request.getMethod());
    Assertions.assertEquals(new URI("https://example.com:8888/base/q").toURL(), request.getUrl());
    Assertions.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assertions.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assertions.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_postTlsNoContent() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.POST, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assertions.assertEquals(HttpMethod.POST, request.getMethod());
    Assertions.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assertions.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assertions.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assertions.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_postTlsWithContent() throws Exception
  {
    final String json = "{\"foo\": 3}";
    final Request request = new RequestBuilder(HttpMethod.POST, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .content("application/json", StringUtils.toUtf8(json))
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assertions.assertEquals(HttpMethod.POST, request.getMethod());
    Assertions.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assertions.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assertions.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assertions.assertTrue(request.hasContent());

    // Read and verify content.
    try (final ChannelBufferInputStream inputStream = new ChannelBufferInputStream(request.getContent())) {
      Assertions.assertEquals(
          json,
          StringUtils.fromUtf8(ByteStreams.toByteArray(inputStream))
      );
    }
  }

  @Test
  public void test_build_postTlsWithJsonContent() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.POST, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .jsonContent(TestHelper.makeJsonMapper(), ImmutableMap.of("foo", 3))
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assertions.assertEquals(HttpMethod.POST, request.getMethod());
    Assertions.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assertions.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assertions.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assertions.assertTrue(request.hasContent());

    // Read and verify content.
    try (final ChannelBufferInputStream inputStream = new ChannelBufferInputStream(request.getContent())) {
      Assertions.assertEquals(
          "{\"foo\":3}",
          StringUtils.fromUtf8(ByteStreams.toByteArray(inputStream))
      );
    }
  }

  @Test
  public void test_timeout()
  {
    Assertions.assertEquals(RequestBuilder.DEFAULT_TIMEOUT, new RequestBuilder(HttpMethod.GET, "/q").getTimeout());
    Assertions.assertEquals(
        Duration.standardSeconds(1),
        new RequestBuilder(HttpMethod.GET, "/q").timeout(Duration.standardSeconds(1)).getTimeout()
    );
    Assertions.assertEquals(
        Duration.ZERO,
        new RequestBuilder(HttpMethod.GET, "/q").timeout(Duration.ZERO).getTimeout()
    );
  }
}
