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

package org.apache.druid.java.util.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

public class URIsTest
{
  @Test
  public void testFullUri()
  {
    final String strUri = "https://test-user@127.0.0.1:8000/test/path?test-query#test-fragment";
    final URI uri = URIs.parse(strUri, "http");

    Assertions.assertEquals("https", uri.getScheme());
    Assertions.assertEquals("test-user", uri.getUserInfo());
    Assertions.assertEquals("127.0.0.1", uri.getHost());
    Assertions.assertEquals(8000, uri.getPort());
    Assertions.assertEquals("/test/path", uri.getPath());
    Assertions.assertEquals("test-query", uri.getQuery());
    Assertions.assertEquals("test-fragment", uri.getFragment());
  }

  @Test
  public void testWithoutScheme()
  {
    final String strUri = "test-user@127.0.0.1:8000/test/path?test-query#test-fragment";
    final URI uri = URIs.parse(strUri, "http");

    Assertions.assertEquals("http", uri.getScheme());
    Assertions.assertEquals("test-user", uri.getUserInfo());
    Assertions.assertEquals("127.0.0.1", uri.getHost());
    Assertions.assertEquals(8000, uri.getPort());
    Assertions.assertEquals("/test/path", uri.getPath());
    Assertions.assertEquals("test-query", uri.getQuery());
    Assertions.assertEquals("test-fragment", uri.getFragment());
  }

  @Test
  public void testSimpleUri()
  {
    final String strUri = "127.0.0.1:8000";
    final URI uri = URIs.parse(strUri, "https");

    Assertions.assertEquals("https", uri.getScheme());
    Assertions.assertNull(uri.getUserInfo());
    Assertions.assertEquals("127.0.0.1", uri.getHost());
    Assertions.assertEquals(8000, uri.getPort());
  }

  @Test
  public void testToURL() throws MalformedURLException
  {
    final URL url = URIs.toURL("https://127.0.0.1:8000/test/path?test-query");

    Assertions.assertEquals("https", url.getProtocol());
    Assertions.assertEquals("127.0.0.1", url.getHost());
    Assertions.assertEquals(8000, url.getPort());
    Assertions.assertEquals("/test/path", url.getPath());
    Assertions.assertEquals("test-query", url.getQuery());
  }

  @Test
  public void testToURLInvalidUri()
  {
    final MalformedURLException e = Assertions.assertThrows(
        MalformedURLException.class,
        () -> URIs.toURL("http://bad host/path")
    );

    Assertions.assertEquals("Invalid URL [http://bad host/path]", e.getMessage());
    Assertions.assertInstanceOf(IllegalArgumentException.class, e.getCause());
  }

  @Test
  public void testToURLNotAbsolute()
  {
    // A relative URI is valid, but cannot be converted to a URL; new URL(String) also threw MalformedURLException.
    Assertions.assertThrows(MalformedURLException.class, () -> URIs.toURL("/relative/path"));
  }
}
