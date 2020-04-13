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

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

public class URIsTest
{
  @Test
  public void testFullUri()
  {
    final String strUri = "https://test-user@127.0.0.1:8000/test/path?test-query#test-fragment";
    final URI uri = URIs.parse(strUri, "http");

    Assert.assertEquals("https", uri.getScheme());
    Assert.assertEquals("test-user", uri.getUserInfo());
    Assert.assertEquals("127.0.0.1", uri.getHost());
    Assert.assertEquals(8000, uri.getPort());
    Assert.assertEquals("/test/path", uri.getPath());
    Assert.assertEquals("test-query", uri.getQuery());
    Assert.assertEquals("test-fragment", uri.getFragment());
  }

  @Test
  public void testWithoutScheme()
  {
    final String strUri = "test-user@127.0.0.1:8000/test/path?test-query#test-fragment";
    final URI uri = URIs.parse(strUri, "http");

    Assert.assertEquals("http", uri.getScheme());
    Assert.assertEquals("test-user", uri.getUserInfo());
    Assert.assertEquals("127.0.0.1", uri.getHost());
    Assert.assertEquals(8000, uri.getPort());
    Assert.assertEquals("/test/path", uri.getPath());
    Assert.assertEquals("test-query", uri.getQuery());
    Assert.assertEquals("test-fragment", uri.getFragment());
  }

  @Test
  public void testSimpleUri()
  {
    final String strUri = "127.0.0.1:8000";
    final URI uri = URIs.parse(strUri, "https");

    Assert.assertEquals("https", uri.getScheme());
    Assert.assertNull(uri.getUserInfo());
    Assert.assertEquals("127.0.0.1", uri.getHost());
    Assert.assertEquals(8000, uri.getPort());
  }
}
