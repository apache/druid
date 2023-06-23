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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

public class RequestTest
{
  @Test
  public void testWithUrl() throws Exception
  {
    Request req = new Request(HttpMethod.GET, new URL("https://apache.org"));
    req.addHeader("some", "header");
    req.setContent(new byte[]{1, 2, 3});

    Request newReq = req.withUrl(new URL("https://druid.apache.org"));

    Assert.assertFalse(newReq.getHeaders().get(HttpHeaderNames.CONTENT_LENGTH.toString()).isEmpty());
    Assert.assertEquals(req.getHeaders(), newReq.getHeaders());
    Assert.assertEquals(new URL("https://druid.apache.org"), newReq.getUrl());
    // 1 reference held by req
    // +1 reference held by newReq
    // +1 reference for every call to getContent
    Assert.assertEquals(3, newReq.getContent().refCnt());
    Assert.assertEquals(4, req.getContent().refCnt());
    Assert.assertEquals(req.getContent(), newReq.getContent());
  }

  @Test
  public void testWithUrlNoContent() throws Exception
  {
    Request req = new Request(HttpMethod.GET, new URL("https://apache.org"));
    req.addHeader("some", "header");

    Request newReq = req.withUrl(new URL("https://druid.apache.org"));

    Assert.assertTrue(newReq.getHeaders().get(HttpHeaderNames.CONTENT_LENGTH.toString()).isEmpty());
    Assert.assertEquals(req.getHeaders(), newReq.getHeaders());
    Assert.assertEquals(new URL("https://druid.apache.org"), newReq.getUrl());
    Assert.assertFalse(newReq.hasContent());
  }
}
