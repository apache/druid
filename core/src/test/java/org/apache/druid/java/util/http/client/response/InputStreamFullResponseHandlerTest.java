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

package org.apache.druid.java.util.http.client.response;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class InputStreamFullResponseHandlerTest
{
  @Test
  public void testSimple() throws Exception
  {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    InputStreamFullResponseHandler responseHandler = new InputStreamFullResponseHandler();
    ClientResponse<InputStreamFullResponseHolder> clientResp = responseHandler.handleResponse(response, null);

    HttpContent chunk0 = new DefaultHttpContent(Unpooled.wrappedBuffer("abcd".getBytes(StringUtils.UTF8_STRING)));
    clientResp = responseHandler.handleChunk(clientResp, chunk0, 0);

    HttpContent chunk1 = new DefaultHttpContent(Unpooled.wrappedBuffer("efg".getBytes(StringUtils.UTF8_STRING)));
    clientResp = responseHandler.handleChunk(clientResp, chunk1, 1);

    clientResp = responseHandler.done(clientResp);

    Assert.assertTrue(clientResp.isFinished());
    Assert.assertEquals("abcdefg", IOUtils.toString(clientResp.getObj().getContent(), StandardCharsets.UTF_8));
  }

  @Test
  public void testException()
  {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    InputStreamFullResponseHandler responseHandler = new InputStreamFullResponseHandler();
    ClientResponse<InputStreamFullResponseHolder> clientResp = responseHandler.handleResponse(response, null);

    Exception ex = new RuntimeException("dummy!");
    responseHandler.exceptionCaught(clientResp, ex);

    Assert.assertTrue(clientResp.isFinished());
  }
}
