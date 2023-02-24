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

import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ObjectOrErrorResponseHandlerTest
{
  @Test
  public void testOk() throws Exception
  {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.setChunked(false);
    response.setContent(new BigEndianHeapChannelBuffer("abcd".getBytes(StringUtils.UTF8_STRING)));

    final ObjectOrErrorResponseHandler<InputStreamFullResponseHolder, InputStreamFullResponseHolder> responseHandler =
        new ObjectOrErrorResponseHandler<>(new InputStreamFullResponseHandler());

    ClientResponse<Either<StringFullResponseHolder, InputStreamFullResponseHolder>> clientResp =
        responseHandler.handleResponse(response, null);

    DefaultHttpChunk chunk =
        new DefaultHttpChunk(new BigEndianHeapChannelBuffer("efg".getBytes(StringUtils.UTF8_STRING)));
    clientResp = responseHandler.handleChunk(clientResp, chunk, 0);
    clientResp = responseHandler.done(clientResp);

    Assert.assertTrue(clientResp.isFinished());
    Assert.assertEquals(
        "abcdefg",
        IOUtils.toString(clientResp.getObj().valueOrThrow().getContent(), StandardCharsets.UTF_8)
    );
  }

  @Test
  public void testExceptionAfterOk() throws Exception
  {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.setChunked(false);
    response.setContent(new BigEndianHeapChannelBuffer("abcd".getBytes(StringUtils.UTF8_STRING)));

    final ObjectOrErrorResponseHandler<InputStreamFullResponseHolder, InputStreamFullResponseHolder> responseHandler =
        new ObjectOrErrorResponseHandler<>(new InputStreamFullResponseHandler());

    ClientResponse<Either<StringFullResponseHolder, InputStreamFullResponseHolder>> clientResp =
        responseHandler.handleResponse(response, null);

    Exception ex = new RuntimeException("dummy!");
    responseHandler.exceptionCaught(clientResp, ex);

    // Exception after HTTP OK still is handled by the "OK handler"
    // (The handler that starts the request gets to finish it.)
    Assert.assertTrue(clientResp.isFinished());
    Assert.assertTrue(clientResp.getObj().isValue());

    final InputStream responseStream = clientResp.getObj().valueOrThrow().getContent();
    final IOException e = Assert.assertThrows(
        IOException.class,
        () -> IOUtils.toString(responseStream, StandardCharsets.UTF_8)
    );
    Assert.assertEquals("java.lang.RuntimeException: dummy!", e.getMessage());
  }

  @Test
  public void testServerError() throws Exception
  {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    response.setChunked(false);
    response.setContent(new BigEndianHeapChannelBuffer("abcd".getBytes(StringUtils.UTF8_STRING)));

    final ObjectOrErrorResponseHandler<InputStreamFullResponseHolder, InputStreamFullResponseHolder> responseHandler =
        new ObjectOrErrorResponseHandler<>(new InputStreamFullResponseHandler());

    ClientResponse<Either<StringFullResponseHolder, InputStreamFullResponseHolder>> clientResp =
        responseHandler.handleResponse(response, null);

    DefaultHttpChunk chunk =
        new DefaultHttpChunk(new BigEndianHeapChannelBuffer("efg".getBytes(StringUtils.UTF8_STRING)));
    clientResp = responseHandler.handleChunk(clientResp, chunk, 0);
    clientResp = responseHandler.done(clientResp);

    // 5xx HTTP code is handled by the error handler.
    Assert.assertTrue(clientResp.isFinished());
    Assert.assertTrue(clientResp.getObj().isError());
    Assert.assertEquals(
        HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(),
        clientResp.getObj().error().getResponse().getStatus().getCode()
    );
    Assert.assertEquals("abcdefg", clientResp.getObj().error().getContent());
  }
}
