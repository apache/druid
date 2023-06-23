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
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
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
    final ObjectOrErrorResponseHandler<InputStreamFullResponseHolder, InputStreamFullResponseHolder> responseHandler =
        new ObjectOrErrorResponseHandler<>(new InputStreamFullResponseHandler());

    ClientResponse<Either<BytesFullResponseHolder, InputStreamFullResponseHolder>> intermediateResp =
        responseHandler.handleResponse(response, null);

    HttpContent chunk0 = new DefaultHttpContent(Unpooled.wrappedBuffer("abcd".getBytes(StringUtils.UTF8_STRING)));
    HttpContent chunk1 = new DefaultHttpContent(Unpooled.wrappedBuffer("efg".getBytes(StringUtils.UTF8_STRING)));
    intermediateResp = responseHandler.handleChunk(intermediateResp, chunk0, 0);
    intermediateResp = responseHandler.handleChunk(intermediateResp, chunk1, 1);
    ClientResponse<Either<StringFullResponseHolder, InputStreamFullResponseHolder>> clientResp =
        responseHandler.done(intermediateResp);

    Assert.assertTrue(clientResp.isFinished());
    Assert.assertEquals(
        "abcdefg",
        IOUtils.toString(clientResp.getObj().valueOrThrow().getContent(), StandardCharsets.UTF_8)
    );
  }

  @Test
  public void testExceptionAfterOk()
  {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    final ObjectOrErrorResponseHandler<InputStreamFullResponseHolder, InputStreamFullResponseHolder> responseHandler =
        new ObjectOrErrorResponseHandler<>(new InputStreamFullResponseHandler());

    ClientResponse<Either<BytesFullResponseHolder, InputStreamFullResponseHolder>> intermediateResp =
        responseHandler.handleResponse(response, null);

    Exception ex = new RuntimeException("dummy!");
    responseHandler.exceptionCaught(intermediateResp, ex);

    // Exception after HTTP OK still is handled by the "OK handler"
    // (The handler that starts the request gets to finish it.)
    Assert.assertTrue(intermediateResp.isFinished());
    Assert.assertTrue(intermediateResp.getObj().isValue());

    final InputStream responseStream = intermediateResp.getObj().valueOrThrow().getContent();
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

    final ObjectOrErrorResponseHandler<InputStreamFullResponseHolder, InputStreamFullResponseHolder> responseHandler =
        new ObjectOrErrorResponseHandler<>(new InputStreamFullResponseHandler());

    ClientResponse<Either<BytesFullResponseHolder, InputStreamFullResponseHolder>> intermediateResp =
        responseHandler.handleResponse(response, null);

    HttpContent chunk0 = new DefaultHttpContent(Unpooled.wrappedBuffer("abcd".getBytes(StringUtils.UTF8_STRING)));
    HttpContent chunk1 = new DefaultHttpContent(Unpooled.wrappedBuffer("efg".getBytes(StringUtils.UTF8_STRING)));

    intermediateResp = responseHandler.handleChunk(intermediateResp, chunk0, 0);
    intermediateResp = responseHandler.handleChunk(intermediateResp, chunk1, 1);
    ClientResponse<Either<StringFullResponseHolder, InputStreamFullResponseHolder>> clientResp =
        responseHandler.done(intermediateResp);

    // 5xx HTTP code is handled by the error handler.
    Assert.assertTrue(clientResp.isFinished());
    Assert.assertTrue(clientResp.getObj().isError());
    Assert.assertEquals(
        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
        clientResp.getObj().error().getResponse().status().code()
    );
    Assert.assertEquals("abcdefg", clientResp.getObj().error().getContent());
  }
}
