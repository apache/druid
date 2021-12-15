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
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class StatusResponseHandlerTest
{
  @Test
  public void testUtf8CharacterSequenceStraddlingHttpChunks() throws Exception
  {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    StatusResponseHandler responseHandler = StatusResponseHandler.getInstance();
    ClientResponse<BytesFullResponseHolder> intermediateResp = responseHandler.handleResponse(response, null);

    final byte[] bytes = "Россия 한국 中国".getBytes(StringUtils.UTF8_STRING);
    final byte[] chunk0 = Arrays.copyOfRange(bytes, 0, 9);
    final byte[] chunk1 = Arrays.copyOfRange(bytes, 9, bytes.length);

    // byte chunks should split such that a utf-8 character sequence straddles both chunks,
    // and concatenation of strings yields incorrect results
    Assert.assertNotEquals("Россия 한국 中国", StringUtils.fromUtf8(chunk0) + StringUtils.fromUtf8(chunk1));

    intermediateResp = responseHandler.handleChunk(intermediateResp, new DefaultHttpContent(Unpooled.wrappedBuffer(chunk0)), 0);
    intermediateResp = responseHandler.handleChunk(intermediateResp, new DefaultHttpContent(Unpooled.wrappedBuffer(chunk1)), 1);
    ClientResponse<StatusResponseHolder> clientResp = responseHandler.done(intermediateResp);

    Assert.assertEquals("Россия 한국 中国", clientResp.getObj().getContent());
  }
}
