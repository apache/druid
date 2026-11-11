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
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/**
 * Regression tests for the {@link StringFullResponseHandler} and {@link StatusResponseHandler} decoders:
 * multi-byte characters that straddle a chunk boundary must be reassembled, not turned into replacement
 * characters. The pre-fix behaviour called {@code ByteBuf.toString(charset)} per chunk in isolation.
 */
public class StringDecodingChunkBoundaryTest
{
  /** UTF-8 mix: ASCII, 2-byte (é), 3-byte (世界), 4-byte (😀) code points. */
  private static final String PAYLOAD = "héllo 世界 😀";

  private static final HttpResponseHandler.TrafficCop NOOP_TRAFFIC_COP = new HttpResponseHandler.TrafficCop()
  {
    @Override
    public long resume(long chunkNum)
    {
      return 0;
    }

    @Override
    public void abort()
    {
    }
  };

  @Test
  public void testStringFullResponseHandlerReassemblesSplitCharacters()
  {
    final StringFullResponseHandler handler = new StringFullResponseHandler(StandardCharsets.UTF_8);
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    ClientResponse<StringFullResponseHolder> clientResponse = handler.handleResponse(response, NOOP_TRAFFIC_COP);
    feedByteByByte(PAYLOAD, (bytes, isLast) -> {
      if (isLast) {
        clientResponse.getObj();
        handler.handleChunk(clientResponse, new DefaultLastHttpContent(Unpooled.wrappedBuffer(bytes)), 1);
      } else {
        handler.handleChunk(clientResponse, new DefaultHttpContent(Unpooled.wrappedBuffer(bytes)), 1);
      }
    });
    final ClientResponse<StringFullResponseHolder> finished = handler.done(clientResponse);

    Assertions.assertEquals(PAYLOAD, finished.getObj().getContent());
  }

  @Test
  public void testStatusResponseHandlerReassemblesSplitCharacters()
  {
    final StatusResponseHandler handler = StatusResponseHandler.getInstance();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    ClientResponse<StatusResponseHolder> clientResponse = handler.handleResponse(response, NOOP_TRAFFIC_COP);
    feedByteByByte(PAYLOAD, (bytes, isLast) -> {
      if (isLast) {
        handler.handleChunk(clientResponse, new DefaultLastHttpContent(Unpooled.wrappedBuffer(bytes)), 1);
      } else {
        handler.handleChunk(clientResponse, new DefaultHttpContent(Unpooled.wrappedBuffer(bytes)), 1);
      }
    });
    final ClientResponse<StatusResponseHolder> finished = handler.done(clientResponse);

    Assertions.assertEquals(PAYLOAD, finished.getObj().getContent());
  }

  /**
   * Feed each byte of the UTF-8 encoding as its own chunk, which guarantees that every multi-byte character
   * is split across chunk boundaries. The pre-fix implementation would emit replacement characters for
   * every non-ASCII byte here.
   */
  private static void feedByteByByte(String payload, ChunkFeeder feeder)
  {
    final byte[] utf8 = payload.getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < utf8.length; i++) {
      final byte[] one = new byte[]{utf8[i]};
      feeder.feed(one, i == utf8.length - 1);
    }
  }

  @FunctionalInterface
  private interface ChunkFeeder
  {
    void feed(byte[] bytes, boolean isLast);
  }
}
