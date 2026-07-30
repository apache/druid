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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class SequenceInputStreamResponseHandlerTest
{
  private static final int TOTAL_BYTES = 1 << 10;
  private static final ArrayList<byte[]> BYTE_LIST = new ArrayList<>();
  private static final Random RANDOM = new Random(378134789L);
  private static byte[] allBytes = new byte[TOTAL_BYTES];

  @BeforeAll
  public static void setUp()
  {
    final ByteBuffer buffer = ByteBuffer.wrap(allBytes);
    while (buffer.hasRemaining()) {
      final byte[] bytes = new byte[Math.min(RANDOM.nextInt(128), buffer.remaining())];
      RANDOM.nextBytes(bytes);
      buffer.put(bytes);
      BYTE_LIST.add(bytes);
    }
  }

  @AfterAll
  public static void tearDown()
  {
    BYTE_LIST.clear();
    allBytes = null;
  }

  private static void fillBuff(InputStream inputStream, byte[] buff) throws IOException
  {
    int off = 0;
    while (off < buff.length) {
      final int read = inputStream.read(buff, off, buff.length - off);
      if (read < 0) {
        throw new IOException("Unexpected end of stream");
      }
      off += read;
    }
  }

  @Test
  public void testExceptionalChunkedStream() throws IOException
  {
    Iterator<byte[]> it = BYTE_LIST.iterator();

    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response, null);
    long chunkNum = 0;
    // Feed a few chunks normally, then simulate a read failure via exceptionCaught (as NettyHttpClient
    // would do when it observes a read error). This replaces the old approach of injecting a malicious
    // ByteBuf, which Netty 4's final UnpooledHeapByteBuf doesn't allow.
    int chunksBeforeFailure = 3;
    while (it.hasNext() && chunkNum < chunksBeforeFailure) {
      final DefaultHttpContent chunk = new DefaultHttpContent(Unpooled.wrappedBuffer(it.next()));
      clientResponse = responseHandler.handleChunk(clientResponse, chunk, ++chunkNum);
    }
    responseHandler.exceptionCaught(clientResponse, new TesterException());

    final InputStream stream = clientResponse.getObj();
    final byte[] buff = new byte[allBytes.length];
    Assertions.assertThrows(IOException.class, () -> fillBuff(stream, buff));
  }

  public static class TesterException extends RuntimeException
  {
  }

  @Test
  public void simpleMultiStreamTest() throws IOException
  {
    Iterator<byte[]> it = BYTE_LIST.iterator();

    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response, null);
    long chunkNum = 0;
    while (it.hasNext()) {
      final DefaultHttpContent chunk = new DefaultHttpContent(Unpooled.wrappedBuffer(it.next()));
      clientResponse = responseHandler.handleChunk(clientResponse, chunk, ++chunkNum);
    }
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final InputStream expectedStream = new ByteArrayInputStream(allBytes);
    int read = 0;
    while (read < allBytes.length) {
      final byte[] expectedBytes = new byte[Math.min(RANDOM.nextInt(128), allBytes.length - read)];
      final byte[] actualBytes = new byte[expectedBytes.length];
      fillBuff(stream, actualBytes);
      fillBuff(expectedStream, expectedBytes);
      Assertions.assertArrayEquals(expectedBytes, actualBytes);
      read += expectedBytes.length;
    }
    Assertions.assertEquals(allBytes.length, responseHandler.getByteCount());
  }


  @Test
  public void alignedMultiStreamTest() throws IOException
  {
    Iterator<byte[]> it = BYTE_LIST.iterator();

    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response, null);
    long chunkNum = 0;
    while (it.hasNext()) {
      final DefaultHttpContent chunk = new DefaultHttpContent(Unpooled.wrappedBuffer(it.next()));
      clientResponse = responseHandler.handleChunk(clientResponse, chunk, ++chunkNum);
    }
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final InputStream expectedStream = new ByteArrayInputStream(allBytes);

    for (byte[] bytes : BYTE_LIST) {
      final byte[] expectedBytes = new byte[bytes.length];
      final byte[] actualBytes = new byte[expectedBytes.length];
      fillBuff(stream, actualBytes);
      fillBuff(expectedStream, expectedBytes);
      Assertions.assertArrayEquals(expectedBytes, actualBytes);
      Assertions.assertArrayEquals(expectedBytes, bytes);
    }
    Assertions.assertEquals(allBytes.length, responseHandler.getByteCount());
  }

  @Test
  public void simpleSingleStreamTest() throws IOException
  {
    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response, null);
    final DefaultHttpContent chunk = new DefaultHttpContent(Unpooled.wrappedBuffer(allBytes));
    clientResponse = responseHandler.handleChunk(clientResponse, chunk, 1);
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final InputStream expectedStream = new ByteArrayInputStream(allBytes);
    int read = 0;
    while (read < allBytes.length) {
      final byte[] expectedBytes = new byte[Math.min(RANDOM.nextInt(128), allBytes.length - read)];
      final byte[] actualBytes = new byte[expectedBytes.length];
      fillBuff(stream, actualBytes);
      fillBuff(expectedStream, expectedBytes);
      Assertions.assertArrayEquals(expectedBytes, actualBytes);
      read += expectedBytes.length;
    }
    Assertions.assertEquals(allBytes.length, responseHandler.getByteCount());
  }

}
