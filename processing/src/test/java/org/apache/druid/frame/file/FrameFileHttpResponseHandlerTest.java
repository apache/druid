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

package org.apache.druid.frame.file;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class FrameFileHttpResponseHandlerTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final int maxRowsPerFrame;

  private StorageAdapter adapter;
  private File file;
  private ReadableByteChunksFrameChannel channel;
  private FrameFileHttpResponseHandler handler;

  public FrameFileHttpResponseHandlerTest(final int maxRowsPerFrame)
  {
    this.maxRowsPerFrame = maxRowsPerFrame;
  }

  @Parameterized.Parameters(name = "maxRowsPerFrame = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (int maxRowsPerFrame : new int[]{1, 50, Integer.MAX_VALUE}) {
      constructors.add(new Object[]{maxRowsPerFrame});
    }

    return constructors;
  }

  @Before
  public void setUp() throws IOException
  {
    adapter = new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());
    file = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .maxRowsPerFrame(maxRowsPerFrame)
                            .frameType(FrameType.ROW_BASED) // No particular reason to test with both frame types
                            .frames(),
        temporaryFolder.newFile()
    );

    channel = ReadableByteChunksFrameChannel.create("test");
    handler = new FrameFileHttpResponseHandler(channel);
  }

  @Test
  public void testNonChunkedResponse() throws Exception
  {
    final ClientResponse<FrameFilePartialFetch> response1 = handler.handleResponse(
        makeResponse(HttpResponseStatus.OK, Files.readAllBytes(file.toPath())),
        null
    );

    Assert.assertFalse(response1.isFinished());
    Assert.assertTrue(response1.isContinueReading());
    Assert.assertFalse(response1.getObj().isExceptionCaught());
    Assert.assertFalse(response1.getObj().isLastFetch());

    final ClientResponse<FrameFilePartialFetch> response2 = handler.done(response1);

    Assert.assertTrue(response2.isFinished());
    Assert.assertTrue(response2.isContinueReading());
    Assert.assertFalse(response2.getObj().isExceptionCaught());
    Assert.assertFalse(response2.getObj().isLastFetch());

    final ListenableFuture<?> backpressureFuture = response2.getObj().backpressureFuture();
    Assert.assertFalse(backpressureFuture.isDone());

    channel.doneWriting();

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
    );

    // Backpressure future resolves once channel is read.
    Assert.assertTrue(backpressureFuture.isDone());
  }

  @Test
  public void testEmptyResponseWithoutLastFetchHeader()
  {
    final ClientResponse<FrameFilePartialFetch> response1 = handler.handleResponse(
        makeResponse(HttpResponseStatus.OK, ByteArrays.EMPTY_ARRAY),
        null
    );

    Assert.assertFalse(response1.isFinished());
    Assert.assertTrue(response1.isContinueReading());
    Assert.assertFalse(response1.getObj().isExceptionCaught());
    Assert.assertFalse(response1.getObj().isLastFetch());

    final ClientResponse<FrameFilePartialFetch> response2 = handler.done(response1);

    Assert.assertTrue(response2.isFinished());
    Assert.assertTrue(response2.isContinueReading());
    Assert.assertFalse(response2.getObj().isExceptionCaught());
    Assert.assertFalse(response2.getObj().isLastFetch());
    Assert.assertTrue(response2.getObj().backpressureFuture().isDone());
  }

  @Test
  public void testEmptyResponseWithLastFetchHeader()
  {
    final HttpResponse serverResponse = makeResponse(HttpResponseStatus.OK, ByteArrays.EMPTY_ARRAY);
    serverResponse.headers().set(
        FrameFileHttpResponseHandler.HEADER_LAST_FETCH_NAME,
        FrameFileHttpResponseHandler.HEADER_LAST_FETCH_VALUE
    );

    final ClientResponse<FrameFilePartialFetch> response1 = handler.handleResponse(
        serverResponse,
        null
    );

    Assert.assertFalse(response1.isFinished());
    Assert.assertTrue(response1.isContinueReading());
    Assert.assertFalse(response1.getObj().isExceptionCaught());
    Assert.assertTrue(response1.getObj().isLastFetch());

    final ClientResponse<FrameFilePartialFetch> response2 = handler.done(response1);

    Assert.assertTrue(response2.isFinished());
    Assert.assertTrue(response2.isContinueReading());
    Assert.assertFalse(response2.getObj().isExceptionCaught());
    Assert.assertTrue(response2.getObj().isLastFetch());
    Assert.assertTrue(response2.getObj().backpressureFuture().isDone());
  }

  @Test
  public void testChunkedResponse() throws Exception
  {
    final int chunkSize = 99;
    final byte[] allBytes = Files.readAllBytes(file.toPath());

    ClientResponse<FrameFilePartialFetch> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.OK, byteSlice(allBytes, 0, chunkSize)),
        null
    );

    Assert.assertFalse(response.isFinished());

    for (int p = chunkSize; p < allBytes.length; p += chunkSize) {
      response = handler.handleChunk(
          response,
          makeChunk(byteSlice(allBytes, p, chunkSize)),
          p / chunkSize
      );

      Assert.assertFalse(response.isFinished());
      Assert.assertFalse(response.getObj().isExceptionCaught());
      Assert.assertFalse(response.getObj().isLastFetch());
    }

    final ClientResponse<FrameFilePartialFetch> finalResponse = handler.done(response);

    Assert.assertTrue(finalResponse.isFinished());
    Assert.assertTrue(finalResponse.isContinueReading());
    Assert.assertFalse(response.getObj().isExceptionCaught());
    Assert.assertFalse(response.getObj().isLastFetch());

    final ListenableFuture<?> backpressureFuture = response.getObj().backpressureFuture();
    Assert.assertFalse(backpressureFuture.isDone());

    channel.doneWriting();

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
    );

    // Backpressure future resolves after channel is read.
    Assert.assertTrue(backpressureFuture.isDone());
  }

  @Test
  public void testServerErrorResponse()
  {
    ClientResponse<FrameFilePartialFetch> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, StringUtils.toUtf8("Oh no!")),
        null
    );

    final ClientResponse<FrameFilePartialFetch> finalResponse = handler.done(response);
    Assert.assertTrue(finalResponse.isFinished());
    Assert.assertTrue(finalResponse.isContinueReading());

    // Verify that the exception handler was called.
    Assert.assertTrue(finalResponse.getObj().isExceptionCaught());
    final Throwable e = finalResponse.getObj().getExceptionCaught();
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.equalTo("Server for [test] returned [500 Internal Server Error]")
        )
    );

    // Verify that the channel has not had an error state set. This enables reconnection later.
    Assert.assertFalse(channel.isErrorOrFinished());
  }

  @Test
  public void testChunkedServerErrorResponse()
  {
    ClientResponse<FrameFilePartialFetch> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, StringUtils.toUtf8("Oh ")),
        null
    );

    response = handler.handleChunk(response, makeChunk(StringUtils.toUtf8("no!")), 1);

    final ClientResponse<FrameFilePartialFetch> finalResponse = handler.done(response);
    Assert.assertTrue(finalResponse.isFinished());
    Assert.assertTrue(finalResponse.isContinueReading());

    // Verify that the exception handler was called.
    Assert.assertTrue(finalResponse.getObj().isExceptionCaught());
    final Throwable e = finalResponse.getObj().getExceptionCaught();
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.equalTo("Server for [test] returned [500 Internal Server Error]")
        )
    );

    // Verify that the channel has not had an error state set. This enables reconnection later.
    Assert.assertFalse(channel.isErrorOrFinished());
  }

  @Test
  public void testCaughtExceptionDuringChunkedResponse() throws Exception
  {
    // Split file into 4 quarters.
    final int chunkSize = Ints.checkedCast(LongMath.divide(file.length(), 4, RoundingMode.CEILING));
    final byte[] allBytes = Files.readAllBytes(file.toPath());

    ClientResponse<FrameFilePartialFetch> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.OK, byteSlice(allBytes, 0, chunkSize)),
        null
    );

    Assert.assertFalse(response.isFinished());

    // Add next chunk.
    response = handler.handleChunk(
        response,
        makeChunk(byteSlice(allBytes, chunkSize, chunkSize)),
        1
    );

    // Set an exception.
    handler.exceptionCaught(response, new ISE("Oh no!"));

    // Add another chunk after the exception is caught (this can happen in real life!). We expect it to be ignored.
    handler.handleChunk(
        response,
        makeChunk(byteSlice(allBytes, chunkSize * 2, chunkSize)),
        2
    );

    // Verify that the exception handler was called.
    Assert.assertTrue(response.getObj().isExceptionCaught());
    final Throwable e = response.getObj().getExceptionCaught();
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("Oh no!")));

    // Verify that the channel has not had an error state set.
    Assert.assertFalse(channel.isErrorOrFinished());

    // Simulate successful reconnection with a different handler: add the rest of the data directly to the channel.
    channel.addChunk(byteSlice(allBytes, chunkSize * 2, chunkSize));
    channel.addChunk(byteSlice(allBytes, chunkSize * 3, chunkSize));
    Assert.assertEquals(allBytes.length, channel.getBytesAdded());
    channel.doneWriting();

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
    );
  }

  private static HttpResponse makeResponse(final HttpResponseStatus status, final byte[] content)
  {
    final ByteBufferBackedChannelBuffer channelBuffer = new ByteBufferBackedChannelBuffer(ByteBuffer.wrap(content));

    return new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    {
      @Override
      public ChannelBuffer getContent()
      {
        return channelBuffer;
      }
    };
  }

  private static HttpChunk makeChunk(final byte[] content)
  {
    final ByteBufferBackedChannelBuffer channelBuffer = new ByteBufferBackedChannelBuffer(ByteBuffer.wrap(content));
    return new DefaultHttpChunk(channelBuffer);
  }

  private static byte[] byteSlice(final byte[] bytes, final int start, final int length)
  {
    final int actualLength = Math.min(bytes.length - start, length);
    final byte[] retVal = new byte[actualLength];
    System.arraycopy(bytes, start, retVal, 0, actualLength);
    return retVal;
  }
}
