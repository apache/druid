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

package org.apache.druid.msq.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.ChannelClosedForWritesException;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileHttpResponseHandler;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CloseableUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BaseWorkerClientImplTest extends InitializedNullHandlingTest
{
  private static final String WORKER_ID = "w0";
  /**
   * Bytes for a {@link FrameFile} with no frames. (Not an empty array.)
   */
  private static byte[] NIL_FILE_BYTES;
  /**
   * Bytes for a {@link FrameFile} holding {@link TestIndex#getMMappedTestIndex()}.
   */
  private static byte[] FILE_BYTES;
  private static FrameReader FRAME_READER;

  private ObjectMapper jsonMapper;
  private MockServiceClient workerServiceClient;
  private WorkerClient workerClient;
  private ExecutorService exec;

  @BeforeClass
  public static void setupClass()
  {
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex());

    NIL_FILE_BYTES = toFileBytes(Sequences.empty());
    FILE_BYTES = toFileBytes(
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .frameType(FrameType.COLUMNAR)
                            .maxRowsPerFrame(10)
                            .frames()
    );
    FRAME_READER = FrameReader.create(cursorFactory.getRowSignature());
  }

  @AfterClass
  public static void afterClass()
  {
    NIL_FILE_BYTES = null;
    FILE_BYTES = null;
    FRAME_READER = null;
  }

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    workerServiceClient = new MockServiceClient();
    workerClient = new TestWorkerClient(jsonMapper, workerServiceClient);
    exec = Execs.singleThreaded(StringUtils.encodeForFormat("exec-for-" + getClass().getName()) + "-%s");
  }

  @After
  public void tearDown() throws InterruptedException
  {
    workerServiceClient.verify();
    exec.shutdownNow();
    if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
      throw new ISE("Timed out waiting for exec to finish");
    }
  }

  @Test
  public void test_fetchChannelData_empty() throws Exception
  {
    workerServiceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=0")
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.OK,
        fetchChannelDataResponseHeaders(false),
        NIL_FILE_BYTES
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=" + NIL_FILE_BYTES.length)
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.OK,
        fetchChannelDataResponseHeaders(true),
        ByteArrays.EMPTY_ARRAY
    );

    // Perform the test.
    final StageId stageId = new StageId("xyz", 1);
    final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("testChannel", false);
    final Future<List<List<Object>>> framesFuture = readChannelAsync(channel);

    Assert.assertFalse(workerClient.fetchChannelData(WORKER_ID, stageId, 2, 0, channel).get());
    Assert.assertTrue(workerClient.fetchChannelData(WORKER_ID, stageId, 2, NIL_FILE_BYTES.length, channel).get());
    channel.doneWriting(); // Caller is expected to call doneWriting after fetchChannelData returns true.

    Assert.assertEquals(
        0,
        framesFuture.get().size()
    );
  }

  @Test
  public void test_fetchChannelData_empty_intoClosedChannel()
  {
    workerServiceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=0")
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.OK,
        fetchChannelDataResponseHeaders(false),
        NIL_FILE_BYTES
    );

    // Perform the test.
    final StageId stageId = new StageId("xyz", 1);
    final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("testChannel", false);
    channel.close(); // ReadableFrameChannel's close() method.

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> workerClient.fetchChannelData(WORKER_ID, stageId, 2, 0, channel).get()
    );

    MatcherAssert.assertThat(
        e.getCause(),
        CoreMatchers.instanceOf(ChannelClosedForWritesException.class)
    );
  }

  @Test
  public void test_fetchChannelData_empty_retry500() throws Exception
  {
    workerServiceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=0")
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.INTERNAL_SERVER_ERROR,
        ImmutableMap.of(),
        ByteArrays.EMPTY_ARRAY
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=0")
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.OK,
        fetchChannelDataResponseHeaders(false),
        NIL_FILE_BYTES
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=" + NIL_FILE_BYTES.length)
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.OK,
        fetchChannelDataResponseHeaders(true),
        ByteArrays.EMPTY_ARRAY
    );

    // Perform the test.
    final StageId stageId = new StageId("xyz", 1);
    final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("testChannel", false);
    final Future<List<List<Object>>> framesFuture = readChannelAsync(channel);

    Assert.assertFalse(workerClient.fetchChannelData(WORKER_ID, stageId, 2, 0, channel).get());
    Assert.assertFalse(workerClient.fetchChannelData(WORKER_ID, stageId, 2, 0, channel).get());
    Assert.assertTrue(workerClient.fetchChannelData(WORKER_ID, stageId, 2, NIL_FILE_BYTES.length, channel).get());
    channel.doneWriting(); // Caller is expected to call doneWriting after fetchChannelData returns true.

    Assert.assertEquals(
        0,
        framesFuture.get().size()
    );
  }

  @Test
  public void test_fetchChannelData_empty_serviceClientError()
  {
    workerServiceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=0")
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        new IOException("Some error")
    );

    // Perform the test.
    final StageId stageId = new StageId("xyz", 1);
    final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("testChannel", false);

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> workerClient.fetchChannelData(WORKER_ID, stageId, 2, 0, channel).get()
    );

    MatcherAssert.assertThat(
        e.getCause(),
        CoreMatchers.allOf(
            CoreMatchers.instanceOf(IOException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("Some error"))
        )
    );

    channel.close();
  }

  @Test
  public void test_fetchChannelData_nonEmpty() throws Exception
  {
    workerServiceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=0")
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM),
        FILE_BYTES
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/channels/xyz/1/2?offset=" + FILE_BYTES.length)
            .header(HttpHeaders.ACCEPT_ENCODING, "identity"),
        HttpResponseStatus.OK,
        fetchChannelDataResponseHeaders(true),
        ByteArrays.EMPTY_ARRAY
    );

    // Perform the test.
    final StageId stageId = new StageId("xyz", 1);
    final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("testChannel", false);
    final Future<List<List<Object>>> framesFuture = readChannelAsync(channel);

    Assert.assertFalse(workerClient.fetchChannelData(WORKER_ID, stageId, 2, 0, channel).get());
    Assert.assertTrue(workerClient.fetchChannelData(WORKER_ID, stageId, 2, FILE_BYTES.length, channel).get());
    channel.doneWriting(); // Caller is expected to call doneWriting after fetchChannelData returns true.

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex())),
        Sequences.simple(framesFuture.get())
    );
  }

  private Future<List<List<Object>>> readChannelAsync(final ReadableFrameChannel channel)
  {
    return exec.submit(() -> {
      final List<List<Object>> retVal = new ArrayList<>();
      while (!channel.isFinished()) {
        FutureUtils.getUnchecked(channel.readabilityFuture(), false);

        if (channel.canRead()) {
          final Frame frame = channel.read();
          retVal.addAll(FrameTestUtil.readRowsFromCursorFactory(FRAME_READER.makeCursorFactory(frame)).toList());
        }
      }
      channel.close();
      return retVal;
    });
  }

  /**
   * Returns a frame file (as bytes) from a sequence of frames.
   */
  private static byte[] toFileBytes(final Sequence<Frame> frames)
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final FrameFileWriter writer =
        FrameFileWriter.open(Channels.newChannel(baos), null, ByteTracker.unboundedTracker());
    frames.forEach(frame -> {
      try {
        writer.writeFrame(frame, FrameFileWriter.NO_PARTITION);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    CloseableUtils.closeAndWrapExceptions(writer);
    return baos.toByteArray();
  }


  /**
   * Expected response headers for the "fetch channel data" API.
   */
  private static Map<String, String> fetchChannelDataResponseHeaders(final boolean lastResponse)
  {
    final ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
                    .put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM);

    if (lastResponse) {
      builder.put(
          FrameFileHttpResponseHandler.HEADER_LAST_FETCH_NAME,
          FrameFileHttpResponseHandler.HEADER_LAST_FETCH_VALUE
      );
    }

    return builder.build();
  }

  /**
   * Worker client that communicates with a single worker named {@link #WORKER_ID}.
   */
  private static class TestWorkerClient extends BaseWorkerClientImpl
  {
    private final ServiceClient workerServiceClient;

    public TestWorkerClient(ObjectMapper objectMapper, ServiceClient workerServiceClient)
    {
      super(objectMapper, MediaType.APPLICATION_JSON);
      this.workerServiceClient = workerServiceClient;
    }

    @Override
    protected ServiceClient getClient(String workerId)
    {
      if (WORKER_ID.equals(workerId)) {
        return workerServiceClient;
      } else {
        throw new ISE("Expected workerId[%s], got[%s]", WORKER_ID, workerId);
      }
    }

    @Override
    public void close()
    {
      // Nothing to close.
    }
  }
}
