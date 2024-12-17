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

package org.apache.druid.server.coordinator.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.DataSegmentChangeCallback;
import org.apache.druid.server.coordination.DataSegmentChangeHandler;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.DataSegmentChangeResponse;
import org.apache.druid.server.coordination.SegmentChangeStatus;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HttpLoadQueuePeonTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private final List<DataSegment> segments =
      CreateDataSegments.ofDatasource("test")
                        .forIntervals(1, Granularities.DAY)
                        .startingAt("2022-01-01")
                        .withNumPartitions(4)
                        .eachOfSizeInMb(100);

  private TestHttpClient httpClient;
  private HttpLoadQueuePeon httpLoadQueuePeon;

  @Before
  public void setUp()
  {
    httpClient = new TestHttpClient();
    httpLoadQueuePeon = new HttpLoadQueuePeon(
        "http://dummy:4000",
        MAPPER,
        httpClient,
        new HttpLoadQueuePeonConfig(null, null, 10),
        new WrappingScheduledExecutorService(
            "HttpLoadQueuePeonTest-%s",
            httpClient.processingExecutor,
            true
        ),
        httpClient.callbackExecutor
    );
    httpLoadQueuePeon.start();
  }

  @After
  public void tearDown()
  {
    httpLoadQueuePeon.stop();
  }

  @Test
  public void testSimple()
  {
    httpLoadQueuePeon
        .dropSegment(segments.get(0), markSegmentProcessed(segments.get(0)));
    httpLoadQueuePeon
        .loadSegment(segments.get(1), SegmentAction.LOAD, markSegmentProcessed(segments.get(1)));
    httpLoadQueuePeon
        .loadSegment(segments.get(2), SegmentAction.REPLICATE, markSegmentProcessed(segments.get(2)));
    httpLoadQueuePeon
        .loadSegment(segments.get(3), SegmentAction.MOVE_TO, markSegmentProcessed(segments.get(3)));

    httpClient.sendRequestToServerAndHandleResponse();
    Assert.assertEquals(segments, httpClient.segmentsSentToServer);

    // Verify that all callbacks are executed
    httpClient.executeCallbacks();
    Assert.assertEquals(segments, httpClient.processedSegments);
  }

  @Test
  public void testLoadDropAfterStop()
  {
    // Verify that requests sent after stopping the peon fail immediately
    httpLoadQueuePeon.stop();

    final Set<DataSegment> failedSegments = new HashSet<>();
    final DataSegment segment1 = segments.get(0);
    httpLoadQueuePeon.dropSegment(segment1, success -> {
      if (!success) {
        failedSegments.add(segment1);
      }
    });
    final DataSegment segment2 = segments.get(1);
    httpLoadQueuePeon.loadSegment(segment2, SegmentAction.MOVE_TO, success -> {
      if (!success) {
        failedSegments.add(segment2);
      }
    });

    Assert.assertTrue(failedSegments.contains(segment1));
    Assert.assertTrue(failedSegments.contains(segment2));
  }

  @Test
  public void testPriorityOfSegmentAction()
  {
    // Shuffle the segments for the same day
    final List<DataSegment> segmentsDay1 = new ArrayList<>(segments);
    Collections.shuffle(segmentsDay1);

    // Assign segments to the actions in their order of priority
    // Order: drop, load, replicate, move
    final List<QueueAction> actions = Arrays.asList(
        QueueAction.of(segmentsDay1.get(0), s -> httpLoadQueuePeon.dropSegment(s, null)),
        QueueAction.of(segmentsDay1.get(1), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.LOAD, null)),
        QueueAction.of(segmentsDay1.get(2), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.REPLICATE, null)),
        QueueAction.of(segmentsDay1.get(3), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.MOVE_TO, null))
    );

    // Queue the actions on the peon in a random order
    Collections.shuffle(actions);
    actions.forEach(QueueAction::invoke);

    httpClient.sendRequestToServerAndHandleResponse();

    // Verify that all segments are sent to the server in the expected order
    Assert.assertEquals(segmentsDay1, httpClient.segmentsSentToServer);
  }

  @Test
  public void testPriorityOfSegmentInterval()
  {
    // Create 8 segments (4 x 2days) and shuffle them
    final List<DataSegment> segmentsDay1 = new ArrayList<>(segments);
    Collections.shuffle(segmentsDay1);

    final List<DataSegment> segmentsDay2 = new ArrayList<>(
        CreateDataSegments.ofDatasource("test")
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-02")
                          .withNumPartitions(4)
                          .eachOfSizeInMb(100)
    );
    Collections.shuffle(segmentsDay2);

    // Assign segments to the actions in their order of priority
    // Order: action (drop, priorityLoad, etc.), then interval (new then old)
    List<QueueAction> actions = Arrays.asList(
        QueueAction.of(segmentsDay2.get(0), s -> httpLoadQueuePeon.dropSegment(s, null)),
        QueueAction.of(segmentsDay1.get(0), s -> httpLoadQueuePeon.dropSegment(s, null)),
        QueueAction.of(segmentsDay2.get(1), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.LOAD, null)),
        QueueAction.of(segmentsDay1.get(1), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.LOAD, null)),
        QueueAction.of(segmentsDay2.get(2), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.REPLICATE, null)),
        QueueAction.of(segmentsDay1.get(2), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.REPLICATE, null)),
        QueueAction.of(segmentsDay2.get(3), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.MOVE_TO, null)),
        QueueAction.of(segmentsDay1.get(3), s -> httpLoadQueuePeon.loadSegment(s, SegmentAction.MOVE_TO, null))
    );
    final List<DataSegment> expectedSegmentOrder =
        actions.stream().map(a -> a.segment).collect(Collectors.toList());

    // Queue the actions on the peon in a random order
    Collections.shuffle(actions);
    actions.forEach(QueueAction::invoke);

    httpClient.sendRequestToServerAndHandleResponse();

    // Verify that all segments are sent to the server in the expected order
    Assert.assertEquals(expectedSegmentOrder, httpClient.segmentsSentToServer);
  }

  @Test
  public void testCancelLoad()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.loadSegment(segment, SegmentAction.REPLICATE, markSegmentProcessed(segment));
    Assert.assertEquals(1, httpLoadQueuePeon.getSegmentsToLoad().size());

    boolean cancelled = httpLoadQueuePeon.cancelOperation(segment);
    Assert.assertTrue(cancelled);
    Assert.assertEquals(0, httpLoadQueuePeon.getSegmentsToLoad().size());

    Assert.assertTrue(httpClient.processedSegments.isEmpty());
  }

  @Test
  public void testCancelDrop()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.dropSegment(segment, markSegmentProcessed(segment));
    Assert.assertEquals(1, httpLoadQueuePeon.getSegmentsToDrop().size());

    boolean cancelled = httpLoadQueuePeon.cancelOperation(segment);
    Assert.assertTrue(cancelled);
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsToDrop().isEmpty());

    Assert.assertTrue(httpClient.processedSegments.isEmpty());
  }

  @Test
  public void testCannotCancelRequestSentToServer()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.loadSegment(segment, SegmentAction.REPLICATE, markSegmentProcessed(segment));
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsToLoad().contains(segment));

    httpClient.sendRequestToServer();
    Assert.assertTrue(httpClient.segmentsSentToServer.contains(segment));

    // Segment is still in queue but operation cannot be cancelled
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsToLoad().contains(segment));
    boolean cancelled = httpLoadQueuePeon.cancelOperation(segment);
    Assert.assertFalse(cancelled);

    httpClient.handleResponseFromServer();

    // Segment has been removed from queue
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsToLoad().isEmpty());
    cancelled = httpLoadQueuePeon.cancelOperation(segment);
    Assert.assertFalse(cancelled);

    // Execute callbacks and verify segment is fully processed
    httpClient.executeCallbacks();
    Assert.assertTrue(httpClient.processedSegments.contains(segment));
  }

  @Test
  public void testCannotCancelOperationMultipleTimes()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.loadSegment(segment, SegmentAction.REPLICATE, markSegmentProcessed(segment));
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsToLoad().contains(segment));

    Assert.assertTrue(httpLoadQueuePeon.cancelOperation(segment));
    Assert.assertFalse(httpLoadQueuePeon.cancelOperation(segment));
  }

  @Test
  public void testLoadRateIsZeroWhenNoLoadHasFinishedYet()
  {
    httpLoadQueuePeon.loadSegment(segments.get(0), SegmentAction.LOAD, null);
    httpClient.sendRequestToServer();
    Assert.assertEquals(1, httpLoadQueuePeon.getSegmentsToLoad().size());
    Assert.assertEquals(0, httpLoadQueuePeon.getLoadRateKbps());
  }

  @Test
  public void testLoadRateIsUnchangedByDrops() throws InterruptedException
  {
    // Drop a segment after a small delay
    final long millisTakenToDropSegment = 10;
    httpLoadQueuePeon.dropSegment(segments.get(0), null);
    httpClient.sendRequestToServer();
    Thread.sleep(millisTakenToDropSegment);
    httpClient.handleResponseFromServer();

    // Verify that load rate is still zero
    Assert.assertEquals(0, httpLoadQueuePeon.getLoadRateKbps());
  }

  @Test
  public void testLoadRateIsChangedWhenLoadSucceeds() throws InterruptedException
  {
    // Load a segment after a small delay
    final long millisTakenToLoadSegment = 10;
    httpLoadQueuePeon.loadSegment(segments.get(0), SegmentAction.LOAD, null);
    httpClient.sendRequestToServer();
    Thread.sleep(millisTakenToLoadSegment);
    httpClient.handleResponseFromServer();

    // Verify that load rate has been updated
    long expectedRateKbps = (8 * segments.get(0).getSize()) / millisTakenToLoadSegment;
    long observedRateKbps = httpLoadQueuePeon.getLoadRateKbps();
    Assert.assertTrue(
        observedRateKbps > expectedRateKbps / 2
        && observedRateKbps <= expectedRateKbps
    );
  }

  private LoadPeonCallback markSegmentProcessed(DataSegment segment)
  {
    return success -> httpClient.processedSegments.add(segment);
  }

  private static class TestHttpClient implements HttpClient, DataSegmentChangeHandler
  {
    final BlockingExecutorService processingExecutor = new BlockingExecutorService("HttpLoadQueuePeonTest-%s");
    final BlockingExecutorService callbackExecutor = new BlockingExecutorService("HttpLoadQueuePeonTest-cb");
    final List<DataSegment> processedSegments = new ArrayList<>();
    final List<DataSegment> segmentsSentToServer = new ArrayList<>();

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler
    )
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler,
        Duration duration
    )
    {
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      httpResponse.setContent(ChannelBuffers.buffer(0));
      httpResponseHandler.handleResponse(httpResponse, null);
      try {
        List<DataSegmentChangeRequest> changeRequests = MAPPER.readValue(
            request.getContent().array(),
            HttpLoadQueuePeon.REQUEST_ENTITY_TYPE_REF
        );

        List<DataSegmentChangeResponse> statuses = new ArrayList<>(changeRequests.size());
        for (DataSegmentChangeRequest cr : changeRequests) {
          cr.go(this, null);
          statuses.add(
              new DataSegmentChangeResponse(cr, SegmentChangeStatus.SUCCESS)
          );
        }
        return (ListenableFuture<Final>) Futures.immediateFuture(
            new ByteArrayInputStream(
                MAPPER
                    .writerWithType(HttpLoadQueuePeon.RESPONSE_ENTITY_TYPE_REF)
                    .writeValueAsBytes(statuses)
            )
        );
      }
      catch (Exception ex) {
        throw new RE(ex, "Unexpected exception.");
      }
    }

    @Override
    public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
    {
      segmentsSentToServer.add(segment);
    }

    @Override
    public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
    {
      segmentsSentToServer.add(segment);
    }

    void sendRequestToServerAndHandleResponse()
    {
      sendRequestToServer();
      handleResponseFromServer();
    }

    void sendRequestToServer()
    {
      processingExecutor.finishNextPendingTask();
    }

    void handleResponseFromServer()
    {
      processingExecutor.finishAllPendingTasks();
    }

    void executeCallbacks()
    {
      callbackExecutor.finishAllPendingTasks();
    }
  }

  /**
   * Represents an action that can be performed on a segment by calling {@link #invoke()}.
   */
  private static class QueueAction
  {
    final DataSegment segment;
    final Consumer<DataSegment> action;

    static QueueAction of(DataSegment segment, Consumer<DataSegment> action)
    {
      return new QueueAction(segment, action);
    }

    QueueAction(DataSegment segment, Consumer<DataSegment> action)
    {
      this.segment = segment;
      this.action = action;
    }

    void invoke()
    {
      action.accept(segment);
    }
  }
}
