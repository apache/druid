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
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
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
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.http.SegmentLoadingCapabilities;
import org.apache.druid.server.http.SegmentLoadingMode;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
  private SegmentLoadingCapabilities segmentLoadingCapabilities;

  @Before
  public void setUp()
  {
    segmentLoadingCapabilities = new SegmentLoadingCapabilities(1, 3);
    httpClient = new TestHttpClient();
    httpLoadQueuePeon = new HttpLoadQueuePeon(
        "http://dummy:4000",
        MAPPER,
        httpClient,
        new HttpLoadQueuePeonConfig(null, null, 10),
        () -> SegmentLoadingMode.NORMAL,
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
  public void testAcknowledgedLoadStaysInQueueUntilInventoryConfirmsIt()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.loadSegment(segment, SegmentAction.LOAD, null);

    httpClient.sendRequestToServerAndHandleResponse();

    // Until the inventory view catches up the load must still read as pending, or the replica is briefly neither
    // loaded nor loading. See apache/druid#18764.
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsToLoad().isEmpty());
    Assert.assertEquals(1, httpLoadQueuePeon.getNumSegmentsAwaitingConfirmation());
    Assert.assertEquals(
        Set.of(segment),
        httpLoadQueuePeon.getSegmentsInQueue().stream()
                         .map(SegmentHolder::getSegment)
                         .collect(Collectors.toSet())
    );

    // An inventory without the segment yet keeps the load pending.
    Assert.assertEquals(1, httpLoadQueuePeon.getQueueSnapshot(emptyInventory()).getSegmentsInQueue().size());
    Assert.assertEquals(1, httpLoadQueuePeon.getNumSegmentsAwaitingConfirmation());

    // An inventory that has caught up retires it.
    Assert.assertTrue(httpLoadQueuePeon.getQueueSnapshot(inventoryWith(segment)).getSegmentsInQueue().isEmpty());
    Assert.assertEquals(0, httpLoadQueuePeon.getNumSegmentsAwaitingConfirmation());
  }

  @Test
  public void testAcknowledgedDropStaysInQueueUntilInventoryConfirmsIt()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.dropSegment(segment, null);

    httpClient.sendRequestToServerAndHandleResponse();

    Assert.assertTrue(httpLoadQueuePeon.getSegmentsToDrop().isEmpty());
    Assert.assertEquals(1, httpLoadQueuePeon.getNumSegmentsAwaitingConfirmation());

    // An inventory still showing the segment loaded is the pre-drop state.
    httpLoadQueuePeon.getQueueSnapshot(inventoryWith(segment));
    Assert.assertEquals(1, httpLoadQueuePeon.getNumSegmentsAwaitingConfirmation());

    httpLoadQueuePeon.getQueueSnapshot(emptyInventory());
    Assert.assertEquals(0, httpLoadQueuePeon.getNumSegmentsAwaitingConfirmation());
  }

  /**
   * An inventory snapshot for this peon's server, standing in for a segment sync that has or has not caught up.
   */
  private static ImmutableDruidServer inventoryWith(DataSegment... loadedSegments)
  {
    final DruidServer server = new DruidServer(
        "dummy", "dummy:4000", null, 10L << 30, null, ServerType.HISTORICAL, "tier1", 0
    );
    for (DataSegment segment : loadedSegments) {
      server.addDataSegment(segment);
    }
    return server.toImmutableDruidServer();
  }

  private static ImmutableDruidServer emptyInventory()
  {
    return inventoryWith();
  }

  @Test
  public void testFailedRequestIsNotRetainedForConfirmation()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.loadSegment(segment, SegmentAction.LOAD, null);

    httpClient.failedSegments.add(segment);
    httpClient.sendRequestToServerAndHandleResponse();

    // A failed request changed nothing on the server, so there is no inventory update to wait for.
    Assert.assertEquals(0, httpLoadQueuePeon.getNumSegmentsAwaitingConfirmation());
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsInQueue().isEmpty());
  }

  @Test
  public void testMarkToDropGraduatesToQueuedDrop()
  {
    final DataSegment segment = segments.get(0);
    httpLoadQueuePeon.markSegmentToDrop(segment);
    Assert.assertEquals(Set.of(segment), httpLoadQueuePeon.getSegmentsMarkedToDrop());

    httpLoadQueuePeon.dropSegment(segment, null);

    // Queueing the drop retires the mark, so callers need not unmark separately. See apache/druid#18764.
    Assert.assertTrue(httpLoadQueuePeon.getSegmentsMarkedToDrop().isEmpty());
    Assert.assertEquals(Set.of(segment), httpLoadQueuePeon.getSegmentsToDrop());
  }

  @Test
  public void testSegmentIsNeverAbsentFromBothHalvesOfQueueSnapshotDuringMoveCompletion()
      throws InterruptedException
  {
    // Graduating a MOVE_FROM mark to a queued DROP must be atomic: a reader catching the moment in between sees the
    // segment in neither collection and counts the replica as plainly loaded. See apache/druid#18764.
    final DataSegment segment = segments.get(0);

    // A seqlock over the window in which the invariant must hold: odd once the mark is in place, even again before
    // teardown. A reader whose snapshot spans an unchanged odd generation knows it read entirely inside the window.
    final AtomicLong generation = new AtomicLong();
    final AtomicBoolean sawSegmentInNeither = new AtomicBoolean(false);
    final AtomicBoolean stopReading = new AtomicBoolean(false);
    final AtomicLong reads = new AtomicLong();

    final Thread reader = new Thread(() -> {
      while (!stopReading.get() && !sawSegmentInNeither.get()) {
        final long before = generation.get();
        if (before % 2 == 0) {
          continue;
        }

        final LoadQueueSnapshot snapshot = httpLoadQueuePeon.getQueueSnapshot();
        if (generation.get() != before) {
          // The cycle ended mid-snapshot, so the segment is legitimately allowed to be absent.
          continue;
        }

        reads.incrementAndGet();
        final boolean queued = snapshot.getSegmentsInQueue().stream()
                                       .anyMatch(holder -> holder.getSegment().equals(segment));
        if (!queued && !snapshot.getSegmentsMarkedToDrop().contains(segment)) {
          sawSegmentInNeither.set(true);
        }
      }
    });

    reader.start();
    try {
      for (int i = 0; i < 20_000 && !sawSegmentInNeither.get(); ++i) {
        httpLoadQueuePeon.markSegmentToDrop(segment);
        generation.incrementAndGet();

        httpLoadQueuePeon.dropSegment(segment, null);

        // Reset for the next cycle, outside the odd generation so the reader ignores it.
        generation.incrementAndGet();
        httpLoadQueuePeon.cancelOperation(segment);
      }
    }
    finally {
      stopReading.set(true);
      reader.join(30_000);
    }

    Assert.assertTrue("Reader never sampled the window", reads.get() > 0);
    Assert.assertFalse(
        "Snapshot observed the segment neither queued nor marked to drop",
        sawSegmentInNeither.get()
    );
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

  @Test
  public void testBatchSize()
  {
    Assert.assertEquals(10, httpLoadQueuePeon.calculateBatchSize(SegmentLoadingMode.NORMAL));

    // Without a batch size runtime parameter
    httpLoadQueuePeon = new HttpLoadQueuePeon(
        "http://dummy:4000",
        MAPPER,
        httpClient,
        new HttpLoadQueuePeonConfig(null, null, null),
        () -> SegmentLoadingMode.NORMAL,
        new WrappingScheduledExecutorService(
            "HttpLoadQueuePeonTest-%s",
            httpClient.processingExecutor,
            true
        ),
        httpClient.callbackExecutor
    );

    Assert.assertEquals(1, httpLoadQueuePeon.calculateBatchSize(SegmentLoadingMode.NORMAL));
    Assert.assertEquals(3, httpLoadQueuePeon.calculateBatchSize(SegmentLoadingMode.TURBO));
  }

  private LoadPeonCallback markSegmentProcessed(DataSegment segment)
  {
    return success -> httpClient.processedSegments.add(segment);
  }

  private class TestHttpClient implements HttpClient, DataSegmentChangeHandler
  {
    final BlockingExecutorService processingExecutor = new BlockingExecutorService("HttpLoadQueuePeonTest-%s");
    final BlockingExecutorService callbackExecutor = new BlockingExecutorService("HttpLoadQueuePeonTest-cb");
    final List<DataSegment> processedSegments = new ArrayList<>();
    final List<DataSegment> segmentsSentToServer = new ArrayList<>();

    /** Segments for which the simulated server should report a failed change request. */
    final Set<DataSegment> failedSegments = new HashSet<>();

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler
    )
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    @SuppressWarnings("unchecked")
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
        if (request.getUrl().toString().contains("/loadCapabilities")) {
          return (ListenableFuture<Final>) Futures.immediateFuture(
              new ByteArrayInputStream(
                  MAPPER.writerFor(SegmentLoadingCapabilities.class)
                        .writeValueAsBytes(segmentLoadingCapabilities)
              )
          );
        }

        List<DataSegmentChangeRequest> changeRequests = MAPPER.readValue(
            request.getContent().array(),
            HttpLoadQueuePeon.REQUEST_ENTITY_TYPE_REF
        );

        List<DataSegmentChangeResponse> statuses = new ArrayList<>(changeRequests.size());
        for (DataSegmentChangeRequest cr : changeRequests) {
          final int numSentBefore = segmentsSentToServer.size();
          cr.go(this, null);

          // The handler callbacks above reveal which segment the request was for.
          final boolean failed = segmentsSentToServer.size() > numSentBefore
                                 && failedSegments.contains(segmentsSentToServer.get(numSentBefore));
          statuses.add(
              new DataSegmentChangeResponse(
                  cr,
                  failed ? SegmentChangeStatus.failed("simulated failure") : SegmentChangeStatus.success()
              )
          );
        }
        return (ListenableFuture<Final>) Futures.immediateFuture(
            new ByteArrayInputStream(
                MAPPER
                    .writerFor(HttpLoadQueuePeon.RESPONSE_ENTITY_TYPE_REF)
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
