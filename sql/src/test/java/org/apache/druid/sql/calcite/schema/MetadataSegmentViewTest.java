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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.metrics.NoopTaskHolder;
import org.apache.druid.metadata.segment.cache.Metric;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.server.metrics.LatchableEmitterConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MetadataSegmentViewTest
{
  private MetadataSegmentView segmentView;
  private LatchableEmitter emitter;
  private CoordinatorClient coordinatorClient;

  @BeforeEach
  public void setup()
  {
    coordinatorClient = Mockito.mock(CoordinatorClient.class);
    emitter = new LatchableEmitter("", "", new LatchableEmitterConfig(null), new NoopTaskHolder());
    segmentView = new MetadataSegmentView(
        coordinatorClient,
        new BrokerSegmentWatcherConfig(),
        new BrokerSegmentMetadataCacheConfig()
        {
          @Override
          public long getMetadataSegmentPollPeriod()
          {
            return 100;
          }
        },
        emitter
    );
  }

  @AfterEach
  public void teardown()
  {
    segmentView.stop();
  }

  @Test
  public void test_start_triggersSegmentPollFromCoordinator_ifCacheIsEnabled()
  {
    // Set up the mock CoordinatorClient to return the expected list of segments
    final List<SegmentStatusInCluster> expectedSegments = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .withNumPartitions(10)
        .eachOfSizeInMb(100)
        .stream()
        .map(s -> new SegmentStatusInCluster(s, false, 1, 100L, false))
        .toList();

    Mockito.when(
        coordinatorClient.fetchAllUsedSegmentsWithOvershadowedStatus(
            ArgumentMatchers.eq(null),
            ArgumentMatchers.eq(true)
        )
    ).thenReturn(
        Futures.immediateFuture(CloseableIterators.withEmptyBaggage(expectedSegments.iterator()))
    );

    // Start the test target and wait for it to sync with the Coordinator
    segmentView.start();
    emitter.waitForEvent(event -> event.hasMetricName(Metric.SYNC_DURATION_MILLIS));

    final List<SegmentStatusInCluster> observedSegments = new ArrayList<>();
    Iterators.addAll(observedSegments, segmentView.getSegments());
    Assertions.assertEquals(10, observedSegments.size());
    Assertions.assertEquals(expectedSegments, observedSegments);
  }

  @Test
  public void test_firstPoll_callsSegmentsAddedThenTimelineInitialized_subsequentPoll_onlyFiresDelta()
  {
    final List<DataSegment> initialSegments = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .withNumPartitions(3)
        .eachOfSizeInMb(100);

    // Second poll: keep first two segments, add one new one
    final List<DataSegment> newSegments = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .startingAt("2020-01-02")
        .withNumPartitions(1)
        .eachOfSizeInMb(100);

    final List<DataSegment> secondPollSegments = new ArrayList<>();
    secondPollSegments.add(initialSegments.get(0));
    secondPollSegments.add(initialSegments.get(1));
    secondPollSegments.addAll(newSegments);

    //noinspection unchecked
    Mockito.when(
        coordinatorClient.fetchAllUsedSegmentsWithOvershadowedStatus(
            ArgumentMatchers.eq(null),
            ArgumentMatchers.eq(true)
        )
    ).thenReturn(
        Futures.immediateFuture(CloseableIterators.withEmptyBaggage(toStatusList(initialSegments).iterator())),
        Futures.immediateFuture(CloseableIterators.withEmptyBaggage(toStatusList(secondPollSegments).iterator()))
    );

    final List<DataSegment> addedCapture = new ArrayList<>();
    final List<SegmentId> removedCapture = new ArrayList<>();
    final AtomicInteger initializedCount = new AtomicInteger(0);
    // Track call order: "A" for segmentsAdded, "I" for timelineInitialized
    final List<String> callOrder = new ArrayList<>();

    segmentView.registerSegmentViewCallback(
        Execs.directExecutor(), new MetadataSegmentViewCallback()
        {
          @Override
          public void timelineInitialized()
          {
            initializedCount.incrementAndGet();
            callOrder.add("I");
          }

          @Override
          public void segmentsAdded(Collection<DataSegment> added)
          {
            addedCapture.addAll(added);
            callOrder.add("A");
          }

          @Override
          public void segmentsRemoved(Collection<SegmentId> removed)
          {
            removedCapture.addAll(removed);
          }
        }
    );

    // First poll: all initial segments are added, timelineInitialized fires once
    segmentView.start();
    emitter.waitForEvent(event -> event.hasMetricName(Metric.SYNC_DURATION_MILLIS));

    Assertions.assertEquals(
        initialSegments.stream().map(DataSegment::getId).collect(Collectors.toSet()),
        addedCapture.stream().map(DataSegment::getId).collect(Collectors.toSet())
    );
    Assertions.assertTrue(removedCapture.isEmpty());
    Assertions.assertEquals(1, initializedCount.get());
    // segmentsAdded must be called before timelineInitialized
    Assertions.assertEquals(List.of("A", "I"), callOrder);

    // Clear captures before second poll
    emitter.flush();
    addedCapture.clear();
    removedCapture.clear();
    callOrder.clear();

    // Second poll: only delta callbacks fire
    emitter.waitForNextEvent(event -> event.hasMetricName(Metric.SYNC_DURATION_MILLIS));

    // Only the new segment is added; the removed one (initialSegments[2]) is reported
    Assertions.assertEquals(
        newSegments.stream().map(DataSegment::getId).collect(Collectors.toSet()),
        addedCapture.stream().map(DataSegment::getId).collect(Collectors.toSet())
    );
    Assertions.assertEquals(
        Set.of(initialSegments.get(2).getId()),
        Set.copyOf(removedCapture)
    );
    // timelineInitialized must NOT fire again on the second poll
    Assertions.assertEquals(1, initializedCount.get());
  }

  @Test
  public void test_pollWithNoChanges_firesNoCallbacks()
  {
    final List<DataSegment> segments = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .withNumPartitions(2)
        .eachOfSizeInMb(100);

    // Both polls return the same segments
    Mockito.when(
        coordinatorClient.fetchAllUsedSegmentsWithOvershadowedStatus(
            ArgumentMatchers.eq(null),
            ArgumentMatchers.eq(true)
        )
    ).thenReturn(
        Futures.immediateFuture(CloseableIterators.withEmptyBaggage(toStatusList(segments).iterator())),
        Futures.immediateFuture(CloseableIterators.withEmptyBaggage(toStatusList(segments).iterator()))
    );

    final AtomicBoolean addedFiredAfterFirstPoll = new AtomicBoolean(false);
    final AtomicBoolean removedFiredAfterFirstPoll = new AtomicBoolean(false);

    segmentView.registerSegmentViewCallback(
        Execs.directExecutor(), new MetadataSegmentViewCallback()
        {
          private boolean firstPollDone = false;

          @Override
          public void timelineInitialized()
          {
            firstPollDone = true;
          }

          @Override
          public void segmentsAdded(Collection<DataSegment> added)
          {
            if (firstPollDone) {
              addedFiredAfterFirstPoll.set(true);
            }
          }

          @Override
          public void segmentsRemoved(Collection<SegmentId> removed)
          {
            if (firstPollDone) {
              removedFiredAfterFirstPoll.set(true);
            }
          }
        }
    );

    segmentView.start();
    emitter.waitForEvent(event -> event.hasMetricName(Metric.SYNC_DURATION_MILLIS));
    emitter.flush();
    emitter.waitForNextEvent(event -> event.hasMetricName(Metric.SYNC_DURATION_MILLIS));

    Assertions.assertFalse(addedFiredAfterFirstPoll.get());
    Assertions.assertFalse(removedFiredAfterFirstPoll.get());
  }

  @Test
  public void test_firstPoll_withNoSegments_firesTimelineInitializedWithoutSegmentsAdded()
  {
    mockCoordinatorToReturn(List.of());

    final AtomicBoolean addedFired = new AtomicBoolean(false);
    final AtomicInteger initializedCount = new AtomicInteger(0);

    segmentView.registerSegmentViewCallback(
        Execs.directExecutor(), new MetadataSegmentViewCallback()
        {
          @Override
          public void timelineInitialized()
          {
            initializedCount.incrementAndGet();
          }

          @Override
          public void segmentsAdded(Collection<DataSegment> added)
          {
            addedFired.set(true);
          }

          @Override
          public void segmentsRemoved(Collection<SegmentId> removed)
          {
          }
        }
    );

    segmentView.start();
    emitter.waitForEvent(event -> event.hasMetricName(Metric.SYNC_DURATION_MILLIS));

    Assertions.assertFalse(addedFired.get());
    Assertions.assertEquals(1, initializedCount.get());
  }

  private void mockCoordinatorToReturn(List<SegmentStatusInCluster> segments)
  {
    Mockito.when(
        coordinatorClient.fetchAllUsedSegmentsWithOvershadowedStatus(
            ArgumentMatchers.eq(null),
            ArgumentMatchers.eq(true)
        )
    ).thenReturn(
        Futures.immediateFuture(CloseableIterators.withEmptyBaggage(segments.iterator()))
    );
  }

  private static List<SegmentStatusInCluster> toStatusList(List<DataSegment> segments)
  {
    return segments.stream()
                   .map(s -> new SegmentStatusInCluster(s, false, 1, 100L, false))
                   .collect(Collectors.toList());
  }
}
