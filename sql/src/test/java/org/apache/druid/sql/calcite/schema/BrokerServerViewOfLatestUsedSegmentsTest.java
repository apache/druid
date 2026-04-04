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

import com.google.common.collect.Ordering;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView.TimelineCallback;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.HistoricalFilter;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BrokerServerViewOfLatestUsedSegmentsTest
{
  private static final DruidServerMetadata HISTORICAL_SERVER = new DruidServerMetadata(
      "historical-1",
      "host1:8083",
      null,
      1000L,
      null,
      ServerType.HISTORICAL,
      "tier1",
      0
  );

  private static final TierSelectorStrategy TIER_STRATEGY =
      new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy());

  private BrokerServerView brokerServerView;
  private MetadataSegmentView metadataSegmentView;
  private TimelineCallback bsvCallback;
  private MetadataSegmentViewCallback msvCallback;
  private Map<String, VersionedIntervalTimeline<String, ServerSelector>> bsvTimelines;

  @BeforeEach
  public void setup()
  {
    brokerServerView = Mockito.mock(BrokerServerView.class);
    metadataSegmentView = Mockito.mock(MetadataSegmentView.class);
    bsvTimelines = new HashMap<>();
  }

  @Test
  public void test_metadataOnlySegment_appearsInTimelineWithEmptySelector()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    msvCallback.segmentsAdded(List.of(segment));

    final PartitionChunk<ServerSelector> chunk = findChunk(view, segment);
    Assertions.assertNotNull(chunk);
    Assertions.assertTrue(chunk.getObject().isEmpty());
  }

  @Test
  public void test_segmentInBothSources_usesBsvSelector_metadataFirst()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    msvCallback.segmentsAdded(List.of(segment));
    final ServerSelector bsvSelector = addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);

    final List<PartitionChunk<ServerSelector>> chunks = findChunks(view, segment);
    Assertions.assertEquals(1, chunks.size(), "merged timeline should not duplicate a segment present in both sources");
    Assertions.assertSame(bsvSelector, chunks.get(0).getObject());
  }

  @Test
  public void test_segmentInBothSources_usesBsvSelector_bsvFirst()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    final ServerSelector bsvSelector = addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);
    msvCallback.segmentsAdded(List.of(segment));

    final List<PartitionChunk<ServerSelector>> chunks = findChunks(view, segment);
    Assertions.assertEquals(
        1,
        chunks.size(),
        "metadata should not create an overlay duplicate for an already-available segment"
    );
    Assertions.assertSame(bsvSelector, chunks.get(0).getObject());
  }

  @Test
  public void test_bsvOnlySegment_appearsThroughMergedTimeline()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    final ServerSelector bsvSelector = addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);

    final PartitionChunk<ServerSelector> chunk = findChunk(view, segment);
    Assertions.assertNotNull(chunk);
    Assertions.assertSame(bsvSelector, chunk.getObject());
  }

  @Test
  public void test_getTimeline_returnsCachedTimelineInstance()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    msvCallback.segmentsAdded(List.of(segment));

    final TimelineLookup<String, ServerSelector> firstTimeline =
        view.getTimeline(TableDataSource.create(TestDataSource.WIKI)).orElse(null);
    final TimelineLookup<String, ServerSelector> secondTimeline =
        view.getTimeline(TableDataSource.create(TestDataSource.WIKI)).orElse(null);

    Assertions.assertNotNull(firstTimeline);
    Assertions.assertSame(firstTimeline, secondTimeline);
  }

  @Test
  public void test_segmentBecomesUnavailable_staysInTimelineWithEmptySelector()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    msvCallback.segmentsAdded(List.of(segment));
    final ServerSelector bsvSelector = addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);

    final List<DataSegment> removedCapture = new ArrayList<>();
    view.registerTimelineCallback(Execs.directExecutor(), captureRemovedCallback(removedCapture));

    removeFromBsvTimeline(segment, bsvSelector);
    bsvCallback.segmentRemoved(segment);

    final PartitionChunk<ServerSelector> chunk = findChunk(view, segment);
    Assertions.assertNotNull(chunk);
    Assertions.assertTrue(chunk.getObject().isEmpty());
    Assertions.assertNotSame(bsvSelector, chunk.getObject());
    Assertions.assertTrue(
        removedCapture.isEmpty(),
        "segmentRemoved should not fire while metadata still keeps the segment used"
    );
  }

  @Test
  public void test_segmentBecomesUsedAgain_replacesOverlaySelectorWithBsvSelector()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    msvCallback.segmentsAdded(List.of(segment));
    final ServerSelector firstSelector = addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);
    removeFromBsvTimeline(segment, firstSelector);
    bsvCallback.segmentRemoved(segment);

    final ServerSelector secondSelector = addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);

    final List<PartitionChunk<ServerSelector>> chunks = findChunks(view, segment);
    Assertions.assertEquals(1, chunks.size());
    Assertions.assertSame(secondSelector, chunks.get(0).getObject());
  }

  @Test
  public void test_segmentRemovedFromMetadata_dropsFromMergedViewEvenIfBrokerStillHasIt()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    msvCallback.segmentsAdded(List.of(segment));
    addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);

    final List<DataSegment> removedCapture = new ArrayList<>();
    view.registerTimelineCallback(Execs.directExecutor(), captureRemovedCallback(removedCapture));

    // This encodes the current expected policy for the overlay view:
    // once metadata says the segment is unused, it should disappear from the merged view immediately,
    // even if BrokerServerView has not observed the unload yet. Revisit if product semantics change.
    msvCallback.segmentsRemoved(List.of(segment.getId()));

    Assertions.assertNull(findChunk(view, segment));
    Assertions.assertEquals(List.of(segment), removedCapture);
  }

  @Test
  public void test_fullRemoval_bsvOnlySegmentRemoved()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    final ServerSelector bsvSelector = addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);

    final List<DataSegment> removedCapture = new ArrayList<>();
    view.registerTimelineCallback(Execs.directExecutor(), captureRemovedCallback(removedCapture));

    removeFromBsvTimeline(segment, bsvSelector);
    bsvCallback.segmentRemoved(segment);

    Assertions.assertNull(findChunk(view, segment));
    Assertions.assertEquals(List.of(segment), removedCapture);
  }

  @Test
  public void test_metadataOnlyAddition_doesNotFireSegmentAddedCallback()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);
    final AtomicBoolean segmentAddedFired = new AtomicBoolean(false);

    view.registerTimelineCallback(
        Execs.directExecutor(),
        new NoopTimelineCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment seg)
          {
            segmentAddedFired.set(true);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    msvCallback.segmentsAdded(List.of(segment));

    Assertions.assertFalse(segmentAddedFired.get());
  }

  @Test
  public void test_delegatesToBrokerForServersAndQueryRunner()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DruidServer server = Mockito.mock(DruidServer.class);
    final QueryRunner<Object> queryRunner = Mockito.mock(QueryRunner.class);
    final List<ImmutableDruidServer> druidServers = List.of(Mockito.mock(ImmutableDruidServer.class));

    Mockito.when(brokerServerView.getDruidServers()).thenReturn(druidServers);
    Mockito.when(brokerServerView.getQueryRunner(server)).thenReturn(queryRunner);

    Assertions.assertSame(druidServers, view.getDruidServers());
    Assertions.assertSame(queryRunner, view.getQueryRunner(server));
  }

  @Test
  public void test_cacheDisabled_constructorSucceeds_methodsThrowISE()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(false);

    Assertions.assertThrows(ISE.class, () -> view.getTimeline(TableDataSource.create(TestDataSource.WIKI)));
    Assertions.assertThrows(ISE.class, view::getDruidServers);
    Assertions.assertThrows(
        ISE.class,
        () -> view.registerTimelineCallback(Execs.directExecutor(), Mockito.mock(TimelineCallback.class))
    );
  }

  @Test
  public void test_timelineInitialized_firesOnceAfterBothSourcesInit()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final AtomicInteger initializedCount = new AtomicInteger(0);
    view.registerTimelineCallback(
        Execs.directExecutor(),
        new NoopTimelineCallback()
        {
          @Override
          public ServerView.CallbackAction timelineInitialized()
          {
            initializedCount.incrementAndGet();
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    bsvCallback.timelineInitialized();
    Assertions.assertEquals(0, initializedCount.get());

    msvCallback.timelineInitialized();
    Assertions.assertEquals(1, initializedCount.get());

    bsvCallback.timelineInitialized();
    msvCallback.timelineInitialized();
    Assertions.assertEquals(1, initializedCount.get());
  }

  @Test
  public void test_passThroughCallbacks_forwarded()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    final AtomicBoolean serverSegmentRemovedFired = new AtomicBoolean(false);
    final AtomicBoolean schemasFired = new AtomicBoolean(false);
    view.registerTimelineCallback(
        Execs.directExecutor(),
        new NoopTimelineCallback()
        {
          @Override
          public ServerView.CallbackAction serverSegmentRemoved(DruidServerMetadata server, DataSegment seg)
          {
            serverSegmentRemovedFired.set(true);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas schemas)
          {
            schemasFired.set(true);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    bsvCallback.serverSegmentRemoved(HISTORICAL_SERVER, segment);
    bsvCallback.segmentSchemasAnnounced(Mockito.mock(SegmentSchemas.class));

    Assertions.assertTrue(serverSegmentRemovedFired.get());
    Assertions.assertTrue(schemasFired.get());
  }

  @Test
  public void test_newerMetadataVersion_overshadowsOlderBsvVersion()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);

    final String startDate = "2020-01-01";
    final DataSegment v1Segment = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .startingAt(startDate)
        .withVersion("v1")
        .withNumPartitions(1)
        .eachOfSizeInMb(100)
        .get(0);
    final DataSegment v2Segment = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .startingAt(startDate)
        .withVersion("v2")
        .withNumPartitions(1)
        .eachOfSizeInMb(100)
        .get(0);

    // v1 is loaded on a historical
    final ServerSelector v1Selector = addToBsvTimeline(v1Segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, v1Segment);

    // v2 exists only in metadata (not yet loaded)
    msvCallback.segmentsAdded(List.of(v1Segment, v2Segment));

    // Merged timeline should return v2 (latest version), not v1
    final TimelineLookup<String, ServerSelector> timeline =
        view.getTimeline(TableDataSource.create(TestDataSource.WIKI)).orElse(null);
    Assertions.assertNotNull(timeline);

    final List<TimelineObjectHolder<String, ServerSelector>> holders =
        timeline.lookup(v1Segment.getInterval());
    Assertions.assertEquals(1, holders.size());
    Assertions.assertEquals("v2", holders.get(0).getVersion());

    // The v2 selector should be empty (not loaded), not the v1 BSV selector
    final PartitionChunk<ServerSelector> chunk = holders.get(0).getObject().iterator().next();
    Assertions.assertTrue(chunk.getObject().isEmpty());
    Assertions.assertNotSame(v1Selector, chunk.getObject());
  }

  @Test
  public void test_getTimeline_returnsEmptyAfterAllSegmentsRemoved()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    // Add segment to metadata only (unavailable overlay)
    msvCallback.segmentsAdded(List.of(segment));
    Assertions.assertTrue(view.getTimeline(TableDataSource.create(TestDataSource.WIKI)).isPresent());

    // Remove it from metadata
    msvCallback.segmentsRemoved(List.of(segment.getId()));
    Assertions.assertFalse(
        view.getTimeline(TableDataSource.create(TestDataSource.WIKI)).isPresent(),
        "getTimeline should return empty after all segments are removed"
    );
  }

  @Test
  public void test_metadataRemoval_suppressesLateBsvAddition()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final DataSegment segment = createSegments(1).get(0);

    msvCallback.segmentsAdded(List.of(segment));
    msvCallback.segmentsRemoved(List.of(segment.getId()));

    addToBsvTimeline(segment);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, segment);

    Assertions.assertFalse(view.getTimeline(TableDataSource.create(TestDataSource.WIKI)).isPresent());
    Assertions.assertNull(findChunk(view, segment));
  }

  @Test
  public void test_removeUnknownSegment_noError()
  {
    createView(true);
    final DataSegment segment = createSegments(1).get(0);

    Assertions.assertDoesNotThrow(() -> msvCallback.segmentsRemoved(List.of(segment.getId())));
    Assertions.assertDoesNotThrow(() -> bsvCallback.segmentRemoved(segment));
  }

  @Test
  public void test_callbackUnregister_stopsReceivingEvents()
  {
    final BrokerServerViewOfLatestUsedSegments view = createView(true);
    final AtomicInteger callCount = new AtomicInteger(0);
    view.registerTimelineCallback(
        Execs.directExecutor(),
        new NoopTimelineCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(DataSegment segment)
          {
            callCount.incrementAndGet();
            return ServerView.CallbackAction.UNREGISTER;
          }
        }
    );

    final DataSegment seg1 = createSegments(1).get(0);
    final DataSegment seg2 = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .startingAt("2020-01-02")
        .withNumPartitions(1)
        .eachOfSizeInMb(100)
        .get(0);

    final ServerSelector selector1 = addToBsvTimeline(seg1);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, seg1);
    removeFromBsvTimeline(seg1, selector1);
    bsvCallback.segmentRemoved(seg1);
    Assertions.assertEquals(1, callCount.get());

    final ServerSelector selector2 = addToBsvTimeline(seg2);
    bsvCallback.segmentAdded(HISTORICAL_SERVER, seg2);
    removeFromBsvTimeline(seg2, selector2);
    bsvCallback.segmentRemoved(seg2);
    Assertions.assertEquals(1, callCount.get());
  }

  private BrokerServerViewOfLatestUsedSegments createView(final boolean cacheEnabled)
  {
    final BrokerSegmentMetadataCacheConfig cacheConfig = Mockito.mock(BrokerSegmentMetadataCacheConfig.class);
    Mockito.when(cacheConfig.isMetadataSegmentCacheEnable()).thenReturn(cacheEnabled);

    final ArgumentCaptor<TimelineCallback> bsvCaptor = ArgumentCaptor.forClass(TimelineCallback.class);
    Mockito.doNothing().when(brokerServerView)
           .registerTimelineCallback(ArgumentMatchers.any(Executor.class), bsvCaptor.capture());

    final ArgumentCaptor<MetadataSegmentViewCallback> msvCaptor =
        ArgumentCaptor.forClass(MetadataSegmentViewCallback.class);
    Mockito.doNothing().when(metadataSegmentView)
           .registerSegmentViewCallback(ArgumentMatchers.any(Executor.class), msvCaptor.capture());

    Mockito.when(brokerServerView.getTimeline(ArgumentMatchers.any(TableDataSource.class)))
           .thenAnswer(invocation -> {
             final TableDataSource dataSource = invocation.getArgument(0);
             return Optional.ofNullable(bsvTimelines.get(dataSource.getName()));
           });

    final BrokerServerViewOfLatestUsedSegments view = new BrokerServerViewOfLatestUsedSegments(
        brokerServerView,
        metadataSegmentView,
        cacheConfig,
        TIER_STRATEGY,
        TIER_STRATEGY,
        Mockito.mock(BrokerViewOfCoordinatorConfig.class)
    );

    bsvCallback = bsvCaptor.getValue();
    msvCallback = msvCaptor.getValue();
    return view;
  }

  private static List<DataSegment> createSegments(final int numPartitions)
  {
    return CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .withNumPartitions(numPartitions)
        .eachOfSizeInMb(100);
  }

  private ServerSelector addToBsvTimeline(final DataSegment segment)
  {
    final ServerSelector selector = new ServerSelector(segment, TIER_STRATEGY, HistoricalFilter.IDENTITY_FILTER);
    final VersionedIntervalTimeline<String, ServerSelector> timeline = bsvTimelines.computeIfAbsent(
        segment.getDataSource(),
        ds -> new VersionedIntervalTimeline<>(Ordering.natural(), true)
    );
    timeline.add(
        segment.getInterval(),
        segment.getVersion(),
        segment.getShardSpec().createChunk(selector)
    );
    return selector;
  }

  private void removeFromBsvTimeline(final DataSegment segment, final ServerSelector selector)
  {
    final VersionedIntervalTimeline<String, ServerSelector> timeline = bsvTimelines.get(segment.getDataSource());
    if (timeline != null) {
      timeline.remove(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk(selector)
      );
    }
  }

  private static PartitionChunk<ServerSelector> findChunk(
      final BrokerServerViewOfLatestUsedSegments view,
      final DataSegment segment
  )
  {
    final List<PartitionChunk<ServerSelector>> chunks = findChunks(view, segment);
    return chunks.isEmpty() ? null : chunks.get(0);
  }

  private static List<PartitionChunk<ServerSelector>> findChunks(
      final BrokerServerViewOfLatestUsedSegments view,
      final DataSegment segment
  )
  {
    final TimelineLookup<String, ServerSelector> timeline =
        view.getTimeline(TableDataSource.create(segment.getDataSource())).orElse(null);
    final List<PartitionChunk<ServerSelector>> matches = new ArrayList<>();

    if (timeline == null) {
      return matches;
    }

    for (TimelineObjectHolder<String, ServerSelector> holder : timeline.lookup(segment.getInterval())) {
      if (!holder.getVersion().equals(segment.getVersion())) {
        continue;
      }
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        if (chunk.getChunkNumber() == segment.getShardSpec().getPartitionNum()) {
          matches.add(chunk);
        }
      }
    }

    return matches;
  }

  private static TimelineCallback captureRemovedCallback(final List<DataSegment> removedCapture)
  {
    return new NoopTimelineCallback()
    {
      @Override
      public ServerView.CallbackAction segmentRemoved(DataSegment segment)
      {
        removedCapture.add(segment);
        return ServerView.CallbackAction.CONTINUE;
      }
    };
  }

  private static class NoopTimelineCallback implements TimelineCallback
  {
    @Override
    public ServerView.CallbackAction timelineInitialized()
    {
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
    {
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentRemoved(DataSegment segment)
    {
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction serverSegmentRemoved(DruidServerMetadata server, DataSegment segment)
    {
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
    {
      return ServerView.CallbackAction.CONTINUE;
    }
  }
}
