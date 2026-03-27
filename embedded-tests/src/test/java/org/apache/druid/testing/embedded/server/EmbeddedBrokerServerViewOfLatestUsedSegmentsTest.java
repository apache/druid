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

package org.apache.druid.testing.embedded.server;

import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.sql.calcite.schema.BrokerServerViewOfLatestUsedSegments;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration test for {@link BrokerServerViewOfLatestUsedSegments}.
 * Verifies that the merged timeline covers both loaded segments and metadata-only segments.
 */
public class EmbeddedBrokerServerViewOfLatestUsedSegmentsTest extends EmbeddedClusterTestBase
{
  private String fixedDataSource;

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");
    broker.addProperty("druid.sql.planner.metadataSegmentCacheEnable", "true");
    broker.addProperty("druid.sql.planner.metadataSegmentPollPeriod", "100");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker);
  }

  @BeforeAll
  @Override
  public void setup() throws Exception
  {
    fixedDataSource = EmbeddedClusterApis.createTestDatasourceName();
    dataSource = fixedDataSource;
    super.setup();
    ingestData(dataSource);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }

  @BeforeEach
  @Override
  protected void refreshDatasourceName()
  {
    dataSource = fixedDataSource;
  }

  @Test
  public void testTimelineContainsAllAvailableSegments()
  {
    final var view = broker.bindings().getInstance(BrokerServerViewOfLatestUsedSegments.class);
    final Set<DataSegment> metadataSegments = coordinator.bindings()
                                                         .segmentsMetadataStorage()
                                                         .retrieveAllUsedSegments(
                                                             dataSource,
                                                             Segments.INCLUDING_OVERSHADOWED
                                                         );
    Assertions.assertFalse(metadataSegments.isEmpty(), "Expected segments in metadata");

    final TimelineLookup<String, ServerSelector> timeline = view.getTimeline(TableDataSource.create(dataSource))
                                                                .orElse(null);
    Assertions.assertNotNull(timeline, "Expected non-empty timeline from BrokerServerViewOfLatestUsedSegments");

    final List<TimelineObjectHolder<String, ServerSelector>> holders =
        timeline.lookup(Intervals.ETERNITY);
    Assertions.assertFalse(holders.isEmpty(), "Expected non-empty lookup result");

    // Collect all segment IDs from the timeline
    final Set<String> timelineSegmentIds = new HashSet<>();
    for (TimelineObjectHolder<String, ServerSelector> holder : holders) {
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        final ServerSelector selector = chunk.getObject();
        timelineSegmentIds.add(selector.getSegment().getId().toString());

        // Segments are on historicals, so the selector should not be empty
        Assertions.assertFalse(
            selector.isEmpty(),
            "Expected available server for segment " + selector.getSegment().getId()
        );
      }
    }

    // Every segment in metadata should be in the timeline
    for (DataSegment segment : metadataSegments) {
      Assertions.assertTrue(
          timelineSegmentIds.contains(segment.getId().toString()),
          "Metadata segment missing from timeline: " + segment.getId()
      );
    }

    // Run SQL query to ensure the server is functional
    String result = cluster.callApi().runSql("SELECT COUNT(*) FROM %s", dataSource);
    Assertions.assertFalse(result.isBlank());
  }

  @Test
  public void testTimelineIncludesMetadataOnlySegmentsNotPresentInBrokerServerView()
  {
    final String metadataOnlyDataSource = EmbeddedClusterApis.createTestDatasourceName();
    final var mergedView = broker.bindings().getInstance(BrokerServerViewOfLatestUsedSegments.class);
    final BrokerServerView brokerServerView = broker.bindings().getInstance(BrokerServerView.class);

    setCoordinatorPaused(true);
    try {
      ingestData(metadataOnlyDataSource);

      final Set<DataSegment> metadataSegments = getMetadataSegments(metadataOnlyDataSource);
      Assertions.assertFalse(metadataSegments.isEmpty(), "Expected segments in metadata");

      // Wait for MetadataSegmentView to poll and update publishedSegments (which backs sys.segments).
      // poll() updates publishedSegments before firing segmentsAdded callbacks,
      // so a present mergedTimeline guarantees sys.segments already reflects the new segments.
      cluster.callApi().waitForResult(
          () -> mergedView.getTimeline(TableDataSource.create(metadataOnlyDataSource)),
          Optional::isPresent
      ).go();

      cluster.callApi().verifySqlQuery(
          "SELECT COUNT(*) FROM sys.segments WHERE datasource = '%s' AND is_available = 0",
          metadataOnlyDataSource,
          String.valueOf(metadataSegments.size())
      );

      Assertions.assertFalse(
          brokerServerView.getTimeline(TableDataSource.create(metadataOnlyDataSource)).isPresent(),
          "Plain BrokerServerView should not expose metadata-only segments"
      );

      final TimelineLookup<String, ServerSelector> mergedTimeline = mergedView
          .getTimeline(TableDataSource.create(metadataOnlyDataSource))
          .orElse(null);
      Assertions.assertNotNull(mergedTimeline, "Expected merged timeline for metadata-only datasource");

      final Set<String> timelineSegmentIds = new HashSet<>();
      for (TimelineObjectHolder<String, ServerSelector> holder : mergedTimeline.lookup(Intervals.ETERNITY)) {
        for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
          final ServerSelector selector = chunk.getObject();
          timelineSegmentIds.add(selector.getSegment().getId().toString());
          Assertions.assertTrue(
              selector.isEmpty(),
              "Expected metadata-only segment to have no available servers: " + selector.getSegment().getId()
          );
        }
      }

      Set<String> expected = metadataSegments.stream()
                                             .map(segment -> segment.getId().toString())
                                             .collect(Collectors.toSet());
      Assertions.assertEquals(expected, timelineSegmentIds);
    }
    finally {
      setCoordinatorPaused(false);
      cluster.callApi().waitForAllSegmentsToBeAvailable(metadataOnlyDataSource, coordinator, broker);
    }
  }

  @Test
  public void testTimelineSelectorsBecomeNonEmptyAfterMetadataOnlySegmentsLoad()
  {
    final String metadataOnlyDataSource = EmbeddedClusterApis.createTestDatasourceName();
    final BrokerServerViewOfLatestUsedSegments mergedView =
        broker.bindings().getInstance(BrokerServerViewOfLatestUsedSegments.class);

    setCoordinatorPaused(true);
    try {
      ingestData(metadataOnlyDataSource);
      final Set<DataSegment> metadataSegments = getMetadataSegments(metadataOnlyDataSource);

      cluster.callApi().waitForResult(
          () -> mergedView.getTimeline(TableDataSource.create(metadataOnlyDataSource)),
          Optional::isPresent
      ).go();

      final TimelineLookup<String, ServerSelector> metadataOnlyTimeline =
          mergedView.getTimeline(TableDataSource.create(metadataOnlyDataSource)).orElse(null);
      Assertions.assertNotNull(metadataOnlyTimeline, "Expected merged timeline before historical load");
      assertTimelineMatchesMetadata(metadataOnlyTimeline, metadataSegments, true);
    }
    finally {
      setCoordinatorPaused(false);
    }

    cluster.callApi().waitForAllSegmentsToBeAvailable(metadataOnlyDataSource, coordinator, broker);

    final TimelineLookup<String, ServerSelector> loadedTimeline =
        mergedView.getTimeline(TableDataSource.create(metadataOnlyDataSource)).orElse(null);

    Assertions.assertNotNull(loadedTimeline, "Expected merged timeline after historical load");
    assertTimelineMatchesMetadata(loadedTimeline, getMetadataSegments(metadataOnlyDataSource), false);
  }

  private void ingestData(final String targetDataSource)
  {
    cluster.callApi().runTask(
        TaskBuilder.ofTypeIndex()
                   .dataSource(targetDataSource)
                   .isoTimestampColumn("time")
                   .csvInputFormatWithColumns("time", "item", "value")
                   .inlineInputSourceWithData(Resources.InlineData.CSV_10_DAYS)
                   .segmentGranularity("DAY")
                   .dimensions()
                   .withId(IdUtils.getRandomId()),
        overlord
    );
  }

  private Set<DataSegment> getMetadataSegments(final String targetDataSource)
  {
    return coordinator.bindings()
                      .segmentsMetadataStorage()
                      .retrieveAllUsedSegments(targetDataSource, Segments.INCLUDING_OVERSHADOWED);
  }

  private void setCoordinatorPaused(final boolean paused)
  {
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(
            paused
            ? CoordinatorDynamicConfig.builder().withPauseCoordination(true).build()
            : CoordinatorDynamicConfig.builder().build()
        )
    );
  }

  private static void assertTimelineMatchesMetadata(
      final TimelineLookup<String, ServerSelector> timeline,
      final Set<DataSegment> metadataSegments,
      final boolean expectEmptySelectors
  )
  {
    final Set<String> timelineSegmentIds = new HashSet<>();
    for (TimelineObjectHolder<String, ServerSelector> holder : timeline.lookup(Intervals.ETERNITY)) {
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        final ServerSelector selector = chunk.getObject();
        timelineSegmentIds.add(selector.getSegment().getId().toString());
        Assertions.assertEquals(
            expectEmptySelectors,
            selector.isEmpty(),
            "Unexpected selector availability for segment " + selector.getSegment().getId()
        );
      }
    }

    for (DataSegment segment : metadataSegments) {
      Assertions.assertTrue(
          timelineSegmentIds.contains(segment.getId().toString()),
          "Metadata segment missing from timeline: " + segment.getId()
      );
    }
  }
}
