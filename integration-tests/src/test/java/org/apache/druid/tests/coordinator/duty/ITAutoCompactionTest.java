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

package org.apache.druid.tests.coordinator.duty;

import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CompactionResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractITBatchIndexTest;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Test(groups = {TestNGGroup.COMPACTION})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAutoCompactionTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITAutoCompactionTest.class);
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final int MAX_ROWS_PER_SEGMENT_COMPACTED = 10000;
  private static final Period NO_SKIP_OFFSET = Period.seconds(0);

  @Inject
  protected CompactionResourceTestClient compactionResource;

  @Inject
  private IntegrationTestingConfig config;

  private String fullDatasourceName;

  @BeforeMethod
  public void setup() throws Exception
  {
    // Set comapction slot to 5
    updateCompactionTaskSlot(0.5, 10);
    fullDatasourceName = "wikipedia_index_test_" + UUID.randomUUID() + config.getExtraDatasourceNameSuffix();
  }

  @Test
  public void testAutoCompactionDutySubmitAndVerifyCompaction() throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
      intervalsBeforeCompaction.sort(null);
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, Period.days(1));
      //...compacted into 1 new segment for 1 day. 1 day compacted and 1 day skipped/remains uncompacted. (3 total)
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      checkCompactionIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          0,
          14312,
          0,
          0,
          2,
          0,
          0,
          1,
          0);
      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET);
      //...compacted into 1 new segment for the remaining one day. 2 day compacted and 0 day uncompacted. (2 total)
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      checkCompactionIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          0,
          22489,
          0,
          0,
          3,
          0,
          0,
          2,
          0);
    }
  }

  @Test
  public void testAutoCompactionDutyCanUpdateCompactionConfig() throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
      intervalsBeforeCompaction.sort(null);
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      // Dummy compaction config which will be overwritten
      submitCompactionConfig(10000, NO_SKIP_OFFSET);
      // New compaction config should overwrites the existing compaction config
      submitCompactionConfig(1, NO_SKIP_OFFSET);

      LOG.info("Auto compaction test with dynamic partitioning");

      // Instead of merging segments, the updated config will split segments!
      //...compacted into 10 new segments across 2 days. 5 new segments each day (10 total)
      forceTriggerAutoCompaction(10);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(10, 1);
      checkCompactionIntervals(intervalsBeforeCompaction);

      LOG.info("Auto compaction test with hash partitioning");

      final HashedPartitionsSpec hashedPartitionsSpec = new HashedPartitionsSpec(null, 3, null);
      submitCompactionConfig(hashedPartitionsSpec, NO_SKIP_OFFSET, 1, null);
      // 2 segments published per day after compaction.
      forceTriggerAutoCompaction(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(hashedPartitionsSpec, 4);
      checkCompactionIntervals(intervalsBeforeCompaction);

      LOG.info("Auto compaction test with range partitioning");

      final SingleDimensionPartitionsSpec rangePartitionsSpec = new SingleDimensionPartitionsSpec(
          5,
          null,
          "city",
          false
      );
      submitCompactionConfig(rangePartitionsSpec, NO_SKIP_OFFSET, 1, null);
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(rangePartitionsSpec, 2);
      checkCompactionIntervals(intervalsBeforeCompaction);
    }
  }

  @Test
  public void testAutoCompactionDutyCanDeleteCompactionConfig() throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
      intervalsBeforeCompaction.sort(null);
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET);
      deleteCompactionConfig();

      // ...should remains unchanged (4 total)
      forceTriggerAutoCompaction(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(0, null);

      checkCompactionIntervals(intervalsBeforeCompaction);
    }
  }

  @Test
  public void testAutoCompactionDutyCanUpdateTaskSlots() throws Exception
  {
    // Set compactionTaskSlotRatio to 0 to prevent any compaction
    updateCompactionTaskSlot(0, 0);
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
      intervalsBeforeCompaction.sort(null);
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, NO_SKIP_OFFSET);
      // ...should remains unchanged (4 total)
      forceTriggerAutoCompaction(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(0, null);
      checkCompactionIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0);
      // Update compaction slots to be 1
      updateCompactionTaskSlot(1, 1);
      // One day compacted (1 new segment) and one day remains uncompacted. (3 total)
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      checkCompactionIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          14312,
          14311,
          0,
          2,
          2,
          0,
          1,
          1,
          0);
      Assert.assertEquals(compactionResource.getCompactionProgress(fullDatasourceName).get("remainingSegmentSize"), "14312");
      // Run compaction again to compact the remaining day
      // Remaining day compacted (1 new segment). Now both days compacted (2 total)
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      checkCompactionIntervals(intervalsBeforeCompaction);
      getAndAssertCompactionStatus(
          fullDatasourceName,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          0,
          22489,
          0,
          0,
          3,
          0,
          0,
          2,
          0);
    }
  }

  @Test
  public void testAutoCompactionDutyWithSegmentGranularity() throws Exception
  {
    loadData(INDEX_TASK);
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
      intervalsBeforeCompaction.sort(null);
      // 4 segments across 2 days (4 total)...
      verifySegmentsCount(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);

      Granularity newGranularity = Granularities.YEAR;
      submitCompactionConfig(1000, NO_SKIP_OFFSET, new UniformGranularitySpec(newGranularity, null, null));

      LOG.info("Auto compaction test with YEAR segment granularity");

      List<String> expectedIntervalAfterCompaction = new ArrayList<>();
      for (String interval : intervalsBeforeCompaction) {
        for (Interval newinterval : newGranularity.getIterable(new Interval(interval, ISOChronology.getInstanceUTC()))) {
          expectedIntervalAfterCompaction.add(newinterval.toString());
        }
      }
      forceTriggerAutoCompaction(1);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, 1000);
      checkCompactionIntervals(expectedIntervalAfterCompaction);

      newGranularity = Granularities.DAY;
      submitCompactionConfig(1000, NO_SKIP_OFFSET, new UniformGranularitySpec(newGranularity, null, null));

      LOG.info("Auto compaction test with DAY segment granularity");

      // The earlier segment with YEAR granularity is still 'used' as it’s not fully overshaowed.
      // This is because we only have newer version on 2013-08-31 to 2013-09-01 and 2013-09-01 to 2013-09-02.
      // The version for the YEAR segment is still the latest for 2013-01-01 to 2013-08-31 and 2013-09-02 to 2014-01-01.
      // Hence, all three segments are available and the expected intervals are combined from the DAY and YEAR segment granularities
      // (which are 2013-08-31 to 2013-09-01, 2013-09-01 to 2013-09-02 and 2013-01-01 to 2014-01-01)
      for (String interval : intervalsBeforeCompaction) {
        for (Interval newinterval : newGranularity.getIterable(new Interval(interval, ISOChronology.getInstanceUTC()))) {
          expectedIntervalAfterCompaction.add(newinterval.toString());
        }
      }
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(3, 1000);
      checkCompactionIntervals(expectedIntervalAfterCompaction);
    }
  }

  private void loadData(String indexTask) throws Exception
  {
    String taskSpec = getResourceAsString(indexTask);
    taskSpec = StringUtils.replace(taskSpec, "%%DATASOURCE%%", fullDatasourceName);
    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment Load"
    );
  }

  private void verifyQuery(String queryResource) throws Exception
  {
    String queryResponseTemplate;
    try {
      InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryResource);
      queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new ISE(e, "could not read query file: %s", queryResource);
    }

    queryResponseTemplate = StringUtils.replace(
        queryResponseTemplate,
        "%%DATASOURCE%%",
        fullDatasourceName
    );

    queryHelper.testQueriesFromString(queryResponseTemplate);
  }

  private void submitCompactionConfig(Integer maxRowsPerSegment, Period skipOffsetFromLatest) throws Exception
  {
    submitCompactionConfig(maxRowsPerSegment, skipOffsetFromLatest, null);
  }

  private void submitCompactionConfig(Integer maxRowsPerSegment, Period skipOffsetFromLatest, GranularitySpec granularitySpec) throws Exception
  {
    submitCompactionConfig(new DynamicPartitionsSpec(maxRowsPerSegment, null), skipOffsetFromLatest, 1, granularitySpec);
  }

  private void submitCompactionConfig(
      PartitionsSpec partitionsSpec,
      Period skipOffsetFromLatest,
      int maxNumConcurrentSubTasks,
      GranularitySpec granularitySpec
  ) throws Exception
  {
    DataSourceCompactionConfig compactionConfig = new DataSourceCompactionConfig(
        fullDatasourceName,
        null,
        null,
        null,
        skipOffsetFromLatest,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            new MaxSizeSplitHintSpec(null, 1),
            partitionsSpec,
            null,
            null,
            null,
            null,
            null,
            maxNumConcurrentSubTasks,
            null,
            null,
            null,
            null,
            null,
            1
        ),
        granularitySpec,
        null
    );
    compactionResource.submitCompactionConfig(compactionConfig);

    // Wait for compaction config to persist
    Thread.sleep(2000);

    // Verify that the compaction config is updated correctly.
    CoordinatorCompactionConfig coordinatorCompactionConfig = compactionResource.getCoordinatorCompactionConfigs();
    DataSourceCompactionConfig foundDataSourceCompactionConfig = null;
    for (DataSourceCompactionConfig dataSourceCompactionConfig : coordinatorCompactionConfig.getCompactionConfigs()) {
      if (dataSourceCompactionConfig.getDataSource().equals(fullDatasourceName)) {
        foundDataSourceCompactionConfig = dataSourceCompactionConfig;
      }
    }
    Assert.assertNotNull(foundDataSourceCompactionConfig);
    Assert.assertNotNull(foundDataSourceCompactionConfig.getTuningConfig());
    Assert.assertEquals(foundDataSourceCompactionConfig.getTuningConfig().getPartitionsSpec(), partitionsSpec);
    Assert.assertEquals(foundDataSourceCompactionConfig.getSkipOffsetFromLatest(), skipOffsetFromLatest);

    foundDataSourceCompactionConfig = compactionResource.getDataSourceCompactionConfig(fullDatasourceName);
    Assert.assertNotNull(foundDataSourceCompactionConfig);
    Assert.assertNotNull(foundDataSourceCompactionConfig.getTuningConfig());
    Assert.assertEquals(foundDataSourceCompactionConfig.getTuningConfig().getPartitionsSpec(), partitionsSpec);
    Assert.assertEquals(foundDataSourceCompactionConfig.getSkipOffsetFromLatest(), skipOffsetFromLatest);
  }

  private void deleteCompactionConfig() throws Exception
  {
    compactionResource.deleteCompactionConfig(fullDatasourceName);

    // Verify that the compaction config is updated correctly.
    CoordinatorCompactionConfig coordinatorCompactionConfig = compactionResource.getCoordinatorCompactionConfigs();
    DataSourceCompactionConfig foundDataSourceCompactionConfig = null;
    for (DataSourceCompactionConfig dataSourceCompactionConfig : coordinatorCompactionConfig.getCompactionConfigs()) {
      if (dataSourceCompactionConfig.getDataSource().equals(fullDatasourceName)) {
        foundDataSourceCompactionConfig = dataSourceCompactionConfig;
      }
    }
    Assert.assertNull(foundDataSourceCompactionConfig);
  }

  private void forceTriggerAutoCompaction(int numExpectedSegmentsAfterCompaction) throws Exception
  {
    compactionResource.forceTriggerAutoCompaction();
    waitForAllTasksToCompleteForDataSource(fullDatasourceName);
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment Compaction"
    );
    verifySegmentsCount(numExpectedSegmentsAfterCompaction);
  }

  private void verifySegmentsCount(int numExpectedSegments)
  {
    ITRetryUtil.retryUntilTrue(
        () -> {
          int metadataSegmentCount = coordinator.getSegments(fullDatasourceName).size();
          LOG.info("Current metadata segment count: %d, expected: %d", metadataSegmentCount, numExpectedSegments);
          return metadataSegmentCount == numExpectedSegments;
        },
        "Compaction segment count check"
    );
  }

  private void checkCompactionIntervals(List<String> expectedIntervals)
  {
    Set<String> expectedIntervalsSet = new HashSet<>(expectedIntervals);
    ITRetryUtil.retryUntilTrue(
        () -> {
          final Set<String> actualIntervals = new HashSet<>(coordinator.getSegmentIntervals(fullDatasourceName));
          System.out.println("ACTUAL: " + actualIntervals);
          System.out.println("EXPECTED: " + expectedIntervalsSet);
          return actualIntervals.equals(expectedIntervalsSet);
        },
        "Compaction interval check"
    );
  }

  private void verifySegmentsCompacted(int expectedCompactedSegmentCount, Integer expectedMaxRowsPerSegment)
  {
    verifySegmentsCompacted(
        new DynamicPartitionsSpec(expectedMaxRowsPerSegment, Long.MAX_VALUE),
        expectedCompactedSegmentCount
    );
  }

  private void verifySegmentsCompacted(PartitionsSpec partitionsSpec, int expectedCompactedSegmentCount)
  {
    List<DataSegment> segments = coordinator.getFullSegmentsMetadata(fullDatasourceName);
    List<DataSegment> foundCompactedSegments = new ArrayList<>();
    for (DataSegment segment : segments) {
      if (segment.getLastCompactionState() != null) {
        foundCompactedSegments.add(segment);
      }
    }
    Assert.assertEquals(foundCompactedSegments.size(), expectedCompactedSegmentCount);
    for (DataSegment compactedSegment : foundCompactedSegments) {
      Assert.assertNotNull(compactedSegment.getLastCompactionState());
      Assert.assertNotNull(compactedSegment.getLastCompactionState().getPartitionsSpec());
      Assert.assertEquals(compactedSegment.getLastCompactionState().getPartitionsSpec(), partitionsSpec);
    }
  }

  private void updateCompactionTaskSlot(double compactionTaskSlotRatio, int maxCompactionTaskSlots) throws Exception
  {
    compactionResource.updateCompactionTaskSlot(compactionTaskSlotRatio, maxCompactionTaskSlots);
    // Verify that the compaction config is updated correctly.
    CoordinatorCompactionConfig coordinatorCompactionConfig = compactionResource.getCoordinatorCompactionConfigs();
    Assert.assertEquals(coordinatorCompactionConfig.getCompactionTaskSlotRatio(), compactionTaskSlotRatio);
    Assert.assertEquals(coordinatorCompactionConfig.getMaxCompactionTaskSlots(), maxCompactionTaskSlots);
  }

  private void getAndAssertCompactionStatus(
      String fullDatasourceName,
      AutoCompactionSnapshot.AutoCompactionScheduleStatus scheduleStatus,
      long bytesAwaitingCompaction,
      long bytesCompacted,
      long bytesSkipped,
      long segmentCountAwaitingCompaction,
      long segmentCountCompacted,
      long segmentCountSkipped,
      long intervalCountAwaitingCompaction,
      long intervalCountCompacted,
      long intervalCountSkipped
  ) throws Exception
  {
    Map<String, String> actualStatus = compactionResource.getCompactionStatus(fullDatasourceName);
    Assert.assertNotNull(actualStatus);
    Assert.assertEquals(actualStatus.get("scheduleStatus"), scheduleStatus.toString());
    Assert.assertEquals(Long.parseLong(actualStatus.get("bytesAwaitingCompaction")), bytesAwaitingCompaction);
    Assert.assertEquals(Long.parseLong(actualStatus.get("bytesCompacted")), bytesCompacted);
    Assert.assertEquals(Long.parseLong(actualStatus.get("bytesSkipped")), bytesSkipped);
    Assert.assertEquals(Long.parseLong(actualStatus.get("segmentCountAwaitingCompaction")), segmentCountAwaitingCompaction);
    Assert.assertEquals(Long.parseLong(actualStatus.get("segmentCountCompacted")), segmentCountCompacted);
    Assert.assertEquals(Long.parseLong(actualStatus.get("segmentCountSkipped")), segmentCountSkipped);
    Assert.assertEquals(Long.parseLong(actualStatus.get("intervalCountAwaitingCompaction")), intervalCountAwaitingCompaction);
    Assert.assertEquals(Long.parseLong(actualStatus.get("intervalCountCompacted")), intervalCountCompacted);
    Assert.assertEquals(Long.parseLong(actualStatus.get("intervalCountSkipped")), intervalCountSkipped);
  }
}
