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
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CompactionResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractITBatchIndexTest;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Test(groups = {TestNGGroup.OTHER_INDEX})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAutoCompactionTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITAutoCompactionTest.class);
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final int MAX_ROWS_PER_SEGMENT_COMPACTED = 10000;
  private static final Period SKIP_OFFSET_FROM_LATEST = Period.seconds(0);

  @Inject
  protected CompactionResourceTestClient compactionResource;

  @Inject
  private IntegrationTestingConfig config;

  private String fullDatasourceName;

  @BeforeMethod
  public void setup() throws Exception
  {
    // Set comapction slot to 10
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

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, SKIP_OFFSET_FROM_LATEST);
      //...compacted into 1 new segment for the remaining one day. 2 day compacted and 0 day uncompacted. (2 total)
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      checkCompactionIntervals(intervalsBeforeCompaction);
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
      submitCompactionConfig(10000, SKIP_OFFSET_FROM_LATEST);
      // New compaction config should overwrites the existing compaction config
      submitCompactionConfig(1, SKIP_OFFSET_FROM_LATEST);

      // Instead of merging segments, the updated config will split segments!
      //...compacted into 10 new segments across 2 days. 5 new segments each day (10 total)
      forceTriggerAutoCompaction(10);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(10, 1);

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

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, SKIP_OFFSET_FROM_LATEST);
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

      submitCompactionConfig(MAX_ROWS_PER_SEGMENT_COMPACTED, SKIP_OFFSET_FROM_LATEST);
      // ...should remains unchanged (4 total)
      forceTriggerAutoCompaction(4);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(0, null);
      checkCompactionIntervals(intervalsBeforeCompaction);

      // Update compaction slots to be 1
      updateCompactionTaskSlot(1, 1);
      // One day compacted (1 new segment) and one day remains uncompacted. (3 total)
      forceTriggerAutoCompaction(3);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
      checkCompactionIntervals(intervalsBeforeCompaction);
      Assert.assertEquals(compactionResource.getCompactionProgress(fullDatasourceName).get("remainingSegmentSize"), "14312");
      // Run compaction again to compact the remaining day
      // Remaining day compacted (1 new segment). Now both days compacted (2 total)
      forceTriggerAutoCompaction(2);
      verifyQuery(INDEX_QUERIES_RESOURCE);
      verifySegmentsCompacted(2, MAX_ROWS_PER_SEGMENT_COMPACTED);
      checkCompactionIntervals(intervalsBeforeCompaction);
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

    queryHelper.testQueriesFromString(queryResponseTemplate, 2);
  }

  private void submitCompactionConfig(Integer maxRowsPerSegment, Period skipOffsetFromLatest) throws Exception
  {
    DataSourceCompactionConfig compactionConfig = new DataSourceCompactionConfig(fullDatasourceName,
                                                                                 null,
                                                                                 null,
                                                                                 maxRowsPerSegment,
                                                                                 skipOffsetFromLatest,
                                                                                 null,
                                                                                 null);
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
    Assert.assertEquals(foundDataSourceCompactionConfig.getMaxRowsPerSegment(), maxRowsPerSegment);
    Assert.assertEquals(foundDataSourceCompactionConfig.getSkipOffsetFromLatest(), skipOffsetFromLatest);

    foundDataSourceCompactionConfig = compactionResource.getDataSourceCompactionConfig(fullDatasourceName);
    Assert.assertNotNull(foundDataSourceCompactionConfig);
    Assert.assertEquals(foundDataSourceCompactionConfig.getMaxRowsPerSegment(), maxRowsPerSegment);
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
    ITRetryUtil.retryUntilTrue(
        () -> {
          final List<String> actualIntervals = coordinator.getSegmentIntervals(fullDatasourceName);
          actualIntervals.sort(null);
          return actualIntervals.equals(expectedIntervals);
        },
        "Compaction interval check"
    );
  }

  private void verifySegmentsCompacted(int expectedCompactedSegmentCount, Integer expectedMaxRowsPerSegment)
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
      Assert.assertEquals(compactedSegment.getLastCompactionState().getPartitionsSpec().getMaxRowsPerSegment(),
                          expectedMaxRowsPerSegment);
      Assert.assertEquals(compactedSegment.getLastCompactionState().getPartitionsSpec().getType(),
                          SecondaryPartitionType.LINEAR
      );
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
}
