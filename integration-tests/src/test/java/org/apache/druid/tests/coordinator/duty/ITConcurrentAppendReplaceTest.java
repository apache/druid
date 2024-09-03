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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.testing.clients.CompactionResourceTestClient;
import org.apache.druid.testing.clients.TaskResponseObject;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.EventSerializer;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.KafkaUtil;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testing.utils.StreamEventWriter;
import org.apache.druid.testing.utils.StreamGenerator;
import org.apache.druid.testing.utils.WikipediaStreamEventStreamGenerator;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractKafkaIndexingServiceTest;
import org.apache.druid.tests.indexer.AbstractStreamIndexingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Integration Test to verify behaviour when there are concurrent
 * compaction tasks with ongoing stream ingestion tasks.
 */
@Test(groups = {TestNGGroup.COMPACTION})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITConcurrentAppendReplaceTest extends AbstractKafkaIndexingServiceTest
{
  private static final Logger LOG = new Logger(ITConcurrentAppendReplaceTest.class);

  @Inject
  private CompactionResourceTestClient compactionResource;
  
  @Inject
  private MsqTestQueryHelper msqHelper;

  private GeneratedTestConfig generatedTestConfig;
  private StreamGenerator streamGenerator;

  private String fullDatasourceName;

  private int currentRowCount = 0;
  private boolean concurrentAppendAndReplaceLocksExisted;

  @DataProvider
  public static Object[] getKafkaTransactionStatesForTest()
  {
    return new Object[]{false, true};
  }

  @BeforeClass
  public void setupClass() throws Exception
  {
    doBeforeClass();
  }

  @BeforeMethod
  public void setup() throws Exception
  {
    generatedTestConfig = new GeneratedTestConfig(
        Specs.PARSER_TYPE,
        getResourceAsString(Specs.INPUT_FORMAT_PATH)
    );
    fullDatasourceName = generatedTestConfig.getFullDatasourceName();
    final EventSerializer serializer = jsonMapper.readValue(
        getResourceAsStream(Specs.SERIALIZER_PATH),
        EventSerializer.class
    );
    streamGenerator = new WikipediaStreamEventStreamGenerator(serializer, 6, 100);
    concurrentAppendAndReplaceLocksExisted = false;
  }

  @Override
  public String getTestNamePrefix()
  {
    return "autocompact_lock_contention";
  }

  @Test(dataProvider = "getKafkaTransactionStatesForTest")
  public void testConcurrentStreamAppendReplace(boolean transactionEnabled) throws Exception
  {
    if (shouldSkipTest(transactionEnabled)) {
      return;
    }

    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      // Start supervisor
      final String taskSpec
          = generatedTestConfig.getStreamIngestionPropsTransform()
                               .apply(getResourceAsString(SUPERVISOR_WITH_CONCURRENT_LOCKS_SPEC_TEMPLATE_PATH));
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      LOG.info("supervisorSpec: [%s]", taskSpec);

      // Generate data for minutes 1, 2 and 3
      final Interval minute1 = Intervals.of("2000-01-01T01:01:00Z/2000-01-01T01:02:00Z");
      long rowsForMinute1 = generateData(minute1, streamEventWriter);

      final Interval minute2 = Intervals.of("2000-01-01T01:02:00Z/2000-01-01T01:03:00Z");
      long rowsForMinute2 = generateData(minute2, streamEventWriter);

      final Interval minute3 = Intervals.of("2000-01-01T01:03:00Z/2000-01-01T01:04:00Z");
      long rowsForMinute3 = generateData(minute3, streamEventWriter);

      Function<String, AggregatorFactory> function = name -> new LongSumAggregatorFactory(name, "count");

      // Wait for data to be ingested for all the minutes
      ensureRowCount(fullDatasourceName, rowsForMinute1 + rowsForMinute2 + rowsForMinute3, function);

      ensureSegmentsLoaded(fullDatasourceName);

      // 2 segments for each minute, total 6
      ensureSegmentsCount(fullDatasourceName, 6);

      // Trigger auto compaction
      submitAndVerifyCompactionConfig(fullDatasourceName, null);
      compactionResource.forceTriggerAutoCompaction();

      // Verify that all the segments are now compacted
      ensureRowCount(fullDatasourceName, rowsForMinute1 + rowsForMinute2 + rowsForMinute3, function);
      ensureSegmentsLoaded(fullDatasourceName);
      ensureSegmentsCount(fullDatasourceName, 3);
      verifyCompactedIntervals(fullDatasourceName, minute1, minute2, minute3);

      // Concurrent compaction with configured segment granularity
      for (int i = 0; i < 5; i++) {
        rowsForMinute1 += generateData(minute1, streamEventWriter);
        rowsForMinute2 += generateData(minute2, streamEventWriter);
        rowsForMinute3 += generateData(minute3, streamEventWriter);

        compactionResource.forceTriggerAutoCompaction();
        ensureRowCount(fullDatasourceName, rowsForMinute1 + rowsForMinute2 + rowsForMinute3, function);
        checkAndSetConcurrentLocks(fullDatasourceName);
      }

      // Verify the state with minute granularity
      ensureSegmentsCount(fullDatasourceName, 3);
      ensureSegmentsLoaded(fullDatasourceName);
      ensureRowCount(fullDatasourceName, rowsForMinute1 + rowsForMinute2 + rowsForMinute3, function);
      verifyCompactedIntervals(fullDatasourceName, minute1, minute2, minute3);


      // Use ALL segment granularity for compaction and run concurrent streaming ingestion
      submitAndVerifyCompactionConfig(fullDatasourceName, Granularities.DAY);

      for (int i = 0; i < 5; i++) {
        rowsForMinute1 += generateData(minute1, streamEventWriter);
        rowsForMinute2 += generateData(minute2, streamEventWriter);
        rowsForMinute3 += generateData(minute3, streamEventWriter);

        compactionResource.forceTriggerAutoCompaction();
        ensureRowCount(fullDatasourceName, rowsForMinute1 + rowsForMinute2 + rowsForMinute3, function);
        checkAndSetConcurrentLocks(fullDatasourceName);
      }

      // Verify the state with all granularity
      ensureSegmentsCount(fullDatasourceName, 1);
      ensureSegmentsLoaded(fullDatasourceName);
      ensureRowCount(fullDatasourceName, rowsForMinute1 + rowsForMinute2 + rowsForMinute3, function);
      verifyCompactedIntervals(fullDatasourceName, Intervals.of("2000-01-01/2000-01-02"));

      Assert.assertTrue(concurrentAppendAndReplaceLocksExisted);
    }
  }

    /**
     * Retries until the segment count is as expected.
     */
  private void ensureSegmentsCount(String datasource, int numExpectedSegments)
  {
    ITRetryUtil.retryUntilTrue(
        () -> {
          compactionResource.forceTriggerAutoCompaction();
          printTaskStatuses(datasource);
          checkAndSetConcurrentLocks(datasource);
          List<DataSegment> segments = coordinator.getFullSegmentsMetadata(datasource);
          StringBuilder sb = new StringBuilder();
          segments.forEach(
              seg -> sb.append("{")
                       .append(seg.getId())
                       .append(", ")
                       .append(seg.getSize())
                       .append("}, ")
          );
          LOG.info("Found Segments: %s", sb);
          LOG.info("Current metadata segment count: %d, expected: %d", segments.size(), numExpectedSegments);
          return segments.size() == numExpectedSegments;
        },
        "Segment count check"
    );
  }

  /**
   * Verifies that the given intervals have been compacted.
   */
  private void verifyCompactedIntervals(String datasource, Interval... compactedIntervals)
  {
    List<DataSegment> segments = coordinator.getFullSegmentsMetadata(datasource);
    List<DataSegment> observedCompactedSegments = new ArrayList<>();
    Set<Interval> observedCompactedIntervals = new HashSet<>();
    for (DataSegment segment : segments) {
      if (segment.getLastCompactionState() != null) {
        observedCompactedSegments.add(segment);
        observedCompactedIntervals.add(segment.getInterval());
      }
    }

    Set<Interval> expectedCompactedIntervals = new HashSet<>(Arrays.asList(compactedIntervals));
    Assert.assertEquals(observedCompactedIntervals, expectedCompactedIntervals);

    DynamicPartitionsSpec expectedPartitionSpec = new DynamicPartitionsSpec(
        PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT,
        Long.MAX_VALUE
    );
    for (DataSegment compactedSegment : observedCompactedSegments) {
      Assert.assertNotNull(compactedSegment.getLastCompactionState());
      Assert.assertEquals(
          compactedSegment.getLastCompactionState().getPartitionsSpec(),
          expectedPartitionSpec
      );
    }
  }

  /**
   * Generates data points for the specified interval.
   *
   * @return Number of rows generated.
   */
  private long generateData(Interval interval, StreamEventWriter streamEventWriter)
  {
    long rowCount = streamGenerator.run(
        generatedTestConfig.getStreamName(),
        streamEventWriter,
        20,
        interval.getStart()
    );
    LOG.info("Generated %d Rows for Interval [%s]", rowCount, interval);

    return rowCount;
  }

  /**
   * Retries until segments have been loaded.
   */
  private void ensureSegmentsLoaded(String datasource)
  {
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(datasource),
        "Segment Loading"
    );
  }

  private void checkAndSetConcurrentLocks(String datasource)
  {
    LockFilterPolicy lockFilterPolicy = new LockFilterPolicy(datasource, 0, null, null);
    final List<TaskLock> locks = indexer.getActiveLocks(ImmutableList.of(lockFilterPolicy))
                                        .getDatasourceToLocks()
                                        .get(datasource);
    if (CollectionUtils.isNullOrEmpty(locks)) {
      return;
    }
    Set<Interval> replaceIntervals = new HashSet<>();
    Set<Interval> appendIntervals = new HashSet<>();
    for (TaskLock lock : locks) {
      if (lock.getType() == TaskLockType.APPEND) {
        appendIntervals.add(lock.getInterval());
      } else if (lock.getType() == TaskLockType.REPLACE) {
        replaceIntervals.add(lock.getInterval());
      }
    }

    for (Interval appendInterval : appendIntervals) {
      for (Interval replaceInterval : replaceIntervals) {
        if (appendInterval.overlaps(replaceInterval)) {
          concurrentAppendAndReplaceLocksExisted = true;
          return;
        }
      }
    }
  }

  /**
   * Checks if a test should be skipped based on whether transaction is enabled or not.
   */
  private boolean shouldSkipTest(boolean testEnableTransaction)
  {
    Map<String, String> kafkaTestProps = KafkaUtil
        .getAdditionalKafkaTestConfigFromProperties(config);
    boolean configEnableTransaction = Boolean.parseBoolean(
        kafkaTestProps.getOrDefault(KafkaUtil.TEST_CONFIG_TRANSACTION_ENABLED, "false")
    );

    return configEnableTransaction != testEnableTransaction;
  }

  /**
   * Submits a compaction config for the current datasource.
   */
  private void submitAndVerifyCompactionConfig(String datasource, Granularity segmentGranularity) throws Exception
  {
    final UserCompactionTaskGranularityConfig granularitySpec =
        new UserCompactionTaskGranularityConfig(segmentGranularity, null, null);
    final DataSourceCompactionConfig compactionConfig =
        DataSourceCompactionConfig.builder()
                                  .forDataSource(datasource)
                                  .withSkipOffsetFromLatest(Period.ZERO)
                                  .withGranularitySpec(granularitySpec)
                                  .withTaskContext(ImmutableMap.of(Tasks.USE_CONCURRENT_LOCKS, true))
                                  .build();
    compactionResource.updateCompactionTaskSlot(0.5, 10, null);
    compactionResource.submitCompactionConfig(compactionConfig);

    // Wait for compaction config to persist
    Thread.sleep(2000);

    // Verify that the compaction config is updated correctly.
    DruidCompactionConfig druidCompactionConfig = compactionResource.getCompactionConfig();
    DataSourceCompactionConfig observedCompactionConfig = null;
    for (DataSourceCompactionConfig dataSourceCompactionConfig : druidCompactionConfig.getCompactionConfigs()) {
      if (dataSourceCompactionConfig.getDataSource().equals(datasource)) {
        observedCompactionConfig = dataSourceCompactionConfig;
      }
    }
    Assert.assertEquals(observedCompactionConfig, compactionConfig);

    observedCompactionConfig = compactionResource.getDataSourceCompactionConfig(datasource);
    Assert.assertEquals(observedCompactionConfig, compactionConfig);
  }

  /**
   * Gets the number of complete compaction tasks.
   */
  private void printTaskStatuses(String datsource)
  {
    List<TaskResponseObject> incompleteTasks = indexer
        .getUncompletedTasksForDataSource(datsource);
    List<TaskResponseObject> completeTasks = indexer
        .getCompleteTasksForDataSource(datsource);

    printTasks(incompleteTasks, "Incomplete");
    printTasks(completeTasks, "Complete");
  }

  private void printTasks(List<TaskResponseObject> tasks, String taskState)
  {
    StringBuilder sb = new StringBuilder();
    tasks.forEach(
        task -> sb.append("{")
                  .append(task.getType())
                  .append(", ")
                  .append(task.getStatus())
                  .append(", ")
                  .append(task.getCreatedTime())
                  .append("}, ")
    );
    LOG.info("%s Tasks: %s", taskState, sb);
  }

  /**
   * Retries until the total row count is as expected.
   * Also verifies that row count is non-decreasing
   */
  private void ensureRowCount(String datasource, long totalRows, Function<String, AggregatorFactory> function)
  {
    LOG.info("Verifying Row Count. Expected: %s", totalRows);
    ITRetryUtil.retryUntilTrue(
        () -> {
          compactionResource.forceTriggerAutoCompaction();
          printTaskStatuses(datasource);
          checkAndSetConcurrentLocks(datasource);
          int newRowCount = this.queryHelper.countRows(
              datasource,
              Intervals.ETERNITY,
              function
          );
          Assert.assertTrue(newRowCount >= currentRowCount, "Number of events queried must be non decreasing");
          currentRowCount = newRowCount;
          return currentRowCount == totalRows;
        },
        StringUtils.format(
            "dataSource[%s] has [%,d] queryable rows, expected [%,d]",
            datasource,
            this.queryHelper.countRows(
                datasource,
                Intervals.ETERNITY,
                function
            ),
            totalRows
        )
    );
  }

  /**
   * Constants for test specs.
   */
  private static class Specs
  {
    static final String SERIALIZER_PATH = DATA_RESOURCE_ROOT + "/csv/serializer/serializer.json";
    static final String INPUT_FORMAT_PATH = DATA_RESOURCE_ROOT + "/csv/input_format/input_format.json";
    static final String PARSER_TYPE = AbstractStreamIndexingTest.INPUT_FORMAT;
  }

}
