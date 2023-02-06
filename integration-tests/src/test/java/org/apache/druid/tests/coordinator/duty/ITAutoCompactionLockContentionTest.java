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
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.testing.clients.CompactionResourceTestClient;
import org.apache.druid.testing.clients.TaskResponseObject;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.CompactionUtil;
import org.apache.druid.testing.utils.EventSerializer;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.KafkaUtil;
import org.apache.druid.testing.utils.StreamEventWriter;
import org.apache.druid.testing.utils.StreamGenerator;
import org.apache.druid.testing.utils.WikipediaStreamEventStreamGenerator;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractKafkaIndexingServiceTest;
import org.apache.druid.tests.indexer.AbstractStreamIndexingTest;
import org.apache.druid.timeline.DataSegment;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration Test to verify behaviour when there is a lock contention between
 * compaction tasks and on-going stream ingestion tasks.
 */
@Test(groups = {TestNGGroup.COMPACTION})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAutoCompactionLockContentionTest extends AbstractKafkaIndexingServiceTest
{
  private static final Logger LOG = new Logger(ITAutoCompactionLockContentionTest.class);

  @Inject
  private CompactionResourceTestClient compactionResource;

  private GeneratedTestConfig generatedTestConfig;
  private StreamGenerator streamGenerator;

  private String fullDatasourceName;

  @DataProvider
  public static Object[] getParameters()
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
  }

  @Override
  public String getTestNamePrefix()
  {
    return "autocompact_lock_contention";
  }

  @Test(dataProvider = "getParameters")
  public void testAutoCompactionSkipsLockedIntervals(boolean transactionEnabled) throws Exception
  {
    if (shouldSkipTest(transactionEnabled)) {
      return;
    }

    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      // Start supervisor
      final String taskSpec = generatedTestConfig.getStreamIngestionPropsTransform()
                                                 .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      LOG.info("supervisorSpec: [%s]", taskSpec);

      // Generate data for minutes 1, 2 and 3
      final Interval minute1 = Intervals.of("2000-01-01T01:01:00Z/2000-01-01T01:02:00Z");
      final long rowsForMinute1 = generateData(minute1, streamEventWriter);

      final Interval minute2 = Intervals.of("2000-01-01T01:02:00Z/2000-01-01T01:03:00Z");
      long rowsForMinute2 = generateData(minute2, streamEventWriter);

      final Interval minute3 = Intervals.of("2000-01-01T01:03:00Z/2000-01-01T01:04:00Z");
      final long rowsForMinute3 = generateData(minute3, streamEventWriter);

      // Wait for data to be ingested for all the minutes
      ensureRowCount(rowsForMinute1 + rowsForMinute2 + rowsForMinute3);

      // Wait for the segments to be loaded and interval locks to be released
      ensureLockedIntervals();
      ensureSegmentsLoaded();

      // 2 segments for each minute, total 6
      ensureSegmentsCount(6);

      // Generate more data for minute2 so that it gets locked
      rowsForMinute2 += generateData(minute2, streamEventWriter);
      ensureLockedIntervals(minute2);

      // Trigger auto compaction
      submitAndVerifyCompactionConfig();
      compactionResource.forceTriggerAutoCompaction();

      // Wait for segments to be loaded
      ensureRowCount(rowsForMinute1 + rowsForMinute2 + rowsForMinute3);
      ensureLockedIntervals();
      ensureSegmentsLoaded();

      // Verify that minute1 and minute3 have been compacted
      ensureCompactionTaskCount(2);
      verifyCompactedIntervals(minute1, minute3);

      // Trigger auto compaction again
      compactionResource.forceTriggerAutoCompaction();

      // Verify that all the segments are now compacted
      ensureCompactionTaskCount(3);
      ensureSegmentsLoaded();
      verifyCompactedIntervals(minute1, minute2, minute3);
      ensureSegmentsCount(3);
    }
  }

  /**
   * Retries until the segment count is as expected.
   */
  private void ensureSegmentsCount(int numExpectedSegments)
  {
    ITRetryUtil.retryUntilTrue(
        () -> {
          List<DataSegment> segments = coordinator.getFullSegmentsMetadata(fullDatasourceName);
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
  private void verifyCompactedIntervals(Interval... compactedIntervals)
  {
    List<DataSegment> segments = coordinator.getFullSegmentsMetadata(fullDatasourceName);
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
        Specs.MAX_ROWS_PER_SEGMENT,
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
        10,
        interval.getStart()
    );
    LOG.info("Generated %d Rows for Interval [%s]", rowCount, interval);

    return rowCount;
  }

  /**
   * Retries until segments have been loaded.
   */
  private void ensureSegmentsLoaded()
  {
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment Loading"
    );
  }

  /**
   * Retries until the specified Intervals are locked for the current datasource.
   * If no interval has been specified, retries until no interval is locked
   */
  private void ensureLockedIntervals(Interval... intervals)
  {
    final Map<String, Integer> minTaskPriority = Collections.singletonMap(fullDatasourceName, 0);
    final List<Interval> lockedIntervals = new ArrayList<>();
    ITRetryUtil.retryUntilTrue(
        () -> {
          lockedIntervals.clear();

          Map<String, List<Interval>> allIntervals = indexer.getLockedIntervals(minTaskPriority);
          if (allIntervals.containsKey(fullDatasourceName)) {
            lockedIntervals.addAll(allIntervals.get(fullDatasourceName));
          }

          LOG.info("Locked intervals: %s", lockedIntervals);
          return intervals.length == lockedIntervals.size();
        },
        "Verify Locked Intervals"
    );

    Assert.assertEquals(lockedIntervals, Arrays.asList(intervals));
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
  private void submitAndVerifyCompactionConfig() throws Exception
  {
    final DataSourceCompactionConfig compactionConfig = CompactionUtil
        .createCompactionConfig(fullDatasourceName, Specs.MAX_ROWS_PER_SEGMENT, Period.ZERO);
    compactionResource.updateCompactionTaskSlot(0.5, 10, null);
    compactionResource.submitCompactionConfig(compactionConfig);

    // Wait for compaction config to persist
    Thread.sleep(2000);

    // Verify that the compaction config is updated correctly.
    CoordinatorCompactionConfig coordinatorCompactionConfig = compactionResource.getCoordinatorCompactionConfigs();
    DataSourceCompactionConfig observedCompactionConfig = null;
    for (DataSourceCompactionConfig dataSourceCompactionConfig : coordinatorCompactionConfig.getCompactionConfigs()) {
      if (dataSourceCompactionConfig.getDataSource().equals(fullDatasourceName)) {
        observedCompactionConfig = dataSourceCompactionConfig;
      }
    }
    Assert.assertEquals(observedCompactionConfig, compactionConfig);

    observedCompactionConfig = compactionResource.getDataSourceCompactionConfig(fullDatasourceName);
    Assert.assertEquals(observedCompactionConfig, compactionConfig);
  }

  /**
   * Checks if the given TaskResponseObject represents a Compaction Task.
   */
  private boolean isCompactionTask(TaskResponseObject taskResponse)
  {
    return "compact".equalsIgnoreCase(taskResponse.getType());
  }

  /**
   * Retries until the total number of complete compaction tasks is as expected.
   */
  private void ensureCompactionTaskCount(int expectedCount)
  {
    LOG.info("Verifying compaction task count. Expected: %d", expectedCount);
    ITRetryUtil.retryUntilTrue(
        () -> getCompactionTaskCount() == expectedCount,
        "Compaction Task Count"
    );
  }

  /**
   * Gets the number of complete compaction tasks.
   */
  private long getCompactionTaskCount()
  {
    List<TaskResponseObject> incompleteTasks = indexer
        .getUncompletedTasksForDataSource(fullDatasourceName);
    List<TaskResponseObject> completeTasks = indexer
        .getCompleteTasksForDataSource(fullDatasourceName);

    printTasks(incompleteTasks, "Incomplete");
    printTasks(completeTasks, "Complete");

    return completeTasks.stream().filter(this::isCompactionTask).count();
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
   */
  private void ensureRowCount(long totalRows)
  {
    LOG.info("Verifying Row Count. Expected: %s", totalRows);
    ITRetryUtil.retryUntilTrue(
        () ->
            totalRows == this.queryHelper.countRows(
                fullDatasourceName,
                Intervals.ETERNITY,
                name -> new LongSumAggregatorFactory(name, "count")
            ),
        StringUtils.format(
            "dataSource[%s] consumed [%,d] events, expected [%,d]",
            fullDatasourceName,
            this.queryHelper.countRows(
                fullDatasourceName,
                Intervals.ETERNITY,
                name -> new LongSumAggregatorFactory(name, "count")
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

    static final int MAX_ROWS_PER_SEGMENT = 10000;
  }

}
