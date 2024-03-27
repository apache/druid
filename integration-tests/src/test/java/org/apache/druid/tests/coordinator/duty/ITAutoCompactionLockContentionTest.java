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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Intervals;
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
  private static final Logger log = new Logger(ITAutoCompactionLockContentionTest.class);

  @Inject
  private CompactionResourceTestClient compactionResource;

  private GeneratedTestConfig generatedTestConfig;
  private StreamGenerator streamGenerator;

  private String fullDatasourceName;

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

  public void testAutoCompactionSkipsLockedIntervals() throws Exception
  {
    final boolean transactionEnabled = isKafkaTransactionEnabled();
    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      // Start supervisor
      final String taskSpec = generatedTestConfig.getStreamIngestionPropsTransform()
                                                 .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      log.info("supervisorSpec: [%s]", taskSpec);

      // Generate data for minutes 1, 2 and 3
      final Interval minute1 = Intervals.of("2000-01-01T01:01:00Z/2000-01-01T01:02:00Z");
      final long rowsForMinute1 = generateData(minute1, streamEventWriter);

      final Interval minute2 = Intervals.of("2000-01-01T01:02:00Z/2000-01-01T01:03:00Z");
      long rowsForMinute2 = generateData(minute2, streamEventWriter);

      final Interval minute3 = Intervals.of("2000-01-01T01:03:00Z/2000-01-01T01:04:00Z");
      final long rowsForMinute3 = generateData(minute3, streamEventWriter);

      // Wait for data to be ingested for all the minutes
      waitUntilDatasourceHasTotalRows(rowsForMinute1 + rowsForMinute2 + rowsForMinute3);

      // Wait for the segments to be loaded and interval locks to be released
      waitUntilLockedIntervalsAre();
      waitUntilDatasourceIsLoaded();

      // 2 segments for each minute, total 6
      waitUntilDatasourceHasNumSegments(6);

      // Generate more data for minute2 so that it gets locked
      rowsForMinute2 += generateData(minute2, streamEventWriter);
      waitUntilLockedIntervalsAre(minute2);

      // Trigger auto compaction
      submitAndVerifyCompactionConfig();
      compactionResource.forceTriggerAutoCompaction();

      // Wait for segments to be loaded
      waitUntilDatasourceHasTotalRows(rowsForMinute1 + rowsForMinute2 + rowsForMinute3);
      waitUntilLockedIntervalsAre();
      waitUntilDatasourceIsLoaded();

      // Verify that minute1 and minute3 have been compacted
      waitUntilNumCompletedCompactionTasksIs(2);
      verifyCompactedIntervalsAre(minute1, minute3);

      // Trigger auto compaction again
      compactionResource.forceTriggerAutoCompaction();

      // Verify that all the segments are now compacted
      waitUntilNumCompletedCompactionTasksIs(3);
      waitUntilDatasourceIsLoaded();
      verifyCompactedIntervalsAre(minute1, minute2, minute3);
      waitUntilDatasourceHasNumSegments(3);
    }
  }

  private void waitUntilDatasourceHasNumSegments(int numExpectedSegments)
  {
    ITRetryUtil.retryUntilEquals(
        () -> coordinator.getFullSegmentsMetadata(fullDatasourceName)
                         .stream().map(DataSegment::getId).count(),
        numExpectedSegments,
        "Num segments in datasource[%s]",
        fullDatasourceName
    );
  }

  private void verifyCompactedIntervalsAre(Interval... compactedIntervals)
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
    log.info("Generated [%d] rows for data for interval[%s]", rowCount, interval);

    return rowCount;
  }

  private void waitUntilDatasourceIsLoaded()
  {
    ITRetryUtil.retryUntilEquals(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Load status of datasource[%s]",
        fullDatasourceName
    );
  }

  /**
   * Retries until the specified Intervals are locked for the current datasource.
   * If no interval has been specified, retries until no interval is locked
   */
  private void waitUntilLockedIntervalsAre(Interval... intervals)
  {
    final Map<String, Integer> minTaskPriority = Collections.singletonMap(fullDatasourceName, 0);
    final Set<Interval> expectedLockedIntervals = Sets.newHashSet(intervals);

    ITRetryUtil.retryUntilEquals(
        () -> {
          Map<String, List<Interval>> allLockedIntervals = indexer.getLockedIntervals(minTaskPriority);
          return new HashSet<>(allLockedIntervals.get(fullDatasourceName));
        },
        expectedLockedIntervals,
        "Locked intervals for datasource[%s]",
        fullDatasourceName
    );

    Assert.assertEquals(expectedLockedIntervals, Arrays.asList(intervals));
  }

  /**
   * Checks if Kafka transaction is enabled for this test run.
   */
  private boolean isKafkaTransactionEnabled()
  {
    Map<String, String> kafkaTestProps = KafkaUtil
        .getAdditionalKafkaTestConfigFromProperties(config);
    return Boolean.parseBoolean(
        kafkaTestProps.getOrDefault(KafkaUtil.TEST_CONFIG_TRANSACTION_ENABLED, "false")
    );
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

  private void waitUntilNumCompletedCompactionTasksIs(int expectedCount)
  {
    ITRetryUtil.retryUntilEquals(
        this::getNumCompleteCompactionTasks,
        expectedCount,
        "Completed compaction tasks"
    );
  }

  private long getNumCompleteCompactionTasks()
  {
    List<TaskResponseObject> incompleteTasks = indexer
        .getUncompletedTasksForDataSource(fullDatasourceName);
    List<TaskResponseObject> completeTasks = indexer
        .getCompleteTasksForDataSource(fullDatasourceName);

    printTasks(incompleteTasks, "Incomplete");
    printTasks(completeTasks, "Complete");

    return completeTasks.stream()
                        .filter(response -> "compact".equalsIgnoreCase(response.getType()))
                        .count();
  }

  private void printTasks(List<TaskResponseObject> tasks, String taskState)
  {
    StringBuilder sb = new StringBuilder();
    tasks.forEach(
        task -> sb.append("{")
                  .append(task.getType())
                  .append(", ").append(task.getStatus())
                  .append(", ").append(task.getCreatedTime())
                  .append("}, ")
    );
    log.info("%s Tasks: %s", taskState, sb);
  }

  private void waitUntilDatasourceHasTotalRows(long totalExpectedRows)
  {
    ITRetryUtil.retryUntilEquals(
        () -> queryHelper.countRows(
            fullDatasourceName,
            Intervals.ETERNITY,
            name -> new LongSumAggregatorFactory(name, "count")
        ),
        totalExpectedRows,
        "Rows in datasource[%s]",
        fullDatasourceName
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
