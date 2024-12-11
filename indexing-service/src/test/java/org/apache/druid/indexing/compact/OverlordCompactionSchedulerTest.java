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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.indexing.ClientMSQContext;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.compaction.Table;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.CompactionSupervisorConfig;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OverlordCompactionSchedulerTest
{
  private static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new DefaultObjectMapper();
    OBJECT_MAPPER.registerModules(new IndexingServiceTuningConfigModule().getJacksonModules());
    OBJECT_MAPPER.setInjectableValues(
        new InjectableValues
            .Std()
            .addValue(
                SegmentCacheManagerFactory.class,
                new SegmentCacheManagerFactory(TestIndex.INDEX_IO, OBJECT_MAPPER)
            )
    );
  }

  private CompactionSupervisorConfig supervisorConfig;
  private DruidCompactionConfig compactionConfig;
  private CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig;

  private TaskMaster taskMaster;
  private TaskQueue taskQueue;
  private BlockingExecutorService executor;

  private HeapMemoryTaskStorage taskStorage;
  private TestSegmentsMetadataManager segmentsMetadataManager;
  private StubServiceEmitter serviceEmitter;

  private OverlordCompactionScheduler scheduler;

  @Before
  public void setUp()
  {
    final TaskRunner taskRunner = Mockito.mock(TaskRunner.class);
    taskQueue = Mockito.mock(TaskQueue.class);

    taskMaster = new TaskMaster(null, null);
    Assert.assertFalse(taskMaster.isHalfOrFullLeader());
    Assert.assertFalse(taskMaster.isFullLeader());

    taskMaster.becomeHalfLeader(taskRunner, taskQueue);
    Assert.assertTrue(taskMaster.isHalfOrFullLeader());
    Assert.assertFalse(taskMaster.isFullLeader());

    taskMaster.becomeFullLeader();
    Assert.assertTrue(taskMaster.isHalfOrFullLeader());
    Assert.assertTrue(taskMaster.isFullLeader());

    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));

    executor = new BlockingExecutorService("test");
    serviceEmitter = new StubServiceEmitter();
    segmentsMetadataManager = new TestSegmentsMetadataManager();

    supervisorConfig = new CompactionSupervisorConfig(true, null);
    compactionConfig = DruidCompactionConfig.empty();
    coordinatorOverlordServiceConfig = new CoordinatorOverlordServiceConfig(false, null);

    initScheduler();
  }

  private void initScheduler()
  {
    TaskLockbox taskLockbox = new TaskLockbox(taskStorage, new TestIndexerMetadataStorageCoordinator());
    WorkerBehaviorConfig defaultWorkerConfig
        = new DefaultWorkerBehaviorConfig(WorkerBehaviorConfig.DEFAULT_STRATEGY, null);
    scheduler = new OverlordCompactionScheduler(
        taskMaster,
        new TaskQueryTool(taskStorage, taskLockbox, taskMaster, null, () -> defaultWorkerConfig),
        segmentsMetadataManager,
        () -> compactionConfig,
        new CompactionStatusTracker(OBJECT_MAPPER),
        supervisorConfig,
        coordinatorOverlordServiceConfig,
        (nameFormat, numThreads) -> new WrappingScheduledExecutorService("test", executor, false),
        serviceEmitter,
        OBJECT_MAPPER
    );
  }

  @Test
  public void testStartStopWhenSchedulerIsEnabled()
  {
    supervisorConfig = new CompactionSupervisorConfig(true, null);
    Assert.assertFalse(scheduler.isRunning());

    scheduler.start();
    Assert.assertTrue(scheduler.isRunning());
    Assert.assertTrue(executor.hasPendingTasks());
    scheduler.stop();
    Assert.assertFalse(scheduler.isRunning());
    Assert.assertTrue(executor.hasPendingTasks());

    scheduler.start();
    Assert.assertTrue(scheduler.isRunning());
    scheduler.stop();
    Assert.assertFalse(scheduler.isRunning());
  }

  @Test
  public void testStartStopWhenScheduledIsDisabled()
  {
    supervisorConfig = new CompactionSupervisorConfig(false, null);
    initScheduler();

    Assert.assertFalse(scheduler.isRunning());
    scheduler.start();
    Assert.assertFalse(scheduler.isRunning());
    Assert.assertFalse(executor.hasPendingTasks());
    scheduler.stop();
    Assert.assertFalse(scheduler.isRunning());
    Assert.assertFalse(executor.hasPendingTasks());
  }

  @Test
  public void testSegmentsAreNotPolledWhenSchedulerIsDisabled()
  {
    supervisorConfig = new CompactionSupervisorConfig(false, null);
    initScheduler();

    verifySegmentPolling(false);
  }

  @Test
  public void testSegmentsArePolledWhenRunningInStandaloneMode()
  {
    coordinatorOverlordServiceConfig = new CoordinatorOverlordServiceConfig(false, null);
    initScheduler();

    verifySegmentPolling(true);
  }

  @Test
  public void testSegmentsAreNotPolledWhenRunningInCoordinatorMode()
  {
    coordinatorOverlordServiceConfig = new CoordinatorOverlordServiceConfig(true, "overlord");
    initScheduler();

    verifySegmentPolling(false);
  }

  private void verifySegmentPolling(boolean enabled)
  {
    scheduler.start();
    Assert.assertEquals(enabled, segmentsMetadataManager.isPollingDatabasePeriodically());

    scheduler.stop();
    Assert.assertFalse(segmentsMetadataManager.isPollingDatabasePeriodically());
  }

  @Test
  public void testNullCompactionConfigIsInvalid()
  {
    final CompactionConfigValidationResult result = scheduler.validateCompactionConfig(null);
    Assert.assertFalse(result.isValid());
    Assert.assertEquals("Cannot be null", result.getReason());
  }

  @Test
  public void testMsqCompactionConfigWithOneMaxTasksIsInvalid()
  {
    final DataSourceCompactionConfig datasourceConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withEngine(CompactionEngine.MSQ)
        .withTaskContext(Collections.singletonMap(ClientMSQContext.CTX_MAX_NUM_TASKS, 1))
        .build();

    final CompactionConfigValidationResult result = scheduler.validateCompactionConfig(datasourceConfig);
    Assert.assertFalse(result.isValid());
    Assert.assertEquals(
        "MSQ: Context maxNumTasks[1] must be at least 2 (1 controller + 1 worker)",
        result.getReason()
    );
  }

  @Test
  public void testStartCompactionForDatasource()
  {
    final List<DataSegment> wikiSegments = CreateDataSegments.ofDatasource(TestDataSource.WIKI).eachOfSizeInMb(100);
    wikiSegments.forEach(segmentsMetadataManager::addSegment);

    scheduler.start();
    scheduler.startCompaction(
        TestDataSource.WIKI,
        DataSourceCompactionConfig.builder()
                                  .forDataSource(TestDataSource.WIKI)
                                  .withSkipOffsetFromLatest(Period.seconds(0))
                                  .build()
    );

    executor.finishNextPendingTask();

    ArgumentCaptor<Task> taskArgumentCaptor = ArgumentCaptor.forClass(Task.class);
    Mockito.verify(taskQueue, Mockito.times(1)).add(taskArgumentCaptor.capture());

    Task submittedTask = taskArgumentCaptor.getValue();
    Assert.assertNotNull(submittedTask);
    Assert.assertTrue(submittedTask instanceof CompactionTask);

    final CompactionTask compactionTask = (CompactionTask) submittedTask;
    Assert.assertEquals(TestDataSource.WIKI, compactionTask.getDataSource());

    final AutoCompactionSnapshot.Builder expectedSnapshot = AutoCompactionSnapshot.builder(TestDataSource.WIKI);
    expectedSnapshot.incrementCompactedStats(CompactionStatistics.create(100_000_000, 1, 1));

    Assert.assertEquals(
        expectedSnapshot.build(),
        scheduler.getCompactionSnapshot(TestDataSource.WIKI)
    );
    Assert.assertEquals(
        Collections.singletonMap(TestDataSource.WIKI, expectedSnapshot.build()),
        scheduler.getAllCompactionSnapshots()
    );

    serviceEmitter.verifyValue(Stats.Compaction.SUBMITTED_TASKS.getMetricName(), 1L);
    serviceEmitter.verifyValue(Stats.Compaction.COMPACTED_BYTES.getMetricName(), 100_000_000L);

    scheduler.stop();
  }

  @Test
  public void testStopCompactionForDatasource()
  {
    final List<DataSegment> wikiSegments = CreateDataSegments.ofDatasource(TestDataSource.WIKI).eachOfSizeInMb(100);
    wikiSegments.forEach(segmentsMetadataManager::addSegment);

    scheduler.start();
    scheduler.startCompaction(
        TestDataSource.WIKI,
        DataSourceCompactionConfig.builder()
                                  .forDataSource(TestDataSource.WIKI)
                                  .withSkipOffsetFromLatest(Period.seconds(0))
                                  .build()
    );
    scheduler.stopCompaction(TestDataSource.WIKI);

    executor.finishNextPendingTask();

    Mockito.verify(taskQueue, Mockito.never()).add(ArgumentMatchers.any());

    Assert.assertNull(scheduler.getCompactionSnapshot(TestDataSource.WIKI));
    Assert.assertTrue(scheduler.getAllCompactionSnapshots().isEmpty());

    serviceEmitter.verifyNotEmitted(Stats.Compaction.SUBMITTED_TASKS.getMetricName());
    serviceEmitter.verifyNotEmitted(Stats.Compaction.COMPACTED_BYTES.getMetricName());

    scheduler.stop();
  }

  @Test
  public void testRunSimulation()
  {
    final List<DataSegment> wikiSegments = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .forIntervals(1, Granularities.DAY)
        .startingAt("2013-01-01")
        .withNumPartitions(10)
        .eachOfSizeInMb(100);
    wikiSegments.forEach(segmentsMetadataManager::addSegment);

    scheduler.start();
    scheduler.startCompaction(
        TestDataSource.WIKI,
        DataSourceCompactionConfig.builder()
                                  .forDataSource(TestDataSource.WIKI)
                                  .withSkipOffsetFromLatest(Period.seconds(0))
                                  .build()
    );

    final CompactionSimulateResult simulateResult = scheduler.simulateRunWithConfigUpdate(
        new ClusterCompactionConfig(null, null, null, null)
    );
    Assert.assertEquals(1, simulateResult.getCompactionStates().size());
    final Table pendingCompactionTable = simulateResult.getCompactionStates().get(CompactionStatus.State.PENDING);
    Assert.assertEquals(
        Arrays.asList("dataSource", "interval", "numSegments", "bytes", "maxTaskSlots", "reasonToCompact"),
        pendingCompactionTable.getColumnNames()
    );
    Assert.assertEquals(
        Collections.singletonList(
            Arrays.asList(
                TestDataSource.WIKI,
                Intervals.of("2013-01-01/P1D"),
                10,
                1_000_000_000L,
                1,
                "not compacted yet"
            )
        ),
        pendingCompactionTable.getRows()
    );

    scheduler.stopCompaction(TestDataSource.WIKI);

    final CompactionSimulateResult simulateResultWhenDisabled = scheduler.simulateRunWithConfigUpdate(
        new ClusterCompactionConfig(null, null, null, null)
    );
    Assert.assertTrue(simulateResultWhenDisabled.getCompactionStates().isEmpty());

    scheduler.stop();
  }

}
