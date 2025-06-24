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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager.BasicState;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager.SeekableStreamExceptionEvent;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager.SeekableStreamState;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SeekableStreamSupervisorStateTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String DATASOURCE = "testDS";
  private static final String SUPERVISOR_ID = "testSupervisorId";
  private static final String STREAM = "stream";
  private static final String SHARD_ID = "0";
  private static final StreamPartition<String> SHARD0_PARTITION = StreamPartition.of(STREAM, SHARD_ID);
  private static final String EXCEPTION_MSG = "I had an exception";
  private static final Map<String, Object> METRIC_TAGS = ImmutableMap.of("k1", "v1", "k2", 20);
  private static final int DEFAULT_WORKER_THREADS = 2;
  private static final int DEFAULT_TASKS_PER_WORKER_THREAD = 4;

  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private TaskQueue taskQueue;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private SeekableStreamIndexTaskClientFactory taskClientFactory;
  private SeekableStreamSupervisorSpec spec;
  private SeekableStreamIndexTaskClient indexTaskClient;
  private RecordSupplier<String, String, ByteEntity> recordSupplier;
  private SeekableStreamIndexTaskRunner<String, String, ByteEntity> streamingTaskRunner;

  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private SupervisorStateManagerConfig supervisorConfig;

  private StubServiceEmitter emitter;

  @Before
  public void setupTest()
  {
    taskStorage = createMock(TaskStorage.class);
    taskMaster = createMock(TaskMaster.class);
    taskRunner = createMock(TaskRunner.class);
    taskQueue = createMock(TaskQueue.class);
    indexerMetadataStorageCoordinator = createMock(IndexerMetadataStorageCoordinator.class);
    taskClientFactory = createMock(SeekableStreamIndexTaskClientFactory.class);
    spec = createMock(SeekableStreamSupervisorSpec.class);
    indexTaskClient = createMock(SeekableStreamIndexTaskClient.class);
    recordSupplier = createMock(RecordSupplier.class);
    streamingTaskRunner = createMock(SeekableStreamIndexTaskRunner.class);

    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();

    supervisorConfig = new SupervisorStateManagerConfig();

    emitter = new StubServiceEmitter("test-supervisor-state", "localhost");
    EmittingLogger.registerEmitter(emitter);

    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR_ID).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(createSupervisorIOConfig()).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getContextValue(DruidMetrics.TAGS)).andReturn(METRIC_TAGS).anyTimes();

    EasyMock.expect(taskClientFactory.build(
        EasyMock.anyString(),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject()
    )).andReturn(
        indexTaskClient).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    EasyMock.expectLastCall().times(0, 1);

    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)).andReturn(null).anyTimes();
    EasyMock.expect(recordSupplier.getAssignment()).andReturn(ImmutableSet.of(SHARD0_PARTITION)).anyTimes();
    EasyMock.expect(recordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("10").anyTimes();
  }

  @Test
  public void testRunning()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testRunningStreamGetSequenceNumberReturnsNull()
  {
    EasyMock.reset(recordSupplier);
    EasyMock.expect(recordSupplier.getAssignment()).andReturn(ImmutableSet.of(SHARD0_PARTITION)).anyTimes();
    EasyMock.expect(recordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CREATING_TASKS, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    List<SupervisorStateManager.ExceptionEvent> exceptionEvents = supervisor.stateManager.getExceptionEvents();
    Assert.assertEquals(1, exceptionEvents.size());
    Assert.assertFalse(((SeekableStreamExceptionEvent) exceptionEvents.get(0)).isStreamException());
    Assert.assertEquals(ISE.class.getName(), exceptionEvents.get(0).getExceptionClass());
    Assert.assertEquals(StringUtils.format("unable to fetch sequence number for partition[%s] from stream", SHARD_ID), exceptionEvents.get(0).getMessage());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CREATING_TASKS, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(2, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testAddDiscoveredTaskToPendingCompletionTaskGroups() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();
    ExecutorService threadExecutor = Execs.multiThreaded(3, "my-thread-pool-%d");

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    Map<String, String> startingPartitions = new HashMap<>();
    startingPartitions.put("partition", "offset");

    // Test concurrent threads adding to same task group
    Callable<Boolean> task1 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_1", startingPartitions);
      return true;
    };
    Callable<Boolean> task2 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_2", startingPartitions);
      return true;
    };
    Callable<Boolean> task3 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_3", startingPartitions);
      return true;
    };

    // Create a list to hold the Callable tasks
    List<Callable<Boolean>> tasks = new ArrayList<>();
    tasks.add(task1);
    tasks.add(task2);
    tasks.add(task3);
    List<Future<Boolean>> futures = threadExecutor.invokeAll(tasks);
    // Wait for all tasks to complete
    for (Future<Boolean> future : futures) {
      try {
        Boolean result = future.get();
        Assert.assertTrue(result);
      }
      catch (ExecutionException e) {
        Assert.fail();
      }
    }
    CopyOnWriteArrayList<SeekableStreamSupervisor.TaskGroup> taskGroups = supervisor.getPendingCompletionTaskGroups(0);
    Assert.assertEquals(1, taskGroups.size());
    Assert.assertEquals(3, taskGroups.get(0).tasks.size());

    // Test concurrent threads adding to different task groups
    task1 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(1, "task_1", startingPartitions);
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(1, "task_1", startingPartitions);
      return true;
    };
    task2 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(2, "task_1", startingPartitions);
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(2, "task_1", startingPartitions);
      return true;
    };
    task3 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(1, "task_2", startingPartitions);
      return true;
    };
    Callable<Boolean> task4 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(2, "task_2", startingPartitions);
      return true;
    };
    Callable<Boolean> task5 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(1, "task_3", startingPartitions);
      return true;
    };
    Callable<Boolean> task6 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(1, "task_1", startingPartitions);
      return true;
    };

    tasks = new ArrayList<>();
    tasks.add(task1);
    tasks.add(task2);
    tasks.add(task3);
    tasks.add(task4);
    tasks.add(task5);
    tasks.add(task6);
    futures = threadExecutor.invokeAll(tasks);
    for (Future<Boolean> future : futures) {
      try {
        Boolean result = future.get();
        Assert.assertTrue(result);
      }
      catch (ExecutionException e) {
        Assert.fail();
      }
    }

    taskGroups = supervisor.getPendingCompletionTaskGroups(1);
    Assert.assertEquals(1, taskGroups.size());
    Assert.assertEquals(3, taskGroups.get(0).tasks.size());

    taskGroups = supervisor.getPendingCompletionTaskGroups(2);
    Assert.assertEquals(1, taskGroups.size());
    Assert.assertEquals(2, taskGroups.get(0).tasks.size());
  }

  @Test
  public void testAddDiscoveredTaskToPendingCompletionMultipleTaskGroups() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();

    // Test adding tasks with same task group and different partition offsets.
    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    ExecutorService threadExecutor = Execs.multiThreaded(3, "my-thread-pool-%d");
    Map<String, String> startingPartiions = new HashMap<>();
    startingPartiions.put("partition", "offset");

    Map<String, String> startingPartiions1 = new HashMap<>();
    startingPartiions.put("partition", "offset1");

    Callable<Boolean> task1 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_1", startingPartiions);
      return true;
    };
    Callable<Boolean> task2 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_2", startingPartiions);
      return true;
    };
    Callable<Boolean> task3 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_3", startingPartiions);
      return true;
    };
    Callable<Boolean> task4 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_7", startingPartiions1);
      return true;
    };
    Callable<Boolean> task5 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_8", startingPartiions1);
      return true;
    };
    Callable<Boolean> task6 = () -> {
      supervisor.addDiscoveredTaskToPendingCompletionTaskGroups(0, "task_9", startingPartiions1);
      return true;
    };

    List<Callable<Boolean>> tasks = new ArrayList<>();
    tasks.add(task1);
    tasks.add(task2);
    tasks.add(task3);
    tasks.add(task4);
    tasks.add(task5);
    tasks.add(task6);

    List<Future<Boolean>> futures = threadExecutor.invokeAll(tasks);

    for (Future<Boolean> future : futures) {
      try {
        Boolean result = future.get();
        Assert.assertTrue(result);
      }
      catch (ExecutionException e) {
        Assert.fail();
      }
    }

    CopyOnWriteArrayList<SeekableStreamSupervisor.TaskGroup> taskGroups = supervisor.getPendingCompletionTaskGroups(0);

    Assert.assertEquals(2, taskGroups.size());
    Assert.assertEquals(3, taskGroups.get(0).tasks.size());
    Assert.assertEquals(3, taskGroups.get(1).tasks.size());
  }

  @Test
  public void testConnectingToStreamFail()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM))
            .andThrow(new StreamException(new IllegalStateException(EXCEPTION_MSG)))
            .anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    List<SupervisorStateManager.ExceptionEvent> exceptionEvents = supervisor.stateManager.getExceptionEvents();
    Assert.assertEquals(1, exceptionEvents.size());
    Assert.assertTrue(((SeekableStreamExceptionEvent) exceptionEvents.get(0)).isStreamException());
    Assert.assertEquals(IllegalStateException.class.getName(), exceptionEvents.get(0).getExceptionClass());
    Assert.assertEquals(
        StringUtils.format("%s: %s", IllegalStateException.class.getName(), EXCEPTION_MSG),
        exceptionEvents.get(0).getMessage()
    );
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(2, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.UNABLE_TO_CONNECT_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testConnectingToStreamFailRecoveryFailRecovery()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM))
            .andThrow(new StreamException(new IllegalStateException()))
            .times(3);
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).times(3);
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM))
            .andThrow(new StreamException(new IllegalStateException()))
            .times(3);
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).times(3);
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(ImmutableList.of()).anyTimes();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.UNABLE_TO_CONNECT_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertEquals(SeekableStreamState.UNABLE_TO_CONNECT_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    supervisor.runInternal();
    Assert.assertEquals(SeekableStreamState.UNABLE_TO_CONNECT_TO_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    supervisor.runInternal();
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    supervisor.runInternal();
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    supervisor.runInternal();
    Assert.assertEquals(SeekableStreamState.LOST_CONTACT_WITH_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.LOST_CONTACT_WITH_STREAM, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.LOST_CONTACT_WITH_STREAM, supervisor.stateManager.getSupervisorState());
    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testDiscoveringInitialTasksFailRecoveryFail()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andThrow(new IllegalStateException(EXCEPTION_MSG)).times(3);
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).times(6);
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andThrow(new IllegalStateException(EXCEPTION_MSG)).times(3);
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    supervisor.start();

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    List<SupervisorStateManager.ExceptionEvent> exceptionEvents = supervisor.stateManager.getExceptionEvents();
    Assert.assertEquals(1, exceptionEvents.size());
    Assert.assertFalse(((SeekableStreamExceptionEvent) exceptionEvents.get(0)).isStreamException());
    Assert.assertEquals(IllegalStateException.class.getName(), exceptionEvents.get(0).getExceptionClass());
    Assert.assertEquals(EXCEPTION_MSG, exceptionEvents.get(0).getMessage());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(2, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testIdleStateTransition() throws Exception
  {
    EasyMock.reset(spec);
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("PT1S"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        new IdleConfig(true, 200L),
        null
    )
    {
    }).anyTimes();
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR_ID).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig() {
      @Override
      public Duration getEmissionDuration()
      {
        return new Period("PT1S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("test").anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    supervisor.setStreamOffsets(ImmutableMap.of("0", "10"));
    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    Thread.sleep(100L);
    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    Thread.sleep(100L);
    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.IDLE, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.IDLE, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    Thread.sleep(100L);
    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.IDLE, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.IDLE, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testIdleOnStartUpAndTurnsToRunningAfterLagUpdates()
  {
    Map<String, String> initialOffsets = ImmutableMap.of("0", "10");
    Map<String, String> laterOffsets = ImmutableMap.of("0", "20");

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)).andReturn(
        new TestSeekableStreamDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                initialOffsets
            )
        )
    ).anyTimes();
    EasyMock.reset(spec);
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR_ID).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getContextValue("tags")).andReturn("").anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("PT1S"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        new IdleConfig(true, 200L),
        null
    )
    {
    }).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig()
    {
      @Override
      public Duration getEmissionDuration()
      {
        return new Period("PT1S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("test").anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(recordSupplier.isOffsetAvailable(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true)
            .anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();
    replayAll();

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.setStreamOffsets(initialOffsets);
    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.IDLE, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.IDLE, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.setStreamOffsets(laterOffsets);
    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());
  }

  @Test
  public void testCreatingTasksFailRecoveryFail()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andThrow(new IllegalStateException(EXCEPTION_MSG)).times(3);
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).times(3);
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andThrow(new IllegalStateException(EXCEPTION_MSG)).times(3);
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(ImmutableList.of()).anyTimes();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    supervisor.start();

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CREATING_TASKS, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    List<SupervisorStateManager.ExceptionEvent> exceptionEvents = supervisor.stateManager.getExceptionEvents();
    Assert.assertEquals(1, exceptionEvents.size());
    Assert.assertFalse(((SeekableStreamExceptionEvent) exceptionEvents.get(0)).isStreamException());
    Assert.assertEquals(IllegalStateException.class.getName(), exceptionEvents.get(0).getExceptionClass());
    Assert.assertEquals(EXCEPTION_MSG, exceptionEvents.get(0).getMessage());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(SeekableStreamState.CREATING_TASKS, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(2, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(3, supervisor.stateManager.getExceptionEvents().size());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();
    Assert.assertFalse(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, supervisor.stateManager.getSupervisorState());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testSuspended()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(true).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.SUSPENDED, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.SUSPENDED, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.SUSPENDED, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.SUSPENDED, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    verifyAll();
  }

  @Test
  public void testStopping()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    taskRunner.unregisterListener("testSupervisorId");
    indexTaskClient.close();
    recordSupplier.close();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.stop(false);

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.STOPPING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.STOPPING, supervisor.stateManager.getSupervisorState().getBasicState());

    verifyAll();
  }

  @Test
  public void testStopGracefully() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    taskRunner.unregisterListener("testSupervisorId");
    indexTaskClient.close();
    recordSupplier.close();

    replayAll();
    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.runInternal();
    ListenableFuture<Void> stopFuture = supervisor.stopAsync();
    stopFuture.get();
    verifyAll();
  }

  @Test
  public void testStoppingGracefully()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    taskRunner.unregisterListener("testSupervisorId");
    indexTaskClient.close();
    recordSupplier.close();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertTrue(supervisor.stateManager.isAtLeastOneSuccessfulRun());

    supervisor.stop(true);

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.STOPPING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.STOPPING, supervisor.stateManager.getSupervisorState().getBasicState());

    // Subsequent run after graceful shutdown has begun
    supervisor.runInternal();
    Assert.assertEquals(BasicState.STOPPING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.STOPPING, supervisor.stateManager.getSupervisorState().getBasicState());

    verifyAll();
  }

  @Test(timeout = 60_000L)
  public void testCheckpointForActiveTaskGroup() throws InterruptedException, JsonProcessingException
  {
    DateTime startTime = DateTimes.nowUtc();
    SeekableStreamSupervisorIOConfig ioConfig = new SeekableStreamSupervisorIOConfig(
        STREAM,
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("PT1S"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        new IdleConfig(true, 200L),
        null
    ) {};

    EasyMock.reset(spec);
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR_ID).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig() {
      @Override
      public Duration getEmissionDuration()
      {
        return new Period("PT2S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("stream").anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getContextValue("tags")).andReturn("").anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    SeekableStreamIndexTaskIOConfig taskIoConfig = createTaskIoConfigExt(
            0,
            Collections.singletonMap("0", "10"),
            Collections.singletonMap("0", "20"),
            "test",
            startTime,
            null,
            Collections.emptySet(),
            ioConfig
    );

    SeekableStreamIndexTaskTuningConfig taskTuningConfig = getTuningConfig().convertToTaskTuningConfig();

    TreeMap<Integer, Map<String, Long>> sequenceOffsets = new TreeMap<>();
    sequenceOffsets.put(0, ImmutableMap.of("0", 10L, "1", 20L));

    Map<String, Object> context = new HashMap<>();
    context.put("checkpoints", new ObjectMapper().writeValueAsString(sequenceOffsets));

    TestSeekableStreamIndexTask id1 = new TestSeekableStreamIndexTask(
            "id1",
            null,
            null,
            getDataSchema(),
            taskTuningConfig,
            taskIoConfig,
            context,
            "0"
    );

    TestSeekableStreamIndexTask id2 = new TestSeekableStreamIndexTask(
            "id2",
            null,
            null,
            getDataSchema(),
            taskTuningConfig,
            taskIoConfig,
            context,
            "0"
    );

    TestSeekableStreamIndexTask id3 = new TestSeekableStreamIndexTask(
        "id3",
        null,
        null,
        getDataSchema(),
        taskTuningConfig,
        taskIoConfig,
        context,
        "0"
    );

    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);
    final TaskLocation location2 = TaskLocation.create("testHost2", 145, -1);
    final TaskLocation location3 = TaskLocation.create("testHost3", 145, -1);

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));
    workItems.add(new TestTaskRunnerWorkItem(id3, null, location3));

    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE))
            .andReturn(Map.of(id1.getId(), id1, id2.getId(), id2, id3.getId(), id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id2)).anyTimes();

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID))
            .andReturn(new TestSeekableStreamDataSourceMetadata(null)).anyTimes();
    EasyMock.expect(indexTaskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.PAUSED))
            .anyTimes();

    EasyMock.expect(indexTaskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime)).anyTimes();
    EasyMock.expect(indexTaskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime)).anyTimes();
    EasyMock.expect(indexTaskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime)).anyTimes();

    ImmutableMap<String, String> partitionOffset = ImmutableMap.of("0", "10");
    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, partitionOffset);

    EasyMock.expect(indexTaskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .anyTimes();
    EasyMock.expect(indexTaskClient.pauseAsync("id1"))
            .andReturn(Futures.immediateFuture(partitionOffset))
            .anyTimes();
    EasyMock.expect(indexTaskClient.pauseAsync("id2"))
            .andReturn(Futures.immediateFuture(partitionOffset))
            .anyTimes();
    EasyMock.expect(indexTaskClient.pauseAsync("id3"))
            .andReturn(Futures.immediateFuture(partitionOffset))
            .anyTimes();
    EasyMock.expect(indexTaskClient.setEndOffsetsAsync("id1", partitionOffset, false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.setEndOffsetsAsync("id2", partitionOffset, false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.setEndOffsetsAsync("id3", partitionOffset, false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.resumeAsync("id1"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.resumeAsync("id2"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.resumeAsync("id3"))
            .andReturn(Futures.immediateFuture(false))
            .anyTimes();
    EasyMock.expect(indexTaskClient.stopAsync("id1", false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.stopAsync("id2", false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();

    taskQueue.shutdown(
        "id3",
        "Killing forcefully as task could not be resumed in the"
        + " first supervisor run after Overlord change."
    );
    EasyMock.expectLastCall().atLeastOnce();

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.runInternal();

    supervisor.checkpoint(
            0,
            new TestSeekableStreamDataSourceMetadata(
                    new SeekableStreamStartSequenceNumbers<>(STREAM, checkpoints.get(0), ImmutableSet.of())
            )
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();

    Assert.assertTrue(supervisor.getNoticesQueueSize() == 0);
  }

  @Test(timeout = 60_000L)
  public void testEarlyStoppingOfTaskGroupBasedOnStopTaskCount() throws InterruptedException, JsonProcessingException
  {
    // Assuming tasks have surpassed their duration limit at test execution
    DateTime startTime = DateTimes.nowUtc().minusHours(2);
    // Configure supervisor to stop only one task at a time
    int stopTaskCount = 1;
    SeekableStreamSupervisorIOConfig ioConfig = new SeekableStreamSupervisorIOConfig(
        STREAM,
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        3,
        new Period("PT1H"),
        new Period("PT1S"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        new IdleConfig(true, 200L),
        stopTaskCount
    )
    {
    };

    EasyMock.reset(spec);
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR_ID).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig()
    {
      @Override
      public Duration getEmissionDuration()
      {
        return new Period("PT2S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("stream").anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getContextValue("tags")).andReturn("").anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    SeekableStreamIndexTaskTuningConfig taskTuningConfig = getTuningConfig().convertToTaskTuningConfig();

    TreeMap<Integer, Map<String, Long>> sequenceOffsets = new TreeMap<>();
    sequenceOffsets.put(0, ImmutableMap.of("0", 10L, "1", 20L));

    Map<String, Object> context = new HashMap<>();
    context.put("checkpoints", new ObjectMapper().writeValueAsString(sequenceOffsets));

    TestSeekableStreamIndexTask id1 = new TestSeekableStreamIndexTask(
        "id1",
        null,
        null,
        getDataSchema(),
        taskTuningConfig,
        createTaskIoConfigExt(
            0,
            Collections.singletonMap("0", "10"),
            Collections.singletonMap("0", "20"),
            "test",
            startTime,
            null,
            Collections.emptySet(),
            ioConfig
        ),
        context,
        "0"
    );

    TestSeekableStreamIndexTask id2 = new TestSeekableStreamIndexTask(
        "id2",
        null,
        null,
        getDataSchema(),
        taskTuningConfig,
        createTaskIoConfigExt(
            1,
            Collections.singletonMap("1", "10"),
            Collections.singletonMap("1", "20"),
            "test",
            startTime,
            null,
            Collections.emptySet(),
            ioConfig
        ),
        context,
        "1"
    );

    TestSeekableStreamIndexTask id3 = new TestSeekableStreamIndexTask(
        "id3",
        null,
        null,
        getDataSchema(),
        taskTuningConfig,
        createTaskIoConfigExt(
            2,
            Collections.singletonMap("2", "10"),
            Collections.singletonMap("2", "20"),
            "test",
            startTime,
            null,
            Collections.emptySet(),
            ioConfig
        ),
        context,
        "2"
    );

    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);
    final TaskLocation location2 = TaskLocation.create("testHost2", 145, -1);
    final TaskLocation location3 = TaskLocation.create("testHost3", 145, -1);

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));
    workItems.add(new TestTaskRunnerWorkItem(id3, null, location3));

    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id1.getId())).andReturn(location1).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id2.getId())).andReturn(location2).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id3.getId())).andReturn(location3).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE))
            .andReturn(Map.of(id1.getId(), id1, id2.getId(), id2, id3.getId(), id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id2)).anyTimes();

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID))
            .andReturn(new TestSeekableStreamDataSourceMetadata(null)).anyTimes();
    EasyMock.expect(indexTaskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();

    EasyMock.expect(indexTaskClient.getStartTimeAsync("id1"))
            .andReturn(Futures.immediateFuture(startTime.plusSeconds(1)))
            .anyTimes();
    // Mocking to return the earliest start time for task id2, indicating it's the first group to start
    EasyMock.expect(indexTaskClient.getStartTimeAsync("id2"))
            .andReturn(Futures.immediateFuture(startTime)).anyTimes();
    EasyMock.expect(indexTaskClient.getStartTimeAsync("id3"))
            .andReturn(Futures.immediateFuture(startTime.plusSeconds(2)))
            .anyTimes();

    ImmutableMap<String, String> partitionOffset = ImmutableMap.of("0", "10");
    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, partitionOffset);

    EasyMock.expect(indexTaskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .anyTimes();
    EasyMock.expect(indexTaskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .anyTimes();
    EasyMock.expect(indexTaskClient.setEndOffsetsAsync("id1", partitionOffset, false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.setEndOffsetsAsync("id2", partitionOffset, false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.setEndOffsetsAsync("id3", partitionOffset, false))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.resumeAsync("id1"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.resumeAsync("id2"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.resumeAsync("id3"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.pauseAsync("id1"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.pauseAsync("id2"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.expect(indexTaskClient.pauseAsync("id3"))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();

    // Expect the earliest-started task (id2) to transition to publishing first
    taskQueue.shutdown("id2", "All tasks in group[%s] failed to transition to publishing state", 1);

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.runInternal();

    Assert.assertNull(id1.getCurrentRunnerStatus());

    supervisor.checkpoint(
        0,
        new TestSeekableStreamDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(STREAM, checkpoints.get(0), ImmutableSet.of())
        )
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();

    Assert.assertTrue(supervisor.getNoticesQueueSize() == 0);
  }

  @Test(timeout = 10_000L)
  public void testSupervisorStopTaskGroupEarly() throws JsonProcessingException, InterruptedException
  {
    DateTime startTime = DateTimes.nowUtc();
    SeekableStreamSupervisorIOConfig ioConfig = new SeekableStreamSupervisorIOConfig(
        STREAM,
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("PT1S"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        new IdleConfig(true, 200L),
        null
    )
    {
    };

    EasyMock.reset(spec);
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR_ID).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig()
    {
      @Override
      public Duration getEmissionDuration()
      {
        return new Period("PT2S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("stream").anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getContextValue("tags")).andReturn("").anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();
    EasyMock.expect(streamingTaskRunner.getStatus()).andReturn(null);
    EasyMock.expect(streamingTaskRunner.getStatus()).andReturn(SeekableStreamIndexTaskRunner.Status.NOT_STARTED);

    SeekableStreamIndexTaskTuningConfig taskTuningConfig = getTuningConfig().convertToTaskTuningConfig();

    TreeMap<Integer, Map<String, Long>> sequenceOffsets = new TreeMap<>();
    sequenceOffsets.put(0, ImmutableMap.of("0", 10L, "1", 20L));

    Map<String, Object> context = new HashMap<>();
    context.put("checkpoints", new ObjectMapper().writeValueAsString(sequenceOffsets));

    TestSeekableStreamIndexTask id1 = new TestSeekableStreamIndexTask(
        "id1",
        null,
        null,
        getDataSchema(),
        taskTuningConfig,
        createTaskIoConfigExt(
            0,
            Collections.singletonMap("0", "10"),
            Collections.singletonMap("0", "20"),
            "test",
            startTime,
            null,
            Collections.emptySet(),
            ioConfig
        ),
        context,
        "0",
        streamingTaskRunner
    );

    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));

    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id1.getId())).andReturn(location1).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE))
        .andReturn(Map.of(id1.getId(), id1))
        .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID))
        .andReturn(new TestSeekableStreamDataSourceMetadata(null)).anyTimes();
    EasyMock.expect(indexTaskClient.getStatusAsync("id1"))
        .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
        .anyTimes();

    EasyMock.expect(indexTaskClient.getStartTimeAsync("id1"))
        .andReturn(Futures.immediateFuture(startTime.plusSeconds(1)))
        .anyTimes();

    ImmutableMap<String, String> partitionOffset = ImmutableMap.of("0", "10");
    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, partitionOffset);

    EasyMock.expect(indexTaskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .anyTimes();

    // The task should only be pause/resumed in one of the runInternal commands, after stopTaskGroupEarly has been called.
    EasyMock.expect(indexTaskClient.resumeAsync("id1"))
        .andReturn(Futures.immediateFuture(true))
        .once();
    EasyMock.expect(indexTaskClient.pauseAsync("id1"))
        .andReturn(Futures.immediateFuture(true))
        .once();
    taskQueue.shutdown("id1", "All tasks in group[%s] failed to transition to publishing state", 0);

    replayAll();

    SeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.runInternal();
    supervisor.handoffTaskGroupsEarly(ImmutableList.of(0));

    Assert.assertNull(id1.getCurrentRunnerStatus());
    Assert.assertEquals("NOT_STARTED", id1.getCurrentRunnerStatus());

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testEmitBothLag() throws Exception
  {
    expectEmitterSupervisor(false);

    CountDownLatch latch = new CountDownLatch(1);
    TestEmittingTestSeekableStreamSupervisor supervisor = new TestEmittingTestSeekableStreamSupervisor(
        latch,
        TestEmittingTestSeekableStreamSupervisor.LAG,
        ImmutableMap.of("1", 100L, "2", 250L, "3", 500L),
        ImmutableMap.of("1", 10000L, "2", 15000L, "3", 20000L)
    );


    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());


    latch.await();

    final Map<String, Object> dimFilters = ImmutableMap.of(DruidMetrics.TAGS, METRIC_TAGS);
    emitter.verifyValue("ingest/test/lag", dimFilters, 850L);
    emitter.verifyValue("ingest/test/maxLag", dimFilters, 500L);
    emitter.verifyValue("ingest/test/avgLag", dimFilters, 283L);
    emitter.verifyValue("ingest/test/lag/time", dimFilters, 45000L);
    emitter.verifyValue("ingest/test/maxLag/time", dimFilters, 20000L);
    emitter.verifyValue("ingest/test/avgLag/time", dimFilters, 15000L);
    verifyAll();
  }

  @Test
  public void testEmitRecordLag() throws Exception
  {
    expectEmitterSupervisor(false);

    CountDownLatch latch = new CountDownLatch(1);
    TestEmittingTestSeekableStreamSupervisor supervisor = new TestEmittingTestSeekableStreamSupervisor(
        latch,
        TestEmittingTestSeekableStreamSupervisor.LAG,
        ImmutableMap.of("1", 100L, "2", 250L, "3", 500L),
        null
    );

    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());


    latch.await();

    final Map<String, Object> dimFilters = ImmutableMap.of(DruidMetrics.TAGS, METRIC_TAGS);
    emitter.verifyValue("ingest/test/lag", dimFilters, 850L);
    emitter.verifyValue("ingest/test/maxLag", dimFilters, 500L);
    emitter.verifyValue("ingest/test/avgLag", dimFilters, 283L);
    verifyAll();
  }

  @Test
  public void testEmitTimeLag() throws Exception
  {
    expectEmitterSupervisor(false);

    CountDownLatch latch = new CountDownLatch(1);
    TestEmittingTestSeekableStreamSupervisor supervisor = new TestEmittingTestSeekableStreamSupervisor(
        latch,
        TestEmittingTestSeekableStreamSupervisor.LAG,
        null,
        ImmutableMap.of("1", 10000L, "2", 15000L, "3", 20000L)
    );


    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());


    latch.await();

    final Map<String, Object> dimFilters = ImmutableMap.of(DruidMetrics.TAGS, METRIC_TAGS);
    emitter.verifyValue("ingest/test/lag/time", dimFilters, 45000L);
    emitter.verifyValue("ingest/test/maxLag/time", dimFilters, 20000L);
    emitter.verifyValue("ingest/test/avgLag/time", dimFilters, 15000L);

    verifyAll();
  }

  @Test
  public void testEmitNoticesQueueSize() throws Exception
  {
    expectEmitterSupervisor(false);

    CountDownLatch latch = new CountDownLatch(1);
    TestEmittingTestSeekableStreamSupervisor supervisor = new TestEmittingTestSeekableStreamSupervisor(
        latch,
        TestEmittingTestSeekableStreamSupervisor.NOTICE_QUEUE,
        null,
        null
    );


    supervisor.start();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());


    latch.await();

    final Map<String, Object> dimFilters = ImmutableMap.of(
        DruidMetrics.TAGS, METRIC_TAGS,
        DruidMetrics.DATASOURCE, "testDS"
    );
    emitter.verifyValue("ingest/notices/queueSize", dimFilters, 0);

    verifyAll();
  }

  @Test
  public void testEmitNoticesTime() throws Exception
  {
    expectEmitterSupervisor(false);
    CountDownLatch latch = new CountDownLatch(1);
    TestEmittingTestSeekableStreamSupervisor supervisor = new TestEmittingTestSeekableStreamSupervisor(
        latch,
        TestEmittingTestSeekableStreamSupervisor.NOTICE_PROCESS,
        null,
        null
    );
    supervisor.start();
    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());
    Assert.assertFalse(supervisor.stateManager.isAtLeastOneSuccessfulRun());
    latch.await();

    final Map<String, Object> dimFilters = ImmutableMap.of(
        DruidMetrics.TAGS, METRIC_TAGS,
        DruidMetrics.DATASOURCE, "testDS",
        "noticeType", "run_notice"
    );
    long observedNoticeTime = emitter.getValue("ingest/notices/time", dimFilters).longValue();
    Assert.assertTrue(observedNoticeTime > 0);

    verifyAll();
  }

  @Test
  public void testEmitNoLagWhenSuspended() throws Exception
  {
    expectEmitterSupervisor(true);

    CountDownLatch latch = new CountDownLatch(1);
    TestEmittingTestSeekableStreamSupervisor supervisor = new TestEmittingTestSeekableStreamSupervisor(
        latch,
        TestEmittingTestSeekableStreamSupervisor.LAG,
        ImmutableMap.of("1", 100L, "2", 250L, "3", 500L),
        ImmutableMap.of("1", 10000L, "2", 15000L, "3", 20000L)
    );


    supervisor.start();
    supervisor.runInternal();

    Assert.assertTrue(supervisor.stateManager.isHealthy());
    Assert.assertEquals(BasicState.SUSPENDED, supervisor.stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.SUSPENDED, supervisor.stateManager.getSupervisorState().getBasicState());
    Assert.assertTrue(supervisor.stateManager.getExceptionEvents().isEmpty());


    latch.await();

    emitter.verifyNotEmitted("ingest/test/lag");
    emitter.verifyNotEmitted("ingest/test/maxLag");
    emitter.verifyNotEmitted("ingest/test/avgLag");
    emitter.verifyNotEmitted("ingest/test/lag/time");
    emitter.verifyNotEmitted("ingest/test/maxLag/time");
    emitter.verifyNotEmitted("ingest/test/avgLag/time");

    verifyAll();
  }

  @Test
  public void testGetStats()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false);
    EasyMock.expect(indexTaskClient.getMovingAveragesAsync("task1"))
            .andReturn(Futures.immediateFuture(ImmutableMap.of("prop1", "val1")))
            .times(1);
    EasyMock.expect(indexTaskClient.getMovingAveragesAsync("task2"))
            .andReturn(Futures.immediateFuture(ImmutableMap.of("prop2", "val2")))
            .times(1);
    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "0"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "0"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );
    Map<String, Map<String, Object>> stats = supervisor.getStats();

    verifyAll();

    Assert.assertEquals(1, stats.size());
    Assert.assertEquals(ImmutableSet.of("0"), stats.keySet());
    Assert.assertEquals(
        ImmutableMap.of(
            "task1", ImmutableMap.of("prop1", "val1"),
            "task2", ImmutableMap.of("prop2", "val2")
        ),
        stats.get("0")
    );
  }

  @Test
  public void testSupervisorResetAllWithCheckpoints() throws InterruptedException
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false);
    EasyMock.expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(SUPERVISOR_ID)).andReturn(
        true
    );
    taskQueue.shutdown("task1", "DataSourceMetadata is not found while reset");
    EasyMock.expectLastCall();
    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "5"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "6"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    Assert.assertEquals(1, supervisor.getActiveTaskGroupsCount());
    Assert.assertEquals(0, supervisor.getNoticesQueueSize());
    Assert.assertEquals(0, supervisor.getPartitionOffsets().size());

    supervisor.reset(null);
    validateSupervisorStateAfterResetOffsets(supervisor, ImmutableMap.of(), 0);
  }

  @Test
  public void testSupervisorResetOneTaskSpecificOffsetsWithCheckpoints() throws InterruptedException, IOException
  {
    final ImmutableMap<String, String> checkpointOffsets = ImmutableMap.of("0", "0", "1", "10", "2", "20", "3", "30");
    final ImmutableMap<String, String> resetOffsets = ImmutableMap.of("0", "1000", "2", "2500");
    final ImmutableMap<String, String> expectedOffsets = ImmutableMap.of("0", "1000", "1", "10", "2", "2500", "3", "30");

    EasyMock.expect(spec.isSuspended()).andReturn(false);
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)).andReturn(
        new TestSeekableStreamDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                checkpointOffsets
            )
        )
    );
    EasyMock.expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(SUPERVISOR_ID, new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            expectedOffsets
        ))
    )).andReturn(
        true
    );

    taskQueue.shutdown("task1", "DataSourceMetadata is updated while reset offsets is called");
    EasyMock.expectLastCall();

    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        checkpointOffsets,
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    final DataSourceMetadata resetMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            resetOffsets
        )
    );

    Assert.assertEquals(1, supervisor.getActiveTaskGroupsCount());
    Assert.assertEquals(0, supervisor.getNoticesQueueSize());
    Assert.assertEquals(0, supervisor.getPartitionOffsets().size());

    supervisor.resetOffsets(resetMetadata);

    validateSupervisorStateAfterResetOffsets(supervisor, resetOffsets, 0);
  }

  @Test
  public void testRegisterNewVersionOfPendingSegment()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false);

    Capture<PendingSegmentRecord> captured0 = Capture.newInstance(CaptureType.FIRST);
    Capture<PendingSegmentRecord> captured1 = Capture.newInstance(CaptureType.FIRST);
    EasyMock.expect(
        indexTaskClient.registerNewVersionOfPendingSegmentAsync(EasyMock.eq("task0"), EasyMock.capture(captured0))
    ).andReturn(Futures.immediateFuture(true));
    EasyMock.expect(
        indexTaskClient.registerNewVersionOfPendingSegmentAsync(EasyMock.eq("task2"), EasyMock.capture(captured1))
    ).andReturn(Futures.immediateFuture(true));

    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    // Spin off two active tasks with each task serving one partition.
    supervisor.getIoConfig().setTaskCount(3);
    supervisor.start();

    final SeekableStreamSupervisor.TaskGroup taskGroup0 = supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "5"),
        null,
        null,
        ImmutableSet.of("task0"),
        ImmutableSet.of()
    );
    final SeekableStreamSupervisor.TaskGroup taskGroup1 = supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "6"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );
    final SeekableStreamSupervisor.TaskGroup taskGroup2 = supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("2"),
        ImmutableMap.of("2", "100"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    final PendingSegmentRecord pendingSegmentRecord0 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "DS",
            Intervals.of("2024/2025"),
            "2024",
            new NumberedShardSpec(1, 0)
        ),
        taskGroup0.getBaseSequenceName(),
        "prevId0",
        "someAppendedSegment0",
        taskGroup0.getBaseSequenceName()
    );
    final PendingSegmentRecord pendingSegmentRecord1 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "DS",
            Intervals.of("2024/2025"),
            "2024",
            new NumberedShardSpec(2, 0)
        ),
        taskGroup2.getBaseSequenceName(),
        "prevId1",
        "someAppendedSegment1",
        taskGroup2.getBaseSequenceName()
    );

    supervisor.registerNewVersionOfPendingSegment(pendingSegmentRecord0);
    supervisor.registerNewVersionOfPendingSegment(pendingSegmentRecord1);

    Assert.assertEquals(pendingSegmentRecord0, captured0.getValue());
    Assert.assertEquals(pendingSegmentRecord1, captured1.getValue());
    verifyAll();
  }

  @Test
  public void testSupervisorResetSpecificOffsetsTasksWithCheckpoints() throws InterruptedException, IOException
  {
    final ImmutableMap<String, String> checkpointOffsets = ImmutableMap.of("0", "5", "1", "6", "2", "100");
    final ImmutableMap<String, String> resetOffsets = ImmutableMap.of("0", "10", "1", "8");
    final ImmutableMap<String, String> expectedOffsets = ImmutableMap.of("0", "10", "1", "8", "2", "100");

    EasyMock.expect(spec.isSuspended()).andReturn(false);
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)).andReturn(
        new TestSeekableStreamDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                checkpointOffsets
            )
        )
    );
    EasyMock.expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(SUPERVISOR_ID, new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            expectedOffsets
        )
    ))).andReturn(true);
    taskQueue.shutdown("task1", "DataSourceMetadata is updated while reset offsets is called");
    EasyMock.expectLastCall();

    taskQueue.shutdown("task2", "DataSourceMetadata is updated while reset offsets is called");
    EasyMock.expectLastCall();

    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    // Spin off two active tasks with each task serving one partition.
    supervisor.getIoConfig().setTaskCount(3);
    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "5"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "6"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("2"),
        ImmutableMap.of("2", "100"),
        null,
        null,
        ImmutableSet.of("task3"),
        ImmutableSet.of()
    );

    final DataSourceMetadata resetMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            resetOffsets
        )
    );

    Assert.assertEquals(3, supervisor.getActiveTaskGroupsCount());
    Assert.assertEquals(0, supervisor.getNoticesQueueSize());
    Assert.assertEquals(0, supervisor.getPartitionOffsets().size());

    supervisor.resetOffsets(resetMetadata);

    validateSupervisorStateAfterResetOffsets(supervisor, resetOffsets, 1);
  }

  @Test
  public void testSupervisorResetOffsetsWithNoCheckpoints() throws InterruptedException
  {
    final ImmutableMap<String, String> resetOffsets = ImmutableMap.of("0", "10", "1", "8");
    final ImmutableMap<String, String> expectedOffsets = ImmutableMap.copyOf(resetOffsets);

    EasyMock.expect(spec.isSuspended()).andReturn(false);
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)).andReturn(null);
    EasyMock.expect(indexerMetadataStorageCoordinator.insertDataSourceMetadata(SUPERVISOR_ID, new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            expectedOffsets
        )
    ))).andReturn(true);
    taskQueue.shutdown("task1", "DataSourceMetadata is updated while reset offsets is called");
    EasyMock.expectLastCall();

    taskQueue.shutdown("task2", "DataSourceMetadata is updated while reset offsets is called");
    EasyMock.expectLastCall();

    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    // Spin off three active tasks with each task serving one partition.
    supervisor.getIoConfig().setTaskCount(3);
    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "5"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "6"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("2"),
        ImmutableMap.of("2", "100"),
        null,
        null,
        ImmutableSet.of("task3"),
        ImmutableSet.of()
    );

    final DataSourceMetadata resetMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            resetOffsets
        )
    );

    Assert.assertEquals(3, supervisor.getActiveTaskGroupsCount());
    Assert.assertEquals(0, supervisor.getNoticesQueueSize());
    Assert.assertEquals(0, supervisor.getPartitionOffsets().size());

    supervisor.resetOffsets(resetMetadata);

    validateSupervisorStateAfterResetOffsets(supervisor, resetOffsets, 1);
  }


  @Test
  public void testSupervisorResetWithNoPartitions() throws IOException, InterruptedException
  {
    final ImmutableMap<String, String> checkpointOffsets = ImmutableMap.of("0", "5", "1", "6");
    final ImmutableMap<String, String> resetOffsets = ImmutableMap.of();
    final ImmutableMap<String, String> expectedOffsets = ImmutableMap.of("0", "5", "1", "6");

    EasyMock.expect(spec.isSuspended()).andReturn(false);
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)).andReturn(
        new TestSeekableStreamDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                checkpointOffsets
            )
        )
    );
    EasyMock.expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(SUPERVISOR_ID, new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            expectedOffsets
        )
    ))).andReturn(true);

    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    // Spin off two active tasks with each task serving one partition.
    supervisor.getIoConfig().setTaskCount(2);
    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "5"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "6"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    final DataSourceMetadata resetMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            resetOffsets
        )
    );

    Assert.assertEquals(2, supervisor.getActiveTaskGroupsCount());
    Assert.assertEquals(0, supervisor.getNoticesQueueSize());
    Assert.assertEquals(0, supervisor.getPartitionOffsets().size());

    supervisor.resetOffsets(resetMetadata);

    validateSupervisorStateAfterResetOffsets(supervisor, resetOffsets, 2);
  }

  @Test
  public void testSupervisorResetWithNewPartition() throws IOException, InterruptedException
  {
    final ImmutableMap<String, String> checkpointOffsets = ImmutableMap.of("0", "5", "1", "6");
    final ImmutableMap<String, String> resetOffsets = ImmutableMap.of("2", "20");
    final ImmutableMap<String, String> expectedOffsets = ImmutableMap.of("0", "5", "1", "6", "2", "20");

    EasyMock.expect(spec.isSuspended()).andReturn(false);
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)).andReturn(
        new TestSeekableStreamDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                checkpointOffsets
            )
        )
    );
    EasyMock.expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(SUPERVISOR_ID, new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            expectedOffsets
        )
    ))).andReturn(true);
    taskQueue.shutdown("task1", "DataSourceMetadata is updated while reset offsets is called");
    EasyMock.expectLastCall();

    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    // Spin off two active tasks with each task serving one partition.
    supervisor.getIoConfig().setTaskCount(2);
    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "5"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "6"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    final DataSourceMetadata resetMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            resetOffsets
        )
    );

    Assert.assertEquals(2, supervisor.getActiveTaskGroupsCount());
    Assert.assertEquals(0, supervisor.getNoticesQueueSize());
    Assert.assertEquals(0, supervisor.getPartitionOffsets().size());

    supervisor.resetOffsets(resetMetadata);

    validateSupervisorStateAfterResetOffsets(supervisor, resetOffsets, 1);
  }

  @Test
  public void testSupervisorNoResetDataSourceMetadata()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false);
    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "0"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "0"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    verifyAll();

    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () ->
            supervisor.resetOffsets(null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Reset dataSourceMetadata is required for resetOffsets."
        )
    );
  }

  @Test
  public void testSupervisorResetWithInvalidStartSequenceMetadata()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false);
    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "0"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "0"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    verifyAll();

    final DataSourceMetadata dataSourceMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            "i-am-not-real",
            ImmutableMap.of("0", "10", "1", "20", "2", "30"),
            ImmutableSet.of()
        )
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () ->
            supervisor.resetOffsets(dataSourceMetadata)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            StringUtils.format(
                "Provided datasourceMetadata[%s] is invalid. Sequence numbers can only be of type[SeekableStreamEndSequenceNumbers], but found[SeekableStreamStartSequenceNumbers].",
                dataSourceMetadata
            )
        )
    );
  }

  @Test
  public void testSupervisorResetInvalidStream()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false);
    replayAll();

    final TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    supervisor.start();
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "0"),
        null,
        null,
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "0"),
        null,
        null,
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    verifyAll();

    final DataSourceMetadata dataSourceMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamEndSequenceNumbers<>(
            "i-am-not-real",
            ImmutableMap.of("0", "10", "1", "20", "2", "30")
        )
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () ->
            supervisor.resetOffsets(dataSourceMetadata)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Stream[i-am-not-real] doesn't exist in the supervisor[testSupervisorId]. Supervisor is consuming stream[stream]."
        )
    );
  }

  @Test
  public void testStaleOffsetsNegativeLagNotEmitted() throws Exception
  {
    expectEmitterSupervisor(false);

    CountDownLatch latch = new CountDownLatch(1);

    final TestEmittingTestSeekableStreamSupervisor supervisor = new TestEmittingTestSeekableStreamSupervisor(
        latch,
        TestEmittingTestSeekableStreamSupervisor.LAG,
        // Record lag must not be emitted
        ImmutableMap.of("0", 10L, "1", -100L),
        null
    );
    supervisor.start();
    // Forcibly set the offsets to be stale
    supervisor.sequenceLastUpdated = DateTimes.nowUtc().minus(Integer.MAX_VALUE);

    latch.await();

    supervisor.emitLag();
    Assert.assertEquals(0, emitter.getEvents().size());
  }

  private void validateSupervisorStateAfterResetOffsets(
      final TestSeekableStreamSupervisor supervisor,
      final ImmutableMap<String, String> expectedResetOffsets,
      final int expectedActiveTaskCount
  ) throws InterruptedException
  {
    // Wait for the notice queue to be drained asynchronously before we validate the supervisor's final state.
    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }
    Thread.sleep(1000);
    Assert.assertEquals(expectedActiveTaskCount, supervisor.getActiveTaskGroupsCount());
    Assert.assertEquals(expectedResetOffsets.size(), supervisor.getPartitionOffsets().size());
    for (Map.Entry<String, String> entry : expectedResetOffsets.entrySet()) {
      Assert.assertEquals(supervisor.getNotSetMarker(), supervisor.getPartitionOffsets().get(entry.getKey()));
    }
    verifyAll();
  }

  @Test
  public void testScheduleReporting()
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    DruidMonitorSchedulerConfig config = new DruidMonitorSchedulerConfig();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(config).times(2);
    ScheduledExecutorService executorService = EasyMock.createMock(ScheduledExecutorService.class);
    EasyMock.expect(executorService.scheduleWithFixedDelay(EasyMock.anyObject(), EasyMock.eq(86415000L), EasyMock.eq(300000L), EasyMock.eq(TimeUnit.MILLISECONDS))).andReturn(EasyMock.createMock(ScheduledFuture.class)).once();
    EasyMock.expect(executorService.scheduleAtFixedRate(EasyMock.anyObject(), EasyMock.eq(86425000L), EasyMock.eq(config.getEmissionDuration().getMillis()), EasyMock.eq(TimeUnit.MILLISECONDS))).andReturn(EasyMock.createMock(ScheduledFuture.class)).times(2);

    EasyMock.replay(executorService, spec);
    final BaseTestSeekableStreamSupervisor supervisor = new BaseTestSeekableStreamSupervisor()
    {
      @Override
      public LagStats computeLagStats()
      {
        return new LagStats(0, 0, 0);
      }
    };
    supervisor.scheduleReporting(executorService);
    EasyMock.verify(executorService, spec);
  }

  @Test
  public void test_calculateWorkerThreads_shouldHonourWorkerConfig()
  {
    final int numWorkerThreads = 5;
    final int taskCount = 1;
    SeekableStreamSupervisorTuningConfig tuningConfig = createSupervisorTuningConfigWithWorkerThreads(numWorkerThreads);
    SeekableStreamSupervisorIOConfig ioConfig = createSupervisorIOConfig(taskCount, null);
    Assert.assertEquals(numWorkerThreads, SeekableStreamSupervisor.calculateWorkerThreads(tuningConfig, ioConfig));
  }

  @Test
  public void test_calculateWorkerThreads_shouldUseDefaultWorkerThreads()
  {
    final int taskCount = 1;
    SeekableStreamSupervisorTuningConfig tuningConfig = createSupervisorTuningConfig();
    SeekableStreamSupervisorIOConfig ioConfig = createSupervisorIOConfig(taskCount, null);
    Assert.assertEquals(
        DEFAULT_WORKER_THREADS,
        SeekableStreamSupervisor.calculateWorkerThreads(tuningConfig, ioConfig)
    );
  }

  @Test
  public void test_calculateWorkerThreads_shouldUseMinimumWorkerThreadstWithTasks()
  {
    final int taskCount = 7;
    SeekableStreamSupervisorTuningConfig tuningConfig = createSupervisorTuningConfig();
    SeekableStreamSupervisorIOConfig ioConfig = createSupervisorIOConfig(taskCount, null);
    Assert.assertEquals(
        DEFAULT_WORKER_THREADS,
        SeekableStreamSupervisor.calculateWorkerThreads(tuningConfig, ioConfig)
    );
  }

  @Test
  public void test_calculateWorkerThreads_shouldUseFactorOfTaskCount()
  {
    final int taskCount = 18;
    SeekableStreamSupervisorTuningConfig tuningConfig = createSupervisorTuningConfig();
    SeekableStreamSupervisorIOConfig ioConfig = createSupervisorIOConfig(taskCount, null);
    Assert.assertEquals(
        taskCount / DEFAULT_TASKS_PER_WORKER_THREAD,
        SeekableStreamSupervisor.calculateWorkerThreads(tuningConfig, ioConfig)
    );
  }

  @Test
  public void test_calculateWorkerThreads_shouldUseAutoScalerConfig()
  {
    final int taskCountMax = 21;
    final int taskCountMin = 5;
    SeekableStreamSupervisorTuningConfig tuningConfig = createSupervisorTuningConfig();
    AutoScalerConfig autoScalerConfig = new LagBasedAutoScalerConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        taskCountMax,
        null,
        taskCountMin,
        null,
        null,
        true,
        null,
        null
    );
    SeekableStreamSupervisorIOConfig ioConfig = createSupervisorIOConfig(1, autoScalerConfig);
    Assert.assertEquals(
        taskCountMax / DEFAULT_TASKS_PER_WORKER_THREAD,
        SeekableStreamSupervisor.calculateWorkerThreads(tuningConfig, ioConfig)
    );
  }

  private void expectEmitterSupervisor(boolean suspended)
  {
    spec = createMock(SeekableStreamSupervisorSpec.class);
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR_ID).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("PT1S"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        null,
        null
    )
    {
    }).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig() {
      @Override
      public Duration getEmissionDuration()
      {
        return new Period("PT1S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(suspended).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("test").anyTimes();
    EasyMock.expect(spec.getContextValue(DruidMetrics.TAGS)).andReturn(METRIC_TAGS).anyTimes();

    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskQueue.getActiveTasksForDatasource(DATASOURCE)).andReturn(Map.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();
  }

  private static DataSchema getDataSchema()
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return DataSchema.builder()
                     .withDataSource(DATASOURCE)
                     .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                     .withDimensions(dimensions)
                     .withAggregators(new CountAggregatorFactory("rows"))
                     .withGranularity(
                         new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.NONE,
                             ImmutableList.of()
                         )
                     )
                     .build();
  }

  private static SeekableStreamSupervisorIOConfig createSupervisorIOConfig()
  {
    return createSupervisorIOConfig(1, OBJECT_MAPPER.convertValue(getProperties(), AutoScalerConfig.class));
  }

  private static SeekableStreamSupervisorIOConfig createSupervisorIOConfig(
      int taskCount,
      @Nullable AutoScalerConfig autoScalerConfig
  )
  {
    return new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        taskCount,
        new Period("PT1H"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        autoScalerConfig,
        LagAggregator.DEFAULT,
        null,
        null,
        null
    )
    {
    };
  }

  private static Map<String, Object> getProperties()
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 500);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 5000000);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.3);
    autoScalerConfig.put("scaleInThreshold", 1000000);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", 8);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);
    return autoScalerConfig;
  }

  private static SeekableStreamSupervisorTuningConfig getTuningConfig()
  {
    return createSupervisorTuningConfigWithWorkerThreads(1);
  }

  private static SeekableStreamSupervisorTuningConfig createSupervisorTuningConfig()
  {
    return createSupervisorTuningConfigWithWorkerThreads(null);
  }

  private static SeekableStreamSupervisorTuningConfig createSupervisorTuningConfigWithWorkerThreads(@Nullable Integer workerThreads)
  {
    return new SeekableStreamSupervisorTuningConfig()
    {
      @Override
      public Integer getWorkerThreads()
      {
        return workerThreads;
      }

      @Override
      public Long getChatRetries()
      {
        return 1L;
      }

      @Override
      public Duration getHttpTimeout()
      {
        return new Period("PT1M").toStandardDuration();
      }

      @Override
      public Duration getShutdownTimeout()
      {
        return new Period("PT1S").toStandardDuration();
      }

      @Override
      public Duration getRepartitionTransitionDuration()
      {
        return new Period("PT2M").toStandardDuration();
      }

      @Override
      public Duration getOffsetFetchPeriod()
      {
        return new Period("PT5M").toStandardDuration();
      }

      @Override
      public SeekableStreamIndexTaskTuningConfig convertToTaskTuningConfig()
      {
        return new SeekableStreamIndexTaskTuningConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
        {
          @Override
          public SeekableStreamIndexTaskTuningConfig withBasePersistDirectory(File dir)
          {
            return null;
          }

          @Override
          public String toString()
          {
            return null;
          }
        };
      }
    };
  }

  private class TestSeekableStreamIndexTask extends SeekableStreamIndexTask<String, String, ByteEntity>
  {
    private final SeekableStreamIndexTaskRunner<String, String, ByteEntity> streamingTaskRunner;

    public TestSeekableStreamIndexTask(
        String id,
        @Nullable String supervisorId,
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig<String, String> ioConfig,
        @Nullable Map<String, Object> context,
        @Nullable String groupId
    )
    {
      this(
          id,
          supervisorId,
          taskResource,
          dataSchema,
          tuningConfig,
          ioConfig,
          context,
          groupId,
          null
      );
    }

    public TestSeekableStreamIndexTask(
        String id,
        @Nullable String supervisorId,
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig<String, String> ioConfig,
        @Nullable Map<String, Object> context,
        @Nullable String groupId,
        @Nullable SeekableStreamIndexTaskRunner<String, String, ByteEntity> streamingTaskRunner
    )
    {
      super(
          id,
          supervisorId,
          taskResource,
          dataSchema,
          tuningConfig,
          ioConfig,
          context,
          groupId
      );
      this.streamingTaskRunner = streamingTaskRunner;
    }

    @Nullable
    @Override
    protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
    {
      return streamingTaskRunner;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier(final TaskToolbox toolbox)
    {
      return recordSupplier;
    }

    @Override
    public String getType()
    {
      return "test";
    }
  }

  private abstract class BaseTestSeekableStreamSupervisor extends SeekableStreamSupervisor<String, String, ByteEntity>
  {
    private BaseTestSeekableStreamSupervisor()
    {
      super(
          "testSupervisorId",
          taskStorage,
          taskMaster,
          indexerMetadataStorageCoordinator,
          taskClientFactory,
          OBJECT_MAPPER,
          spec,
          rowIngestionMetersFactory,
          false
      );
    }

    @Override
    protected String baseTaskName()
    {
      return "test";
    }

    @Override
    protected void updatePartitionLagFromStream()
    {
      // do nothing
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionRecordLag()
    {
      return null;
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionTimeLag()
    {
      return null;
    }

    @Override
    protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(
        int groupId,
        Map<String, String> startPartitions,
        Map<String, String> endPartitions,
        String baseSequenceName,
        DateTime minimumMessageTime,
        DateTime maximumMessageTime,
        Set<String> exclusiveStartSequenceNumberPartitions,
        SeekableStreamSupervisorIOConfig ioConfig
    )
    {
      return new SeekableStreamIndexTaskIOConfig<>(
          groupId,
          baseSequenceName,
          new SeekableStreamStartSequenceNumbers<>(STREAM, startPartitions, exclusiveStartSequenceNumberPartitions),
          new SeekableStreamEndSequenceNumbers<>(STREAM, endPartitions),
          true,
          minimumMessageTime,
          maximumMessageTime,
          ioConfig.getInputFormat(),
          ioConfig.getTaskDuration().getStandardMinutes()
      )
      {
      };
    }

    @Override
    protected List<SeekableStreamIndexTask<String, String, ByteEntity>> createIndexTasks(
        int replicas,
        String baseSequenceName,
        ObjectMapper sortingMapper,
        TreeMap<Integer, Map<String, String>> sequenceOffsets,
        SeekableStreamIndexTaskIOConfig taskIoConfig,
        SeekableStreamIndexTaskTuningConfig taskTuningConfig,
        RowIngestionMetersFactory rowIngestionMetersFactory
    )
    {
      return ImmutableList.of(new TestSeekableStreamIndexTask(
          "id",
          null,
          null,
          getDataSchema(),
          taskTuningConfig,
          taskIoConfig,
          null,
          null
      ));
    }

    @Override
    protected int getTaskGroupIdForPartition(String partition)
    {
      try {
        return Integer.parseInt(partition) % spec.getIoConfig().getTaskCount();
      }
      catch (NumberFormatException e) {
        return 0;
      }
    }

    @Override
    protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
    {
      return true;
    }

    @Override
    protected boolean doesTaskMatchSupervisor(Task task)
    {
      return true;
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetaDataForReset(
        String stream,
        Map<String, String> map
    )
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber<String> makeSequenceNumber(String seq, boolean isExclusive)
    {
      return new OrderedSequenceNumber<>(seq, isExclusive)
      {
        @Override
        public int compareTo(OrderedSequenceNumber<String> o)
        {
          return new BigInteger(this.get()).compareTo(new BigInteger(o.get()));
        }
      };
    }

    @Override
    protected Map<String, Long> getRecordLagPerPartition(Map<String, String> currentOffsets)
    {
      return null;
    }

    @Override
    protected Map<String, Long> getTimeLagPerPartition(Map<String, String> currentOffsets)
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> setupRecordSupplier()
    {
      return SeekableStreamSupervisorStateTest.this.recordSupplier;
    }

    @Override
    protected SeekableStreamSupervisorReportPayload<String, String> createReportPayload(
        int numPartitions,
        boolean includeOffsets
    )
    {
      return new SeekableStreamSupervisorReportPayload<>(
          SUPERVISOR_ID,
          DATASOURCE,
          STREAM,
          1,
          1,
          1L,
          null,
          null,
          null,
          null,
          null,
          null,
          false,
          true,
          null,
          null,
          null
      )
      {
      };
    }

    @Override
    protected String getNotSetMarker()
    {
      return "NOT_SET";
    }

    @Override
    protected String getEndOfPartitionMarker()
    {
      return "EOF";
    }

    @Override
    protected boolean isEndOfShard(String seqNum)
    {
      return false;
    }

    @Override
    protected boolean isShardExpirationMarker(String seqNum)
    {
      return false;
    }

    @Override
    protected boolean useExclusiveStartSequenceNumberForNonFirstSequence()
    {
      return false;
    }
  }

  private class TestSeekableStreamSupervisor extends BaseTestSeekableStreamSupervisor
  {
    Map<String, String> streamOffsets = new HashMap<>();

    @Override
    protected void scheduleReporting(ScheduledExecutorService reportingExec)
    {
      // do nothing
    }

    @Override
    public LagStats computeLagStats()
    {
      return new LagStats(0, 0, 0);
    }

    @Override
    protected Map<String, String> getLatestSequencesFromStream()
    {
      return streamOffsets;
    }

    public void setStreamOffsets(Map<String, String> streamOffsets)
    {
      this.streamOffsets = streamOffsets;
    }
  }

  private class TestEmittingTestSeekableStreamSupervisor extends BaseTestSeekableStreamSupervisor
  {
    private final CountDownLatch latch;
    private final Map<String, Long> partitionsRecordLag;
    private final Map<String, Long> partitionsTimeLag;
    private final byte metricFlag;

    private static final byte LAG = 0x01;
    private static final byte NOTICE_QUEUE = 0x02;
    private static final byte NOTICE_PROCESS = 0x04;


    TestEmittingTestSeekableStreamSupervisor(
        CountDownLatch latch,
        byte metricFlag,
        Map<String, Long> partitionsRecordLag,
        Map<String, Long> partitionsTimeLag
    )
    {
      this.latch = latch;
      this.metricFlag = metricFlag;
      this.partitionsRecordLag = partitionsRecordLag;
      this.partitionsTimeLag = partitionsTimeLag;
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionRecordLag()
    {
      return partitionsRecordLag;
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionTimeLag()
    {
      return partitionsTimeLag;
    }

    @Override
    protected void emitLag()
    {
      if ((metricFlag & LAG) == 0) {
        return;
      }
      super.emitLag();
      if (stateManager.isSteadyState()) {
        latch.countDown();
      }
    }

    @Override
    protected void emitNoticesQueueSize()
    {
      if ((metricFlag & NOTICE_QUEUE) == 0) {
        return;
      }
      super.emitNoticesQueueSize();
      latch.countDown();
    }

    @Override
    public void emitNoticeProcessTime(String noticeType, long timeInMillis)
    {
      if ((metricFlag & NOTICE_PROCESS) == 0) {
        return;
      }
      super.emitNoticeProcessTime(noticeType, timeInMillis);
      latch.countDown();
    }

    @Override
    public LagStats computeLagStats()
    {
      return null;
    }

    @Override
    protected void scheduleReporting(ScheduledExecutorService reportingExec)
    {
      SeekableStreamSupervisorIOConfig ioConfig = spec.getIoConfig();
      reportingExec.scheduleAtFixedRate(
          this::emitLag,
          ioConfig.getStartDelay().getMillis(),
          spec.getMonitorSchedulerConfig().getEmissionDuration().getMillis(),
          TimeUnit.MILLISECONDS
      );
      reportingExec.scheduleAtFixedRate(
          this::emitNoticesQueueSize,
          ioConfig.getStartDelay().getMillis(),
          spec.getMonitorSchedulerConfig().getEmissionDuration().getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
  }

  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final String taskType;
    private final TaskLocation location;
    private final String dataSource;

    TestTaskRunnerWorkItem(Task task, ListenableFuture<TaskStatus> result, TaskLocation location)
    {
      super(task.getId(), result);
      this.taskType = task.getType();
      this.location = location;
      this.dataSource = task.getDataSource();
    }

    @Override
    public TaskLocation getLocation()
    {
      return location;
    }

    @Override
    public String getTaskType()
    {
      return taskType;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }
  }

  private static class TestSeekableStreamDataSourceMetadata extends SeekableStreamDataSourceMetadata<String, String>
  {

    @JsonCreator
    public TestSeekableStreamDataSourceMetadata(
            @JsonProperty("partitions") SeekableStreamSequenceNumbers<String, String> partitions
    )
    {
      super(partitions);
    }

    @Override
    public DataSourceMetadata asStartMetadata()
    {
      final SeekableStreamSequenceNumbers<String, String> sequenceNumbers = getSeekableStreamSequenceNumbers();
      if (sequenceNumbers instanceof SeekableStreamEndSequenceNumbers) {
        return createConcreteDataSourceMetaData(
                ((SeekableStreamEndSequenceNumbers<String, String>) sequenceNumbers).asStartPartitions(true)
        );
      } else {
        return this;
      }
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createConcreteDataSourceMetaData(
            SeekableStreamSequenceNumbers<String, String> seekableStreamSequenceNumbers
    )
    {
      return new TestSeekableStreamDataSourceMetadata(seekableStreamSequenceNumbers);
    }
  }

  private static SeekableStreamIndexTaskIOConfig createTaskIoConfigExt(
          int groupId,
          Map<String, String> startPartitions,
          Map<String, String> endPartitions,
          String baseSequenceName,
          DateTime minimumMessageTime,
          DateTime maximumMessageTime,
          Set<String> exclusiveStartSequenceNumberPartitions,
          SeekableStreamSupervisorIOConfig ioConfig
  )
  {
    return new SeekableStreamIndexTaskIOConfig<>(
            groupId,
            baseSequenceName,
            new SeekableStreamStartSequenceNumbers<>(STREAM, startPartitions, exclusiveStartSequenceNumberPartitions),
            new SeekableStreamEndSequenceNumbers<>(STREAM, endPartitions),
            true,
            minimumMessageTime,
            maximumMessageTime,
            ioConfig.getInputFormat(),
            ioConfig.getTaskDuration().getStandardMinutes()
    )
    {
    };
  }
}
