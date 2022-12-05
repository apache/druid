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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
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
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager.SeekableStreamExceptionEvent;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager.SeekableStreamState;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SeekableStreamSupervisorStateTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String DATASOURCE = "testDS";
  private static final String STREAM = "stream";
  private static final String SHARD_ID = "0";
  private static final StreamPartition<String> SHARD0_PARTITION = StreamPartition.of(STREAM, SHARD_ID);
  private static final String EXCEPTION_MSG = "I had an exception";

  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private TaskQueue taskQueue;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private SeekableStreamIndexTaskClientFactory taskClientFactory;
  private SeekableStreamSupervisorSpec spec;
  private SeekableStreamIndexTaskClient indexTaskClient;
  private RecordSupplier<String, String, ByteEntity> recordSupplier;

  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private SupervisorStateManagerConfig supervisorConfig;

  private TestEmitter emitter;

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
    recordSupplier = (RecordSupplier<String, String, ByteEntity>) createMock(RecordSupplier.class);

    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();

    supervisorConfig = new SupervisorStateManagerConfig();

    emitter = new TestEmitter();

    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig()).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();

    EasyMock.expect(taskClientFactory.build(
        EasyMock.anyString(),
        EasyMock.anyObject(),
        EasyMock.anyInt(),
        EasyMock.anyObject()
    )).andReturn(
        indexTaskClient).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    EasyMock.expectLastCall().times(0, 1);

    EasyMock
        .expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(null).anyTimes();
    EasyMock.expect(recordSupplier.getAssignment()).andReturn(ImmutableSet.of(SHARD0_PARTITION)).anyTimes();
    EasyMock.expect(recordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("10").anyTimes();
  }

  @Test
  public void testRunning() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testRunningStreamGetSequenceNumberReturnsNull() throws Exception
  {
    EasyMock.reset(recordSupplier);
    EasyMock.expect(recordSupplier.getAssignment()).andReturn(ImmutableSet.of(SHARD0_PARTITION)).anyTimes();
    EasyMock.expect(recordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testConnectingToStreamFail() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM))
            .andThrow(new StreamException(new IllegalStateException(EXCEPTION_MSG)))
            .anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testConnectingToStreamFailRecoveryFailRecovery() throws Exception
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
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testDiscoveringInitialTasksFailRecoveryFail() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andThrow(new IllegalStateException(EXCEPTION_MSG)).times(3);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).times(6);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andThrow(new IllegalStateException(EXCEPTION_MSG)).times(3);
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
        null,
        new IdleConfig(true, 200L)
    )
    {
    }).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig() {
      @Override
      public Duration getEmitterPeriod()
      {
        return new Period("PT1S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("test").anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testCreatingTasksFailRecoveryFail() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testSuspended() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(true).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testStopping() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
  public void testStoppingGracefully() throws Exception
  {
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
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
    List<Event> events = emitter.getEvents();
    List<String> whitelist = Arrays.asList("ingest/test/lag", "ingest/test/maxLag",
        "ingest/test/avgLag", "ingest/test/lag/time", "ingest/test/maxLag/time", "ingest/test/avgLag/time");
    events = filterMetrics(events, whitelist);
    Assert.assertEquals(6, events.size());
    Assert.assertEquals("ingest/test/lag", events.get(0).toMap().get("metric"));
    Assert.assertEquals(850L, events.get(0).toMap().get("value"));
    Assert.assertEquals("ingest/test/maxLag", events.get(1).toMap().get("metric"));
    Assert.assertEquals(500L, events.get(1).toMap().get("value"));
    Assert.assertEquals("ingest/test/avgLag", events.get(2).toMap().get("metric"));
    Assert.assertEquals(283L, events.get(2).toMap().get("value"));
    Assert.assertEquals("ingest/test/lag/time", events.get(3).toMap().get("metric"));
    Assert.assertEquals(45000L, events.get(3).toMap().get("value"));
    Assert.assertEquals("ingest/test/maxLag/time", events.get(4).toMap().get("metric"));
    Assert.assertEquals(20000L, events.get(4).toMap().get("value"));
    Assert.assertEquals("ingest/test/avgLag/time", events.get(5).toMap().get("metric"));
    Assert.assertEquals(15000L, events.get(5).toMap().get("value"));
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
    List<Event> events = emitter.getEvents();
    List<String> whitelist = Arrays.asList("ingest/test/lag", "ingest/test/maxLag", "ingest/test/avgLag");
    events = filterMetrics(events, whitelist);
    Assert.assertEquals(3, events.size());
    Assert.assertEquals("ingest/test/lag", events.get(0).toMap().get("metric"));
    Assert.assertEquals(850L, events.get(0).toMap().get("value"));
    Assert.assertEquals("ingest/test/maxLag", events.get(1).toMap().get("metric"));
    Assert.assertEquals(500L, events.get(1).toMap().get("value"));
    Assert.assertEquals("ingest/test/avgLag", events.get(2).toMap().get("metric"));
    Assert.assertEquals(283L, events.get(2).toMap().get("value"));
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
    List<Event> events = emitter.getEvents();
    List<String> whitelist = Arrays.asList("ingest/test/lag/time", "ingest/test/maxLag/time", "ingest/test/avgLag/time");
    events = filterMetrics(events, whitelist);
    Assert.assertEquals(3, events.size());
    Assert.assertEquals("ingest/test/lag/time", events.get(0).toMap().get("metric"));
    Assert.assertEquals(45000L, events.get(0).toMap().get("value"));
    Assert.assertEquals("ingest/test/maxLag/time", events.get(1).toMap().get("metric"));
    Assert.assertEquals(20000L, events.get(1).toMap().get("value"));
    Assert.assertEquals("ingest/test/avgLag/time", events.get(2).toMap().get("metric"));
    Assert.assertEquals(15000L, events.get(2).toMap().get("value"));
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
    List<Event> events = emitter.getEvents();
    List<String> whitelist = Collections.singletonList("ingest/notices/queueSize");
    events = filterMetrics(events, whitelist);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals("ingest/notices/queueSize", events.get(0).toMap().get("metric"));
    Assert.assertEquals(0, events.get(0).toMap().get("value"));
    Assert.assertEquals("testDS", events.get(0).toMap().get("dataSource"));
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
    List<Event> events = emitter.getEvents();
    List<String> whitelist = Collections.singletonList("ingest/notices/time");
    events = filterMetrics(events, whitelist);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals("ingest/notices/time", events.get(0).toMap().get("metric"));
    Assert.assertTrue(String.valueOf(events.get(0).toMap().get("value")), (long) events.get(0).toMap().get("value") > 0);
    Assert.assertEquals("testDS", events.get(0).toMap().get("dataSource"));
    Assert.assertEquals("run_notice", events.get(0).toMap().get("noticeType"));
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
    List<Event> events = emitter.getEvents();
    List<String> whitelist = Arrays.asList("ingest/test/lag", "ingest/test/maxLag",
        "ingest/test/avgLag", "ingest/test/lag/time", "ingest/test/maxLag/time", "ingest/test/avgLag/time");
    events = filterMetrics(events, whitelist);
    Assert.assertEquals(0, events.size());
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
        Optional.absent(),
        Optional.absent(),
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("1", "0"),
        Optional.absent(),
        Optional.absent(),
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

  private List<Event> filterMetrics(List<Event> events, List<String> whitelist)
  {
    List<Event> result = events.stream()
        .filter(e -> whitelist.contains(e.toMap().get("metric").toString()))
        .collect(Collectors.toList());
    return result;
  }

  private void expectEmitterSupervisor(boolean suspended) throws EntryExistsException
  {
    spec = createMock(SeekableStreamSupervisorSpec.class);
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
        null,
        null
    )
    {
    }).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.getMonitorSchedulerConfig()).andReturn(new DruidMonitorSchedulerConfig() {
      @Override
      public Duration getEmitterPeriod()
      {
        return new Period("PT1S").toStandardDuration();
      }
    }).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(suspended).anyTimes();
    EasyMock.expect(spec.getType()).andReturn("test").anyTimes();

    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).anyTimes();

    replayAll();
  }

  private static DataSchema getDataSchema()
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return new DataSchema(
        DATASOURCE,
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(dimensions),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.of()
        ),
        null
    );
  }

  private static SeekableStreamSupervisorIOConfig getIOConfig()
  {
    return new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        OBJECT_MAPPER.convertValue(getProperties(), AutoScalerConfig.class),
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
    return new SeekableStreamSupervisorTuningConfig()
    {
      @Override
      public Integer getWorkerThreads()
      {
        return 1;
      }

      @Override
      public boolean getChatAsync()
      {
        return false;
      }

      @Override
      public Integer getChatThreads()
      {
        return 1;
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
    public TestSeekableStreamIndexTask(
        String id,
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig<String, String> ioConfig,
        @Nullable Map<String, Object> context,
        @Nullable String groupId
    )
    {
      super(
          id,
          taskResource,
          dataSchema,
          tuningConfig,
          ioConfig,
          context,
          groupId
      );
    }

    @Override
    protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier()
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
      return new SeekableStreamIndexTaskIOConfig<String, String>(
          groupId,
          baseSequenceName,
          new SeekableStreamStartSequenceNumbers<>(STREAM, startPartitions, exclusiveStartSequenceNumberPartitions),
          new SeekableStreamEndSequenceNumbers<>(STREAM, endPartitions),
          true,
          minimumMessageTime,
          maximumMessageTime,
          ioConfig.getInputFormat()
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
      return 0;
    }

    @Override
    protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
    {
      return true;
    }

    @Override
    protected boolean doesTaskTypeMatchSupervisor(Task task)
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
      return new OrderedSequenceNumber<String>(seq, isExclusive)
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
      return new SeekableStreamSupervisorReportPayload<String, String>(
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
          spec.getMonitorSchedulerConfig().getEmitterPeriod().getMillis(),
          TimeUnit.MILLISECONDS
      );
      reportingExec.scheduleAtFixedRate(
          this::emitNoticesQueueSize,
          ioConfig.getStartDelay().getMillis(),
          spec.getMonitorSchedulerConfig().getEmitterPeriod().getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
  }

  private static class TestEmitter extends NoopServiceEmitter
  {
    @GuardedBy("events")
    private final List<Event> events = new ArrayList<>();

    @Override
    public void emit(Event event)
    {
      synchronized (events) {
        events.add(event);
      }
    }

    public List<Event> getEvents()
    {
      synchronized (events) {
        return ImmutableList.copyOf(events);
      }
    }
  }
}
