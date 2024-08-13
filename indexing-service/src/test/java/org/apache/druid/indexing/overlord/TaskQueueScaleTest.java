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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.NoopTaskContextEnricher;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests that {@link TaskQueue} is able to handle large numbers of concurrently-running tasks.
 */
public class TaskQueueScaleTest
{
  private static final String DATASOURCE = "ds";

  private final int numTasks = 1000;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TaskQueue taskQueue;
  private TaskStorage taskStorage;
  private SimpleTaskRunner taskRunner;
  private Closer closer;
  private SegmentSchemaManager segmentSchemaManager;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    closer = Closer.create();

    // Be as realistic as possible; use actual classes for storage rather than mocks.
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(Period.hours(1)));
    taskRunner = new SimpleTaskRunner();
    closer.register(taskRunner::stop);
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    segmentSchemaManager = new SegmentSchemaManager(derbyConnectorRule.metadataTablesConfigSupplier().get(), jsonMapper, derbyConnectorRule.getConnector());
    final IndexerSQLMetadataStorageCoordinator storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector(),
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create()
    );

    final TaskActionClientFactory unsupportedTaskActionFactory =
        task -> new TaskActionClient()
        {
          @Override
          public <RetType> RetType submit(TaskAction<RetType> taskAction)
          {
            throw new UnsupportedOperationException();
          }
        };

    taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, Period.millis(1), null, null, null, null, null, null),
        new DefaultTaskConfig(),
        taskStorage,
        taskRunner,
        unsupportedTaskActionFactory, // Not used for anything serious
        new TaskLockbox(taskStorage, storageCoordinator),
        new NoopServiceEmitter(),
        jsonMapper,
        new NoopTaskContextEnricher()
    );

    taskQueue.start();
    closer.register(taskQueue::stop);
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Test(timeout = 60_000L) // more than enough time if the task queue is efficient
  public void doMassLaunchAndExit() throws Exception
  {
    Assert.assertEquals("no tasks should be running", 0, taskRunner.getKnownTasks().size());
    Assert.assertEquals("no tasks should be known", 0, taskQueue.getTasks().size());
    Assert.assertEquals("no tasks should be running", 0, taskQueue.getRunningTaskCount().size());

    // Add all tasks.
    for (int i = 0; i < numTasks; i++) {
      final NoopTask testTask = createTestTask(2000L);
      taskQueue.add(testTask);
    }

    // in theory we can get a race here, since we fetch the counts at separate times
    Assert.assertEquals("all tasks should be known", numTasks, taskQueue.getTasks().size());
    long runningTasks = taskQueue.getRunningTaskCount().values().stream().mapToLong(Long::longValue).sum();
    long pendingTasks = taskQueue.getPendingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    long waitingTasks = taskQueue.getWaitingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    Assert.assertEquals("all tasks should be known", numTasks, (runningTasks + pendingTasks + waitingTasks));

    // Wait for all tasks to finish.
    final TaskLookup.CompleteTaskLookup completeTaskLookup =
        TaskLookup.CompleteTaskLookup.of(numTasks, Duration.standardHours(1));

    while (taskStorage.getTaskInfos(completeTaskLookup, DATASOURCE).size() < numTasks) {
      Thread.sleep(100);
    }

    Thread.sleep(100);

    Assert.assertEquals("no tasks should be active", 0, taskStorage.getActiveTasks().size());
    runningTasks = taskQueue.getRunningTaskCount().values().stream().mapToLong(Long::longValue).sum();
    pendingTasks = taskQueue.getPendingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    waitingTasks = taskQueue.getWaitingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    Assert.assertEquals("no tasks should be running", 0, runningTasks);
    Assert.assertEquals("no tasks should be pending", 0, pendingTasks);
    Assert.assertEquals("no tasks should be waiting", 0, waitingTasks);
  }

  @Test(timeout = 60_000L) // more than enough time if the task queue is efficient
  public void doMassLaunchAndShutdown() throws Exception
  {
    Assert.assertEquals("no tasks should be running", 0, taskRunner.getKnownTasks().size());

    // Add all tasks.
    final List<String> taskIds = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      final NoopTask testTask = createTestTask(
          Duration.standardHours(1).getMillis() /* very long runtime millis, so we can do a shutdown */
      );
      taskQueue.add(testTask);
      taskIds.add(testTask.getId());
    }

    // wait for all tasks to progress to running state
    while (taskStorage.getActiveTasks().size() < numTasks) {
      Thread.sleep(100);
    }
    Assert.assertEquals("all tasks should be running", numTasks, taskStorage.getActiveTasks().size());

    // Shut down all tasks.
    for (final String taskId : taskIds) {
      taskQueue.shutdown(taskId, "test shutdown");
    }

    // Wait for all tasks to finish.
    while (!taskStorage.getActiveTasks().isEmpty()) {
      Thread.sleep(100);
    }

    Assert.assertEquals("no tasks should be running", 0, taskStorage.getActiveTasks().size());

    int completed = taskStorage.getTaskInfos(
        TaskLookup.CompleteTaskLookup.of(numTasks, Duration.standardHours(1)),
        DATASOURCE
    ).size();
    Assert.assertEquals("all tasks should have completed", numTasks, completed);
  }

  private NoopTask createTestTask(long runtimeMillis)
  {
    return new NoopTask(null, null, DATASOURCE, runtimeMillis, 0, Collections.emptyMap());
  }
}

