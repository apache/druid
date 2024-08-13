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
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class MSQControllerTaskLimitTest
{
  private static final String DATASOURCE = "ds";


  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TaskQueue taskQueue;
  private TaskStorage taskStorage;
  private SimpleTaskRunner taskRunner;
  private int numTasks = 3;
  private Closer closer;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    closer = Closer.create();

    // Be as realistic as possible; use actual classes for storage rather than mocks.
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(Period.hours(1)));
    taskRunner = new SimpleTaskRunner(3);
    closer.register(taskRunner::stop);
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Test
  public void testMaxControllerTasksLimit() throws InterruptedException
  {

    startTaskQueue(new TaskQueueConfig(null, Period.millis(1), null, null, null, null, 2));

    Assert.assertEquals("no tasks should be running", 0, taskRunner.getKnownTasks().size());
    Assert.assertEquals("no tasks should be known", 0, taskQueue.getTasks().size());
    Assert.assertEquals("no tasks should be running", 0, taskQueue.getRunningTaskCount().size());

    // Add all tasks.
    for (int i = 0; i < numTasks; i++) {
      final NoopTask testTask = createMsqTestTask(1000L);
      taskQueue.add(testTask);
    }

    Thread.sleep(3000);

    Assert.assertEquals("all tasks should be known", numTasks, taskQueue.getTasks().size());
    Assert.assertEquals(2, taskQueue.getRunningControllerTaskCount().longValue());

    Thread.sleep(3000);
    Assert.assertEquals(2, taskQueue.getSuccessfulTaskCount().get(DATASOURCE).longValue());
    Assert.assertEquals(1, taskQueue.getRunningControllerTaskCount().longValue());
  }

  @Test
  public void testRatioControllerTasksLimit() throws InterruptedException
  {

    startTaskQueue(new TaskQueueConfig(null, Period.millis(1), null, null, null, 0.8f, null));

    Assert.assertEquals("no tasks should be running", 0, taskRunner.getKnownTasks().size());
    Assert.assertEquals("no tasks should be known", 0, taskQueue.getTasks().size());
    Assert.assertEquals("no tasks should be running", 0, taskQueue.getRunningTaskCount().size());

    // Add all tasks.
    for (int i = 0; i < numTasks; i++) {
      final NoopTask testTask = createMsqTestTask(1000L);
      taskQueue.add(testTask);
    }

    Thread.sleep(3000);

    Assert.assertEquals("all tasks should be known", numTasks, taskQueue.getTasks().size());
    Assert.assertEquals(2, taskQueue.getRunningControllerTaskCount().longValue());

    Thread.sleep(3000);
    Assert.assertEquals(2, taskQueue.getSuccessfulTaskCount().get(DATASOURCE).longValue());
    Assert.assertEquals(1, taskQueue.getRunningControllerTaskCount().longValue());
  }


  private NoopMSQTask createMsqTestTask(long runtimeMillis)
  {
    return new NoopMSQTask(null, null, DATASOURCE, runtimeMillis, 0, Collections.emptyMap());
  }

  private void startTaskQueue(final TaskQueueConfig config)
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    final IndexerSQLMetadataStorageCoordinator storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector()
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
        config,
        new DefaultTaskConfig(),
        taskStorage,
        taskRunner,
        unsupportedTaskActionFactory, // Not used for anything serious
        new TaskLockbox(taskStorage, storageCoordinator),
        new NoopServiceEmitter()
    );

    taskQueue.start();
    closer.register(taskQueue::stop);
  }

  private static class NoopMSQTask extends NoopTask
  {
    public NoopMSQTask(
        String id,
        String groupId,
        String dataSource,
        long runTimeMillis,
        long isReadyTime,
        Map<String, Object> context
    )
    {
      super(id, groupId, dataSource, runTimeMillis, isReadyTime, context);
    }

    @Override
    public String getType()
    {
      return "query_controller";
    }
  }
}
