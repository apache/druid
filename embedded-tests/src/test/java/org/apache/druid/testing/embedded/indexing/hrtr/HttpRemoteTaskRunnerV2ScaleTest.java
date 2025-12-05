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

package org.apache.druid.testing.embedded.indexing.hrtr;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Scale test for HttpRemoteTaskRunnerV2 with large number of tasks with varying priorities and runtime durations.
 */
public class HttpRemoteTaskRunnerV2ScaleTest extends EmbeddedClusterTestBase
{
  private static final Logger log = new Logger(HttpRemoteTaskRunnerV2ScaleTest.class);
  private static final int NUM_TASKS = 5_000;
  private static final int NUM_WORKERS = 10;
  private static final int WORKER_CAPACITY = 50;
  private static final int TASK_RUN_TIME_MS = 100;

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.indexer.runner.type", "httpRemoteV2")
      .addProperty("druid.indexer.runner.pendingTasksRunnerNumThreads", String.valueOf(Runtime.getRuntime().availableProcessors()));

  private final EmbeddedIndexer[] indexers = new EmbeddedIndexer[NUM_WORKERS];

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    for (int i = 0; i < NUM_WORKERS; i++) {
      indexers[i] = new EmbeddedIndexer()
          .addProperty("druid.worker.capacity", String.valueOf(WORKER_CAPACITY))
          .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
          .addProperty("druid.plaintextPort", String.valueOf(8100 + i));
    }

    EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                                                       .useLatchableEmitter()
                                                       .useDefaultTimeoutForLatchableEmitter(240)
                                                       .addServer(new EmbeddedCoordinator())
                                                       .addServer(overlord);

    for (EmbeddedIndexer indexer : indexers) {
      cluster.addServer(indexer);
    }

    return cluster
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedBroker())
        .addServer(new EmbeddedRouter());
  }

  @Test
  public void test_scaleWithManyTasks()
  {
    log.info("Creating [%d] tasks...", NUM_TASKS);
    List<NoopTask> tasks = createTasks(NUM_TASKS);

    long startTime = System.currentTimeMillis();
    List<String> taskIds = new ArrayList<>();
    double totalTime = 0;
    for (NoopTask task : tasks) {
      String taskId = task.getId();
      taskIds.add(taskId);
      long t0 = System.nanoTime();
      cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
      long t1 = System.nanoTime();
      totalTime += t1 - t0;
    }

    int successCount = 0;
    int failureCount = 0;
    int reportInterval = NUM_TASKS / 10; // Report every 10%

    log.info("Waiting for tasks to complete...");
    for (int i = 0; i < taskIds.size(); i++) {
      String taskId = taskIds.get(i);
      try {
        cluster.callApi().waitForTaskToSucceed(taskId, overlord);
        ++successCount;
        if ((i + 1) % reportInterval == 0) {
          log.info("Progress: %d/%d tasks completed (%.1f%%)", i + 1, NUM_TASKS, ((i + 1) * 100.0) / NUM_TASKS);
        }
      }
      catch (AssertionError e) {
        ++failureCount;
        log.error("Task %s failed: %s", taskId, e.getMessage());
      }
    }

    long endTime = System.currentTimeMillis();
    long durationMs = endTime - startTime;

    double tasksPerSecond = (NUM_TASKS * 1000.0) / durationMs;
    log.info("All tasks submitted in [%d] ms", System.currentTimeMillis() - startTime);
    log.info("Avg task submission time: [%f] ns", totalTime / NUM_TASKS);
    log.info(
        "Task runner completed %d/%d tasks in %dms (%.2f tasks/sec)%n",
        successCount,
        NUM_TASKS,
        durationMs,
        tasksPerSecond
    );

    Assertions.assertEquals(NUM_TASKS, successCount, "All tasks should succeed");
    Assertions.assertEquals(0, failureCount, "No tasks should fail");
  }

  private List<NoopTask> createTasks(int numTasks)
  {
    List<NoopTask> tasks = new ArrayList<>();
    Random random = new Random(12345);

    for (int i = 0; i < numTasks; i++) {
      int priority = random.nextInt(101);
      String taskId = IdUtils.getRandomIdWithPrefix(dataSource);

      Map<String, Object> context = new HashMap<>();
      context.put("priority", priority);
      tasks.add(new NoopTask(taskId, null, dataSource, TASK_RUN_TIME_MS, 0, context));
    }

    return tasks;
  }
}
