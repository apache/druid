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

package org.apache.druid.testing.embedded.server;

import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OverlordIntegrationTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer().addProperty("druid.worker.capacity", "3");

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addServer(new EmbeddedCoordinator())
        .addServer(new EmbeddedBroker())
        .addServer(overlord)
        .addServer(indexer);
  }

  @Test
  public void test_overlord_marksTaskAsFailed_ifIndexerCrashes() throws Exception
  {
    final String taskId = IdUtils.newTaskId("sim_test_noop", TestDataSource.WIKI, null);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(taskId, new NoopTask(taskId, null, null, 8000L, 0L, null))
    );
    // give some time for the overlord to dispatch the task to the worker
    Thread.sleep(100);
    overlord.stop();
    overlord.start();
    // give some time for the overlord to load the task from the worker
    Thread.sleep(100);
    indexer.stop();
    indexer.start();
    // Wait for the Overlord to mark the task as FAILED
    overlord.latchableEmitter().waitForEvent(
       event -> event.hasMetricName("task/run/time")
                               .hasDimension(DruidMetrics.TASK_ID, taskId)
                               .hasDimension(DruidMetrics.TASK_STATUS, "FAILED")
    );

    TaskStatusResponse jobStatus = cluster.callApi().onLeaderOverlord(oc -> oc.taskStatus(taskId));
    // the task should have failed
    assertEquals(TaskState.FAILED, jobStatus.getStatus().getStatusCode());
    assertEquals(
        "This task disappeared on the worker where it was assigned. See overlord logs for more details.",
        jobStatus.getStatus().getErrorMsg()
    );
  }
}
