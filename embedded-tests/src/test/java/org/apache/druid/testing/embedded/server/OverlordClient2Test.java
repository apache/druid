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

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Test;

/**
 * Tests all the REST APIs exposed by the Overlord using the
 * {@link OverlordClient}.
 */
public class OverlordClient2Test extends EmbeddedClusterTestBase
{
  private static final String UNKNOWN_TASK_ID
      = IdUtils.newTaskId("sim_test_noop", "dummy", null);
  private static final String UNKNOWN_TASK_ERROR
      = StringUtils.format("Cannot find any task with id: [%s]", UNKNOWN_TASK_ID);

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer1 = new EmbeddedIndexer().addProperty("druid.worker.capacity", "3");
  private final EmbeddedIndexer indexer2 = new EmbeddedIndexer().addProperty("druid.worker.capacity", "3");
//  private final EmbeddedIndexer indexer1 = new EmbeddedIndexer().addProperty("druid.worker.capacity", "3");
//      .addProperty("druid.plaintextPort", "17083");

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(new EmbeddedCoordinator())
                               .addServer(new EmbeddedBroker())
                               .addServer(overlord)
                               .addServer(indexer1)
                               ;
  }

  @Test
  public void test_runTask_ofTypeNoop() throws Exception
  {
    final String taskId = IdUtils.newTaskId("sim_test_noop", TestDataSource.WIKI, null);
    Thread.sleep(500);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(taskId, new NoopTask(taskId, null, null, 4000L, 0L, null))
    );
    Thread.sleep(500);

    System.out.println("----stop:overlord---");
    overlord.stop();
    Thread.sleep(400);
    overlord.start();
    Thread.sleep(400);
    System.out.println("----stop2---");
    indexer1.stop();
    Thread.sleep(400);
    indexer1.start();
//    EmbeddedIndexer indexer2 = new EmbeddedIndexer().addProperty("druid.worker.capacity", "3");
    // cluster.revoidServer(indexer1);
//    cluster.addServer(indexer2);
//    indexer2.start();
//    System.out.println("----stop3---");
//    Thread.sleep(500);
////    overlord.stop();
//    System.out.println("----stop4---");
//    Thread.sleep(500);

  int cnt = 0;
  while (cnt < 100) {
    cnt++;
    Thread.sleep(1000);
    if (cnt == 4) {
      System.out.println("@@@cancel task");
      cluster.callApi().onLeaderOverlord(oc -> oc.cancelTask(taskId));
    }
    Object r = cluster.callApi().onLeaderOverlord(oc -> oc.taskStatus(taskId));
    System.out.println(r);
  }

//    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
  }
//
//  @Test
//  public void test_runKillTask() throws Exception
//  {
//    final String taskId = cluster.callApi().onLeaderOverlord(
//        o -> o.runKillTask("sim_test", TestDataSource.WIKI, Intervals.ETERNITY, null, null, null)
//    );
//
////    indexer1.stop();
////    indexer1.start();
////    cluster.addServer(indexer2);
////    indexer2.start();
//
//    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
//  }

}
