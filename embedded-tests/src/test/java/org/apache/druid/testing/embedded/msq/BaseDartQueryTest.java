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

package org.apache.druid.testing.embedded.msq;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.guice.SleepModule;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.sql.http.GetQueryReportResponse;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.auth.EmbeddedBasicAuthResource;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.BeforeAll;

/**
 * Shared embedded cluster setup for Dart query tests.
 */
public abstract class BaseDartQueryTest extends EmbeddedClusterTestBase
{
  private static final int MAX_RETAINED_REPORT_COUNT = 10;

  protected final EmbeddedBroker broker1 = new EmbeddedBroker();
  protected final EmbeddedBroker broker2 = new EmbeddedBroker();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer();
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  protected EmbeddedMSQApis msqApis;
  protected String ingestedDataSource;

  private void configureBroker(final EmbeddedBroker broker, final int port)
  {
    broker.addProperty("druid.msq.dart.controller.heapFraction", "0.5")
          .addProperty("druid.msq.dart.controller.maxRetainedReportCount", String.valueOf(MAX_RETAINED_REPORT_COUNT))
          .addProperty("druid.query.default.context.maxConcurrentStages", "1")
          .addProperty("druid.sql.planner.enableSysQueriesTable", "true")
          .addProperty("druid.plaintextPort", String.valueOf(port));
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");
    overlord.addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    configureBroker(broker1, 7082);
    configureBroker(broker2, 7083);

    historical.addProperty("druid.msq.dart.worker.heapFraction", "0.5")
              .addProperty("druid.msq.dart.worker.concurrentQueries", "1");

    indexer.setServerMemory(400_000_000)
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.worker.capacity", "4");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.msq.dart.enabled", "true")
                               .addResource(new EmbeddedBasicAuthResource())
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker1)
                               .addServer(broker2)
                               .addServer(indexer)
                               .addServer(historical)
                               .addExtension(SleepModule.class);
  }

  @BeforeAll
  protected void setupData()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);

    ingestedDataSource = EmbeddedClusterApis.createTestDatasourceName();
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = MoreResources.Task.BASIC_INDEX.get().dataSource(ingestedDataSource).withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);

    cluster.callApi().waitForAllSegmentsToBeAvailable(ingestedDataSource, coordinator, broker1);
    cluster.callApi().waitForAllSegmentsToBeAvailable(ingestedDataSource, coordinator, broker2);
  }

  /**
   * Polls the report API on {@link #broker1} until a report is available.
   */
  protected GetQueryReportResponse waitForReport(final String sqlQueryId)
  {
    final long timeout = 30_000;
    final long deadline = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < deadline) {
      final GetQueryReportResponse report = msqApis.getDartQueryReport(sqlQueryId, broker1);
      if (report != null) {
        return report;
      }
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new ISE("Timed out after[%,d] ms waiting for query to be in RUNNING state", timeout);
  }
}
