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

import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.error.DruidExceptionFault;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class MSQSegmentFetchCapacityTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();

  // A tiny tmpStorageBytesPerTask means the segment-fetch cache in IndexerWorkerContext (sized at 1/3 of this
  // value) cannot possibly hold even the small segment ingested below, so any query reading it back through MSQ
  // must fail.
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2")
      .addProperty("druid.indexer.task.tmpStorageBytesPerTask", "3000");

  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addCommonProperty("druid.emitter.latching.defaultWaitTimeoutMillis", "60000")
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(historical);
  }

  @BeforeAll
  public void initTestClient()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
  }

  @Test
  public void testQueryFailsWhenSegmentFetchCacheIsSmallerThanInputSegment()
  {
    final String insertSql = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    );
    final SqlTaskStatus insertTaskStatus = msqApis.submitTaskSql(insertSql);
    cluster.callApi().waitForTaskToSucceed(insertTaskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    final String selectSql = StringUtils.format(
        "SELECT COUNT(DISTINCT isRobot) FROM %s",
        dataSource
    );
    final SqlTaskStatus selectTaskStatus = msqApis.submitTaskSql(
        Map.of("maxNumTasks", 2, "useApproximateCountDistinct", true),
        selectSql
    );

    final TaskStatus finalStatus =
        cluster.callApi().waitForTaskToFinish(selectTaskStatus.getTaskId(), overlord.latchableEmitter());

    Assertions.assertEquals(TaskState.FAILED, finalStatus.getStatusCode());

    final TaskReport.ReportMap taskReport = cluster.callApi().onLeaderOverlord(
        o -> o.taskReportAsMap(selectTaskStatus.getTaskId())
    );
    final MSQTaskReportPayload payload = taskReport.<MSQTaskReport>findReport(MSQTaskReport.REPORT_KEY)
                                                    .map(MSQTaskReport::getPayload)
                                                    .orElse(null);
    final DruidExceptionFault druidExceptionFault = (DruidExceptionFault) payload.getStatus().getErrorReport().getFault();

    Assertions.assertEquals(DruidException.Category.CAPACITY_EXCEEDED.name(), druidExceptionFault.getCategory());
    Assertions.assertEquals(DruidException.Persona.OPERATOR.name(), druidExceptionFault.getPersona());
    Assertions.assertTrue(
        finalStatus.getErrorMsg() != null && finalStatus.getErrorMsg().contains("Unable to load segment"),
        StringUtils.format("Unexpected error message: %s", finalStatus.getErrorMsg())
    );
  }
}
