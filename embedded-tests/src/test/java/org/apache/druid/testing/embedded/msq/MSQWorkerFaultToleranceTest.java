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

import org.apache.druid.guice.ClusterTestingModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Test to verify that cancelled worker tasks are retried when fault tolerance
 * is enabled. This test uses the {@link ClusterTestingModule} to create a
 * faulty Indexer which blocks the completion of the worker task. This allows
 * time to kill off the worker before it can finish, thus triggering a relaunch.
 */
public class MSQWorkerFaultToleranceTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .addProperty("druid.plaintextPort", "7091")
      .addProperty("druid.worker.capacity", "1");

  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addExtension(ClusterTestingModule.class)
        .addResource(new MSQLocalDurableStorage())
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(new EmbeddedBroker())
        .addServer(new EmbeddedHistorical());
  }

  @BeforeAll
  public void initTestClient()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
  }

  @Test
  public void test_cancelledWorker_isRetried_ifFaultToleranceIsEnabled() throws Exception
  {
    String queryLocal =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(\"timestamp\") AS __time,\n"
            + "  isRobot,\n"
            + "  diffUrl,\n"
            + "  added,\n"
            + "  countryIsoCode,\n"
            + "  regionName,\n"
            + "  channel,\n"
            + "  flags,\n"
            + "  delta,\n"
            + "  isUnpatrolled,\n"
            + "  isNew,\n"
            + "  deltaBucket,\n"
            + "  isMinor,\n"
            + "  isAnonymous,\n"
            + "  deleted,\n"
            + "  cityName,\n"
            + "  metroCode,\n"
            + "  namespace,\n"
            + "  comment,\n"
            + "  page,\n"
            + "  commentLength,\n"
            + "  countryName,\n"
            + "  user,\n"
            + "  regionIsoCode\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "PARTITIONED BY DAY\n"
            + "CLUSTERED BY \"__time\"",
            dataSource,
            Resources.DataFile.tinyWiki1Json().getAbsolutePath()
        );

    // Run the MSQ task in fault tolerance mode
    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(
        Map.of("faultTolerance", true),
        queryLocal
    );

    // Add a faulty Indexer to the cluster so that worker is launched but doesn't finish
    final EmbeddedIndexer faultyIndexer = new EmbeddedIndexer()
        .addProperty("druid.unsafe.cluster.testing", "true")
        .addProperty("druid.unsafe.cluster.testing.overlordClient.taskStatusDelay", "PT1H")
        .addProperty("druid.worker.capacity", "1");
    cluster.addServer(faultyIndexer);
    faultyIndexer.start();

    // Let the worker run for a bit so that controller task moves to READING_INPUT phase
    final ServiceMetricEvent matchingEvent = faultyIndexer.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("ingest/count")
    );
    final String workerTaskId = (String) matchingEvent.getUserDims().get(DruidMetrics.TASK_ID);
    Thread.sleep(100);

    // Cancel the worker task and verify that it has failed
    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(workerTaskId));
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasDimension(DruidMetrics.TASK_STATUS, "FAILED")
    );
    faultyIndexer.stop();

    // Add a functional Indexer so that the worker is relaunched successfully
    final EmbeddedIndexer functionalIndexer = new EmbeddedIndexer()
        .addProperty("druid.worker.capacity", "1");
    cluster.addServer(functionalIndexer);
    functionalIndexer.start();

    // Verify that the controller task eventually succeeds
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, isRobot, added, delta, deleted, namespace FROM %s",
        dataSource,
        "2013-08-31T01:02:33.000Z,,57,-143,200,article\n"
        + "2013-08-31T03:32:45.000Z,,459,330,129,wikipedia\n"
        + "2013-08-31T07:11:21.000Z,,123,111,12,article"
    );
  }
}
