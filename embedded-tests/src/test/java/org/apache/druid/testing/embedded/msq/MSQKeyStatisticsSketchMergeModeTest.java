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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class MSQKeyStatisticsSketchMergeModeTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "3");
  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(new EmbeddedBroker())
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedRouter());
  }

  @BeforeAll
  public void initTestClient()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
  }

  @Test
  public void testMsqIngestionParallelMerging()
  {
    String queryLocal = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json()
    );

    Map<String, Object> context = Map.of(
        MultiStageQueryContext.CTX_CLUSTER_STATISTICS_MERGE_MODE,
        ClusterStatisticsMergeMode.PARALLEL
    );

    final SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(context, queryLocal);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, isRobot, added, delta, deleted, namespace FROM %s",
        dataSource,
        "2013-08-31T01:02:33.000Z,,57,-143,200,article\n"
        + "2013-08-31T03:32:45.000Z,,459,330,129,wikipedia\n"
        + "2013-08-31T07:11:21.000Z,,123,111,12,article"
    );
  }

  @Test
  public void testMsqIngestionSequentialMerging()
  {
    String queryLocal = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json()
    );

    Map<String, Object> context = Map.of(
        MultiStageQueryContext.CTX_CLUSTER_STATISTICS_MERGE_MODE,
        ClusterStatisticsMergeMode.SEQUENTIAL
    );

    SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(context, queryLocal);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, isRobot, added, delta, deleted, namespace FROM %s",
        dataSource,
        "2013-08-31T01:02:33.000Z,,57,-143,200,article\n"
        + "2013-08-31T03:32:45.000Z,,459,330,129,wikipedia\n"
        + "2013-08-31T07:11:21.000Z,,123,111,12,article"
    );
  }


  @Test
  public void testMsqIngestionSequentialMergingWithEmptyStatistics()
  {
    String queryLocal =
        StringUtils.format(
            "Replace INTO %s overwrite ALL \n"
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
            + "    '{\"type\":\"local\",\"files\":[\"%s\",\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "where delta=111 "
            // we add this filter since delta=111 is only present in wikipedia_index_data1.json. This means partitions from worker 2 will be empty.
            + "PARTITIONED BY DAY\n"
            + "CLUSTERED BY \"__time\"",
            dataSource,
            Resources.DataFile.tinyWiki1Json(),
            Resources.DataFile.tinyWiki2Json()
        );

    Map<String, Object> context = Map.of(
        MultiStageQueryContext.CTX_CLUSTER_STATISTICS_MERGE_MODE,
        ClusterStatisticsMergeMode.SEQUENTIAL,
        MultiStageQueryContext.CTX_MAX_NUM_TASKS,
        3
    );

    SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(context, queryLocal);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, isRobot, added, delta, deleted, namespace FROM %s",
        dataSource,
        "2013-08-31T07:11:21.000Z,,123,111,12,article"
    );
  }
}
