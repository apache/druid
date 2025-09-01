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
import org.apache.druid.msq.indexing.report.MSQResultsReport;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiStageQueryTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final MSQExportDirectory exportDirectory = new MSQExportDirectory();

  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addResource(exportDirectory)
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
  public void testMsqIngestionAndQuerying()
  {
    final String sql = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
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

  @Test
  @Disabled("Function LOCALFILES() is currently disabled")
  public void testMsqIngestionAndQueryingWithLocalFn()
  {
    final String sql =
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
            + "  LOCALFILES(\n"
            + "    files => ARRAY['%s'],\n"
            + "    format => 'json'\n"
            + "  ))\n"
            + "  (\"timestamp\" VARCHAR, isRobot VARCHAR, diffUrl VARCHAR, added BIGINT, countryIsoCode VARCHAR, regionName VARCHAR,\n"
            + "   channel VARCHAR, flags VARCHAR, delta BIGINT, isUnpatrolled VARCHAR, isNew VARCHAR, deltaBucket DOUBLE,\n"
            + "   isMinor VARCHAR, isAnonymous VARCHAR, deleted BIGINT, cityName VARCHAR, metroCode BIGINT, namespace VARCHAR,\n"
            + "   comment VARCHAR, page VARCHAR, commentLength BIGINT, countryName VARCHAR, \"user\" VARCHAR, regionIsoCode VARCHAR)\n"
            + "PARTITIONED BY DAY\n",
            dataSource,
            Resources.DataFile.tinyWiki1Json().getAbsolutePath()
        );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, isRobot, added, delta, deleted, namespace FROM %s",
        dataSource,
        ""
    );
  }

  @Test
  public void testExport()
  {
    final String exportSql =
        StringUtils.format(
            "INSERT INTO extern(local(exportPath => '%s'))\n"
            + "AS CSV\n"
            + "SELECT page, added, delta\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n",
            new File(exportDirectory.get(), dataSource).getAbsolutePath(),
            Resources.DataFile.tinyWiki1Json().getAbsolutePath()
        );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(exportSql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());

    final String selectSql = StringUtils.format(
        "SELECT page, delta, added\n"
        + "  FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      '{\"type\":\"local\",\"baseDir\":\"%s\",\"filter\":\"*.csv\"}',\n"
        + "      '{\"type\":\"csv\",\"findColumnsFromHeader\":true}'\n"
        + "    )\n"
        + "  ) EXTEND (\"added\" BIGINT, \"delta\" BIGINT, \"page\" VARCHAR)\n"
        + "   WHERE delta != 0\n"
        + "   ORDER BY page",
        exportDirectory.get()
    );

    final MSQTaskReportPayload statusReport = msqApis.runTaskSqlAndGetReport(selectSql);
    Assertions.assertNotNull(statusReport);
    Assertions.assertNotNull(statusReport.getResults());

    MSQResultsReport resultsReport = statusReport.getResults();

    List<List<Object>> actualResults = new ArrayList<>();
    for (final Object[] row : resultsReport.getResults()) {
      actualResults.add(Arrays.asList(row));
    }

    List<List<Object>> expectedResults = List.of(
        List.of("Cherno Alpha", 111, 123),
        List.of("Gypsy Danger", -143, 57),
        List.of("Striker Eureka", 330, 459)
    );

    Assertions.assertEquals(
        expectedResults,
        actualResults
    );
  }
}
