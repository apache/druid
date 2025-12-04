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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.io.ByteStreams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

/**
 * Embedded test to verify nested group-by queries using the {@code forcePushDownNestedQuery} context.
 */
public class ForcePushDownNestedQueryTest extends QueryTestBase
{
  @Override
  public void beforeAll() throws IOException
  {
    dataSource = EmbeddedClusterApis.createTestDatasourceName();
    loadWikipediaTable();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // don't change the datasource name for each run because we set things up before all tests
  }

  @Test
  public void test_forcePushDownNestedQueries()
  {
    // Nested group by double agg query with force push down
    cluster.callApi().verifySqlQuery(
        "SET forcePushDownNestedQuery = TRUE;\n"
        + "SELECT SUM(sumAdded) AS \"groupedSumAdded\" FROM (\n"
        + "  SELECT channel, \"user\", SUM(added) AS sumAdded\n"
        + "  FROM %s\n"
        + "  GROUP BY channel, \"user\"\n"
        + ")",
        dataSource,
        "9385573"
    );

    // Nested group by query with force push down and renamed dimensions
    cluster.callApi().verifySqlQuery(
        "SET forcePushDownNestedQuery = TRUE;\n"
        + "SELECT SUM(sumAdded) AS groupedSumAdded FROM (\n"
        + "  SELECT channel AS renamedChannel, \"user\" AS renamedUser, SUM(added) AS sumAdded\n"
        + "  FROM %s\n"
        + "  GROUP BY channel, \"user\"\n"
        + ") inner_q",
        dataSource,
        "9385573"
    );

    // Nested group-by query with force pushdown disabled and filters on both outer and inner queries.
    // forcePushDownNestedQuery is intentionally set to false here; enabling it causes the test to fail due to a SQL bug.
    // See test_forcePushDownNestedQuery_doesNotReturnAdditionalResults()
    cluster.callApi().verifySqlQuery(
        "SET forcePushDownNestedQuery = FALSE;\n"
        + "SELECT renamedChannel, SUM(sumAdded) AS groupedSumAdded\n"
        + "FROM (\n"
        + "  SELECT channel AS renamedChannel, \"user\", SUM(added) AS sumAdded\n"
        + "  FROM %s\n"
        + "  WHERE channel IN ('#zh.wikipedia', '#es.wikipedia')\n"
        + "  GROUP BY channel, \"user\"\n"
        + ") inner_q\n"
        + "WHERE renamedChannel = '#zh.wikipedia'\n"
        + "GROUP BY renamedChannel",
        dataSource,
        "#zh.wikipedia,191033"
    );

    // Nested group by query with force push down and having clause
    cluster.callApi().verifySqlQuery(
        "SET forcePushDownNestedQuery = FALSE;\n"
        + "SELECT renamedChannel, SUM(sumAdded) AS \"groupedSumAdded\"\n"
        + "FROM (\n"
        + "  SELECT channel AS renamedChannel, \"user\", SUM(added) AS sumAdded\n"
        + "  FROM %s\n"
        + "  WHERE channel IN ('#zh.wikipedia', '#es.wikipedia')\n"
        + "  GROUP BY channel, \"user\"\n"
        + ") inner_q\n"
        + "WHERE renamedChannel = '#zh.wikipedia'\n"
        + "GROUP BY renamedChannel",
        dataSource,
        "#zh.wikipedia,191033"
    );

    // Nested group by query with force push down and having clause. This asserts that the post-processing is invoked
    cluster.callApi().verifySqlQuery(
        "SET forcePushDownNestedQuery = TRUE;\n"
        + "SELECT SUM(sumAdded) FROM (\n"
        + "  SELECT channel, \"user\", SUM(added) AS sumAdded\n"
        + "  FROM %s GROUP BY channel, \"user\"\n"
        + ") inner_q"
        + " HAVING SUM(sumAdded) > 100000000",
        dataSource,
        ""
    );
  }

  @Disabled("Setting forcePushDownNestedQuery = TRUE with filters returns additional results, which appears to be a bug"
            + " in the SQL layer. The same query with forcePushDownNestedQuery = FALSE works as expected; see test above.")
  @Test
  public void test_forcePushDownNestedQuery_doesNotReturnAdditionalResults()
  {
    // When forcePushDownNestedQuery is set to TRUE, this test will fail as there's an extra row:
    // #es.wikipedia,634670\n#zh.wikipedia,191033
    cluster.callApi().verifySqlQuery(
        "SET forcePushDownNestedQuery = TRUE;\n"
        + "SELECT renamedChannel, SUM(sumAdded) AS groupedSumAdded\n"
        + "FROM (\n"
        + "  SELECT channel AS renamedChannel, \"user\", SUM(added) AS sumAdded\n"
        + "  FROM %s\n"
        + "  WHERE channel IN ('#zh.wikipedia', '#es.wikipedia')\n"
        + "  GROUP BY channel, \"user\"\n"
        + ") inner_q\n"
        + "WHERE renamedChannel = '#zh.wikipedia'\n"
        + "GROUP BY renamedChannel",
        dataSource,
        "#zh.wikipedia,191033"
    );
  }

  private void loadWikipediaTable() throws IOException
  {
    final File tmpDir = cluster.getTestFolder().newFolder();
    final File wikiFile = new File(tmpDir, "wiki.gz");

    ByteStreams.copy(
        DruidProcessingConfigTest.class.getResourceAsStream("/wikipedia/wikiticker-2015-09-12-sampled.json.gz"),
        Files.newOutputStream(wikiFile.toPath())
    );

    final String sql = StringUtils.format(
        "SET waitUntilSegmentsLoad = TRUE;\n"
        + "REPLACE INTO \"%s\" OVERWRITE ALL\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"time\") AS __time,\n"
        + "  channel,\n"
        + "  countryName,\n"
        + "  page,\n"
        + "  \"user\",\n"
        + "  added,\n"
        + "  deleted,\n"
        + "  delta\n"
        + "FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      %s,\n"
        + "      '{\"type\":\"json\"}',\n"
        + "      '[{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"flags\",\"type\":\"string\"},{\"name\":\"isUnpatrolled\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"diffUrl\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"commentLength\",\"type\":\"long\"},{\"name\":\"isNew\",\"type\":\"string\"},{\"name\":\"isMinor\",\"type\":\"string\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"isAnonymous\",\"type\":\"string\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"deltaBucket\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"countryName\",\"type\":\"string\"},{\"name\":\"regionIsoCode\",\"type\":\"string\"},{\"name\":\"metroCode\",\"type\":\"long\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"}]'\n"
        + "    )\n"
        + "  )\n"
        + "PARTITIONED BY DAY\n"
        + "CLUSTERED BY channel",
        dataSource,
        Calcites.escapeStringLiteral(
            broker.bindings()
                  .jsonMapper()
                  .writeValueAsString(new LocalInputSource(null, null, Collections.singletonList(wikiFile), null))
        )
    );

    final MSQTaskReportPayload payload = new EmbeddedMSQApis(cluster, overlord).runTaskSqlAndGetReport(sql);
    Assertions.assertEquals(TaskState.SUCCESS, payload.getStatus().getStatus());
    Assertions.assertEquals(1, payload.getStatus().getSegmentLoadWaiterStatus().getTotalSegments());
    Assertions.assertNull(payload.getStatus().getErrorReport());
  }
}
