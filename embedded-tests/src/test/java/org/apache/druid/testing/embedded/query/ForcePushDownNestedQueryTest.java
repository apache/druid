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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.groupby.having.OrHavingSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.io.ByteStreams;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Embedded test that verifies nested group by native and SQL queries
 * when using the {@link GroupByQueryConfig#CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY} context.
 */
public class ForcePushDownNestedQueryTest extends QueryTestBase
{
  private final String interval = "2015-09-12/2015-09-13";
  private final Map<String, Object> forcePushDownNestedContext = Map.of("forcePushDownNestedQuery", "true");

  @Override
  public void beforeAll() throws IOException
  {
    dataSource = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());
    loadWikipediaTable();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // don't change the datasource name for each run because we set things up before all tests
  }

  @Test
  public void test_native_forcePushDownNestedQueryWithMultipleAggregators()
  {
    verifyQuery(
        GroupByQuery
            .builder()
            .setDataSource(
                GroupByQuery
                    .builder()
                    .setDataSource(dataSource)
                    .setInterval(interval)
                    .setDimensions(
                        new DefaultDimensionSpec("channel", null),
                        new DefaultDimensionSpec("user", null)
                    )
                    .setAggregatorSpecs(new LongSumAggregatorFactory("sumAdded", "added"))
                    .setGranularity(Granularities.ALL)
                    .setContext(Map.of("forcePushDownNestedQuery", "true"))
                    .build()
            )
            .setInterval(interval)
            .setAggregatorSpecs(new LongSumAggregatorFactory("groupedSumAdded", "sumAdded"))
            .setGranularity(Granularities.ALL)
            .setContext(forcePushDownNestedContext)
            .build(),
        List.of(
            Map.of(
                "version", "v1",
                "timestamp", "2015-09-12T00:00:00.000Z",
                "event", Map.of("groupedSumAdded", 9385573)
            )
        )
    );
  }

  @Test
  public void test_native_forcePushDownNestedQueryWithAliasDimensions()
  {
    verifyQuery(
        GroupByQuery
            .builder()
            .setDataSource(
                GroupByQuery
                    .builder()
                    .setDataSource(dataSource)
                    .setInterval(interval)
                    .setDimensions(
                        new DefaultDimensionSpec("channel", "renamedChannel"),
                        new DefaultDimensionSpec("user", "renamedUser")
                    )
                    .setAggregatorSpecs(new LongSumAggregatorFactory("sumAdded", "added"))
                    .setGranularity(Granularities.ALL)
                    .setContext(forcePushDownNestedContext)
                    .build()
            )
            .setInterval(interval)
            .setAggregatorSpecs(new LongSumAggregatorFactory("groupedSumAdded", "sumAdded"))
            .setGranularity(Granularities.ALL)
            .setContext(forcePushDownNestedContext)
            .build(),
        List.of(
            Map.of(
                "version", "v1",
                "timestamp", "2015-09-12T00:00:00.000Z",
                "event", Map.of("groupedSumAdded", 9385573)
            )
        )
    );
  }

  @Test
  public void test_native_forcePushDownNestedQueryWithFiltersInInnerAndOuterQueries()
  {
    verifyQuery(
        GroupByQuery
            .builder()
            .setDataSource(
                GroupByQuery
                    .builder()
                    .setDataSource(dataSource)
                    .setInterval(interval)
                    .setDimensions(
                        new DefaultDimensionSpec("channel", "renamedChannel"),
                        new DefaultDimensionSpec("user", "renamedUser")
                    )
                    .setAggregatorSpecs(new LongSumAggregatorFactory("sumAdded", "added"))
                    .setDimFilter(
                        new OrDimFilter(
                            List.of(
                                new SelectorDimFilter("channel", "#zh.wikipedia", null),
                                new SelectorDimFilter("channel", "#es.wikipedia", null)
                            )
                        )
                    )
                    .setGranularity(Granularities.ALL)
                    .setContext(forcePushDownNestedContext)
                    .build()
            )
            .setInterval(interval)
            .setAggregatorSpecs(new LongSumAggregatorFactory("groupedSumAdded", "sumAdded"))
            .setDimFilter(new AndDimFilter(List.of(new SelectorDimFilter("renamedChannel", "#zh.wikipedia", null))))
            .setGranularity(Granularities.ALL)
            .setContext(forcePushDownNestedContext)
            .build(),
        List.of(
            Map.of(
                "version", "v1",
                "timestamp", "2015-09-12T00:00:00.000Z",
                "event", Map.of("groupedSumAdded", 191033)
            )
        )
    );
  }

  @Test
  public void test_native_forcePushDownNestedQueryWithHavingClause()
  {
    verifyQuery(
        GroupByQuery
            .builder()
            .setDataSource(
                GroupByQuery
                    .builder()
                    .setDataSource(dataSource)
                    .setInterval(interval)
                    .setDimensions(
                        new DefaultDimensionSpec("channel", null),
                        new DefaultDimensionSpec("user", null)
                    )
                    .setAggregatorSpecs(new LongSumAggregatorFactory("sumAdded", "added"))
                    .setGranularity(Granularities.ALL)
                    .setContext(forcePushDownNestedContext)
                    .build()
            )
            .setInterval(interval)
            .setAggregatorSpecs(new LongSumAggregatorFactory("outerSum", "sumAdded"))
            .setHavingSpec(new OrHavingSpec(List.of(new GreaterThanHavingSpec("outerSum", 9_385_570))))
            .setGranularity(Granularities.ALL)
            .setContext(forcePushDownNestedContext)
            .build(),
        List.of(
            Map.of(
                "version", "v1",
                "timestamp", "2015-09-12T00:00:00.000Z",
                "event", Map.of("outerSum", 9_385_573)
            )
        )
    );
  }

  @Test
  public void test_native_forcePushDownNestedQueryWithHavingClause2()
  {
    verifyQuery(
        GroupByQuery
            .builder()
            .setDataSource(
                GroupByQuery
                    .builder()
                    .setDataSource(dataSource)
                    .setInterval(interval)
                    .setDimensions(
                        new DefaultDimensionSpec("channel", null),
                        new DefaultDimensionSpec("user", null)
                    )
                    .setAggregatorSpecs(new LongSumAggregatorFactory("sumAdded", "added"))
                    .setGranularity(Granularities.ALL)
                    .setContext(forcePushDownNestedContext)
                    .build()
            )
            .setInterval(interval)
            .setAggregatorSpecs(new LongSumAggregatorFactory("outerSum", "sumAdded"))
            .setHavingSpec(new OrHavingSpec(List.of(new GreaterThanHavingSpec("outerSum", 100_000_000))))
            .setGranularity(Granularities.ALL)
            .setContext(forcePushDownNestedContext)
            .build(),
        List.of()
    );
  }

  @Test
  public void test_sql_forcePushDownNestedQueryWithMultipleAggregators()
  {
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
  }

  @Test
  public void test_sql_forcePushDownNestedQueryWithAliasDimensions()
  {
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
  }

  /**
   * Same as {@link #test_sql_forcePushDownNestedQuery_doesNotReturnAdditionalResults},
   * but with forcePushDownNestedQuery set to false.
   */
  @Test
  public void test_sql_forcePushDownNestedDisabledWithFilters()
  {
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
  }

  @Test
  public void test_sql_forcePushDownNestedQueryWithHavingClause()
  {
    cluster.callApi().verifySqlQuery(
        "SET forcePushDownNestedQuery = TRUE;\n"
        + "SELECT SUM(sumAdded) AS outerSum\n"
        + "FROM (\n"
        + "  SELECT channel, \"user\", SUM(added) AS sumAdded\n"
        + "  FROM %s\n"
        + "  GROUP BY channel, \"user\"\n"
        + ") inner_q\n"
        + "HAVING SUM(sumAdded) > 9385570",
        dataSource,
        "9385573"
    );
  }

  @Test
  public void test_sql_forcePushDownNestedQueryWithHavingClause2()
  {
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
            + " in the SQL layer. The same query with forcePushDownNestedQuery = FALSE works as expected in test_forcePushDownNestedSql_filters_aliasQuoted_forcePushDownDisabled()")
  @Test
  public void test_sql_forcePushDownNestedQuery_doesNotReturnAdditionalResults()
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

  private void verifyQuery(Query<?> query, List<Map<String, Object>> expectedResult)
  {
    final String resultAsJson = cluster.callApi().onAnyBroker(b -> b.submitNativeQuery(query));
    final List<Map<String, Object>> resultList = JacksonUtils.readValue(
        TestHelper.JSON_MAPPER,
        resultAsJson.getBytes(StandardCharsets.UTF_8),
        new TypeReference<>() {}
    );
    Assertions.assertEquals(expectedResult, resultList);
  }
}
