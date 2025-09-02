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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.search.InsensitiveContainsSearchQuerySpec;
import org.apache.druid.query.topn.LexicographicTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UnionQueryTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addExtensions(SketchModule.class, HllSketchModule.class, DoublesSketchModule.class)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(new EmbeddedIndexer())
        .addServer(new EmbeddedBroker())
        .addServer(new EmbeddedHistorical());
  }

  @Test
  public void test_ingestData_andVerifyNativeAndSQLQueries()
  {
    final int numDatasources = 3;

    final List<String> datasourceNames = IntStream
        .range(0, numDatasources)
        .mapToObj(i -> EmbeddedClusterApis.createTestDatasourceName())
        .collect(Collectors.toList());

    for (String datasourceName : datasourceNames) {
      final Task task = MoreResources.Task.INDEX_TASK_WITH_AGGREGATORS
          .get()
          .dataSource(datasourceName)
          .withId(IdUtils.getRandomId());
      cluster.callApi().runTask(task, overlord);
      cluster.callApi().waitForAllSegmentsToBeAvailable(datasourceName, coordinator);
    }

    // Verify some native queries
    final DataSource unionDatasource = new UnionDataSource(
        datasourceNames.stream()
                       .map(TableDataSource::new)
                       .collect(Collectors.toList())
    );

    // Timeseries query
    verifyQuery(
        Druids
            .newTimeseriesQueryBuilder()
            .intervals("2013-08-31/2013-09-01")
            .dataSource(unionDatasource)
            .filters("language", "en")
            .aggregators(
                new CountAggregatorFactory("rows"),
                new LongSumAggregatorFactory("count", "ingested_events"),
                new DoubleSumAggregatorFactory("added", "added"),
                new DoubleSumAggregatorFactory("deleted", "deleted"),
                new DoubleSumAggregatorFactory("delta", "delta")
            )
            .build(),
        List.of(
            Map.of(
                "timestamp", "2013-08-31T01:02:33.000Z",
                "result", Map.of("count", 6, "delta", 561.0, "deleted", 987.0, "rows", 6, "added", 1548.0)
            )
        )
    );

    // Search query
    verifyQuery(
        Druids
            .newSearchQueryBuilder()
            .dataSource(unionDatasource)
            .intervals("2013-08-31/2013-09-01")
            .granularity(Granularities.ALL)
            .query(new InsensitiveContainsSearchQuerySpec("ip"))
            .build(),
        List.of(
            Map.of(
                "timestamp", "2013-08-31T00:00:00.000Z",
                "result", List.of(
                    Map.of("dimension", "user", "value", "triplets", "count", 3),
                    Map.of("dimension", "namespace", "value", "wikipedia", "count", 9)
                )
            )
        )
    );

    // TopN lexicographic
    verifyQuery(
        new TopNQueryBuilder()
            .dataSource(unionDatasource)
            .intervals("2013-08-31/2013-09-01")
            .granularity(Granularities.ALL)
            .aggregators(
                new CountAggregatorFactory("rows"),
                new LongSumAggregatorFactory("count", "ingested_events")
            )
            .postAggregators(
                new ArithmeticPostAggregator(
                    "sumOfRowsAndCount",
                    "+",
                    List.of(new FieldAccessPostAggregator(null, "rows"), new FieldAccessPostAggregator(null, "count"))
                )
            )
            .dimension("language")
            .metric(new LexicographicTopNMetricSpec("a"))
            .threshold(3)
            .build(),
        List.of(
            Map.of(
                "timestamp", "2013-08-31T01:02:33.000Z",
                "result",
                List.of(
                    Map.of("sumOfRowsAndCount", 12.0, "count", 6, "language", "en", "rows", 6),
                    Map.of("sumOfRowsAndCount", 6.0, "count", 3, "language", "ja", "rows", 3),
                    Map.of("sumOfRowsAndCount", 6.0, "count", 3, "language", "ru", "rows", 3)
                )
            )
        )
    );

    // Verify some SQL queries
    cluster.callApi().verifySqlQuery(
        "SELECT page, COUNT(*), SUM(ingested_events), SUM(added), SUM(deleted), SUM(delta) FROM (%s)"
        + " WHERE __time >= '2013-08-31' AND __time < '2013-09-01'"
        + " GROUP BY 1 ORDER BY 4 DESC LIMIT 3",
        unionAll("SELECT * FROM %s", datasourceNames),
        "Crimson Typhoon,3,3,2715.0,15.0,2700.0\n"
        + "Striker Eureka,3,3,1377.0,387.0,990.0\n"
        + "Cherno Alpha,3,3,369.0,36.0,333.0"
    );
    cluster.callApi().verifySqlQuery(
        "SELECT MIN(__time), MAX(__time) FROM (%s)",
        unionAll("SELECT * FROM %s", datasourceNames),
        "2013-08-31T01:02:33.000Z,2013-09-01T12:41:27.000Z"
    );
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*), SUM(ingested_events), SUM(added), SUM(deleted), SUM(delta) FROM (%s)"
        + " WHERE \"language\" = 'en' AND __time >= '2013-08-31' AND __time < '2013-09-01'",
        unionAll("SELECT * FROM %s", datasourceNames),
        "6,6,1548.0,987.0,561.0"
    );
    cluster.callApi().verifySqlQuery(
        unionAll(
            "SELECT COUNT(*), SUM(ingested_events), SUM(added), SUM(deleted), SUM(delta)"
            + " FROM %s"
            + " WHERE \"language\" = 'en' AND __time >= '2013-08-31' AND __time < '2013-09-01'",
            datasourceNames
        ),
        null,
        "2,2,516.0,329.0,187.0\n"
        + "2,2,516.0,329.0,187.0\n"
        + "2,2,516.0,329.0,187.0"
    );
  }

  /**
   * Creates a SQL for each of the datasources and then combines them with {@code UNION ALL}.
   */
  private String unionAll(String sqlFormat, List<String> datasourceNames)
  {
    return datasourceNames.stream()
                          .map(ds -> StringUtils.format(sqlFormat, ds))
                          .collect(Collectors.joining("\nUNION ALL\n"));
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
