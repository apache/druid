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

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UnionQueryTest extends EmbeddedClusterTestBase
{
  private static final String UNION_SUPERVISOR_TEMPLATE = "/query/union_kafka_supervisor_template.json";
  private static final String UNION_DATA_FILE = "/query/union_data.json";
  private static final String UNION_QUERIES_RESOURCE = "/query/union_queries.json";

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
  public void testUnionQuery()
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

    final List<DataSource> dataSources = datasourceNames.stream().map(TableDataSource::new).collect(Collectors.toList());
    final DataSource unionDatasource = new UnionDataSource(dataSources);

    verifyQuery(
        Druids
            .newTimeseriesQueryBuilder()
            .intervals(Intervals.ONLY_ETERNITY)
            .dataSource(unionDatasource)
            .filters("language", "en")
            .aggregators(new CountAggregatorFactory("rows"), new LongSumAggregatorFactory("count", "count"))
            .build(),
        "[{\"timestamp\":\"2013-08-31T01:02:33.000Z\",\"result\":{\"rows\":12,\"count\":null}}]"
    );
  }

  private void verifyQuery(Query<?> query, String expectedResult)
  {
    final String result = cluster.callApi().onAnyBroker(b -> b.submitNativeQuery(query));
    Assertions.assertEquals(expectedResult, result);
  }
}
