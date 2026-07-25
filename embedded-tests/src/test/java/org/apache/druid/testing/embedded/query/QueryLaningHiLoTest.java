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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Covers the case the {@code manual} strategy in {@link QueryLaningTest} cannot: the {@code hilo} strategy derives the
 * lane from the priority, so a negative-priority query is assigned to the {@code low} lane even though its context
 * never carried a lane. The lane therefore only reaches {@code query/time} if the scheduler's assignment is reported
 * back to the query lifecycle, which captured the query before the scheduler ran.
 */
public class QueryLaningHiLoTest extends QueryTestBase
{
  private static final String LOW_LANE = "low";

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    broker.addProperty("druid.query.scheduler.laning.strategy", "hilo")
          .addProperty("druid.query.scheduler.laning.maxLowPercent", "50");

    return super.createCluster().useDefaultTimeoutForLatchableEmitter(100);
  }

  @Override
  protected void beforeAll()
  {
    jsonMapper = overlord.bindings().jsonMapper();
  }

  @Test
  public void test_queryTimeReportsStrategyAssignedLane_inHiLoStrategy()
  {
    final String testDatasource = ingestBasicData();
    final String result = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(createNegativePriorityQuery("SELECT SUM(\"value\") FROM %s", testDatasource))
    ).trim();
    Assertions.assertEquals("3003.0", result);

    broker.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("query/priority").hasDimension("lane", LOW_LANE)
    );

    // The lane was never in the query context; only the laning strategy knows it.
    broker.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("query/time")
                      .hasDimension("lane", LOW_LANE)
                      .hasDimension("priority", -1)
    );
  }

  private ClientSqlQuery createNegativePriorityQuery(String sql, String dataSource)
  {
    return new ClientSqlQuery(
        StringUtils.format(sql, dataSource),
        ResultFormat.CSV.name(),
        false,
        false,
        false,
        Map.of(QueryContexts.PRIORITY_KEY, -1),
        null
    );
  }
}
