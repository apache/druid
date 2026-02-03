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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class QueryLaningTest extends QueryTestBase
{
  private static final String LANE_1 = "lane1";

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    broker.addProperty("druid.query.scheduler.laning.strategy", "manual")
          .addProperty("druid.query.scheduler.laning.lanes.lane1", LANE_1);

    return super.createCluster().useDefaultTimeoutForLatchableEmitter(100);
  }

  @Override
  protected void beforeAll()
  {
    jsonMapper = overlord.bindings().jsonMapper();
  }

  @Test
  public void test_queryUsesLaneInQueryContext_inManualStrategy()
  {
    final String testDatasource = ingestBasicData();
    final String result = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(createQuery("SELECT SUM(\"value\") FROM %s", testDatasource, LANE_1))
    ).trim();
    Assertions.assertEquals("3003.0", result);

    broker.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("query/priority").hasDimension("lane", LANE_1)
    );
  }

  @Test
  @Disabled("sleep() function does not seem to keep the lane occupied")
  public void test_queryFails_ifLaneIsFull()
  {
    final String testDatasource = ingestBasicData();

    // Fire a slow query which keeps the lane occupied
    executeQueryAsync(
        routerEndpoint,
        createQuery("SELECT sleep(10), SUM(\"value\") FROM %s", testDatasource, LANE_1)
    );

    // Fire another query and ensure that we get a capacity exceeded exception
    cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(createQuery("SELECT SUM(\"value\") FROM %s", testDatasource, LANE_1))
    );
  }

  private ClientSqlQuery createQuery(String sql, String dataSource, String lane)
  {
    return new ClientSqlQuery(
        StringUtils.format(sql, dataSource),
        ResultFormat.CSV.name(),
        false,
        false,
        false,
        Map.of(QueryContexts.LANE_KEY, lane),
        null
    );
  }
}
