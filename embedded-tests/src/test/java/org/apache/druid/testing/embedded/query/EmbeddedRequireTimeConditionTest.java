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

import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for the {@code requireTimeCondition} planner guard. The guard is enabled
 * cluster-wide via the Broker property {@code druid.sql.planner.requireTimeCondition=true} so
 * the default property-driven path is exercised, and the per-query override via
 * {@link QueryContextParameters#REQUIRE_TIME_CONDITION} (which takes precedence over the
 * Broker-level default) is covered by {@link #testQueryContextOverrideCanDisableGuardPerQuery}.
 * SQL is submitted through the Broker's HTTP endpoint — the same path a client would take.
 */
public class EmbeddedRequireTimeConditionTest extends QueryTestBase
{
  private static final Map<String, Object> DISABLE_GUARD_CONTEXT = Map.of(
      QueryContextParameters.REQUIRE_TIME_CONDITION.getName(),
      false
  );

  private String dataSourceName;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    broker.addProperty("druid.sql.planner.requireTimeCondition", "true");
    return super.createCluster();
  }

  @Override
  protected void beforeAll()
  {
    dataSourceName = ingestBasicData();
  }

  @Test
  public void testQueryWithTimeConditionSucceeds()
  {
    final String result = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(buildQuery(
            "SELECT COUNT(*) FROM %s WHERE __time >= '2025-06-01' AND __time < '2025-06-11'",
            null
        ))
    );
    Assertions.assertEquals("10", result.trim());
  }

  @Test
  public void testQueryWithoutTimeConditionIsRejected()
  {
    final Throwable thrown = Assertions.assertThrows(
        Exception.class,
        () -> cluster.callApi().onAnyBroker(
            b -> b.submitSqlQuery(buildQuery("SELECT COUNT(*) FROM %s", null))
        )
    );
    ExceptionMatcher.of(HttpResponseException.class)
                    .expectMessageContains("has no filter on the __time column")
                    .assertThat(thrown);
  }

  @Test
  public void testQueryContextOverrideCanDisableGuardPerQuery()
  {
    final String result = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(buildQuery("SELECT COUNT(*) FROM %s", DISABLE_GUARD_CONTEXT))
    );
    Assertions.assertEquals("10", result.trim());
  }

  private ClientSqlQuery buildQuery(String sqlFormat, Map<String, Object> context)
  {
    return new ClientSqlQuery(
        StringUtils.format(sqlFormat, dataSourceName),
        ResultFormat.CSV.name(),
        false,
        false,
        false,
        context,
        List.of()
    );
  }
}
