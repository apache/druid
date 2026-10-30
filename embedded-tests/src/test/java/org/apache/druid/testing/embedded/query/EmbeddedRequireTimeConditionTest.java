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
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for the {@code requireTimeCondition} planner guard. The guard is enabled
 * via the SQL query context key {@link PlannerConfig#CTX_REQUIRE_TIME_CONDITION} (which takes
 * precedence over the broker-level default), the test ingests a datasource and submits SQL
 * through the Broker's HTTP endpoint — the same path a client would take.
 */
public class EmbeddedRequireTimeConditionTest extends QueryTestBase
{
  private static final Map<String, Object> REQUIRE_TIME_CONDITION_CONTEXT =
      Map.of(PlannerConfig.CTX_REQUIRE_TIME_CONDITION, true);

  private String dataSourceName;

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
            "SELECT COUNT(*) FROM %s WHERE __time >= '2025-06-01' AND __time < '2025-06-11'"
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
            b -> b.submitSqlQuery(buildQuery("SELECT COUNT(*) FROM %s"))
        )
    );
    ExceptionMatcher.of(HttpResponseException.class)
                    .expectMessageContains("has no filter on the __time column")
                    .assertThat(thrown);
  }

  private ClientSqlQuery buildQuery(String sqlFormat)
  {
    return new ClientSqlQuery(
        StringUtils.format(sqlFormat, dataSourceName),
        ResultFormat.CSV.name(),
        false,
        false,
        false,
        REQUIRE_TIME_CONDITION_CONTEXT,
        List.of()
    );
  }
}
