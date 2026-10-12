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
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage for the {@code druid.sql.planner.requireTimeCondition} broker property.
 * Runs an in-process cluster with the guard enabled, ingests a datasource, and submits SQL
 * through the Broker's HTTP endpoint — the same path a client would take.
 */
public class EmbeddedRequireTimeConditionTest extends QueryTestBase
{
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
    Assertions.assertEquals(
        "10",
        cluster.runSql(
            "SELECT COUNT(*) FROM %s WHERE __time >= '2025-06-01' AND __time < '2025-06-11'",
            dataSourceName
        )
    );
  }

  @Test
  public void testQueryWithoutTimeConditionIsRejected()
  {
    final Throwable thrown = Assertions.assertThrows(
        Exception.class,
        () -> cluster.runSql("SELECT COUNT(*) FROM %s", dataSourceName)
    );
    ExceptionMatcher.of(HttpResponseException.class)
                    .expectMessageContains("has no filter on the __time column")
                    .assertThat(thrown);
  }
}
