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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.druid.testing.embedded.query.ServerManagerForQueryErrorTest.QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY;
import static org.apache.druid.testing.embedded.query.ServerManagerForQueryErrorTest.QUERY_FAILURE_TEST_CONTEXT_KEY;
import static org.apache.druid.testing.embedded.query.ServerManagerForQueryErrorTest.QUERY_TIMEOUT_TEST_CONTEXT_KEY;
import static org.apache.druid.testing.embedded.query.ServerManagerForQueryErrorTest.QUERY_UNSUPPORTED_TEST_CONTEXT_KEY;
import static org.apache.druid.testing.embedded.query.ServerManagerForQueryErrorTest.RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY;

/**
 * This class tests various query failures.
 * <p>
 * - SQL planning failures. Both {@link org.apache.calcite.sql.parser.SqlParseException}
 * and {@link org.apache.calcite.tools.ValidationException} are tested using SQLs that must fail.
 * - Various query errors from historicals. These tests use {@link ServerManagerForQueryErrorTest} to make
 * the query to always throw an exception.
 */
public class QueryErrorTest extends QueryTestBase
{
  //  TODO: introduce onAnyRouter(...) and use it; add TLS tests in the follow-up patches
  protected static List<Boolean> SHOULD_USE_SQL_ENGINE = List.of(true, false);

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    overlord.addProperty("druid.manager.segments.pollDuration", "PT0.1s");
    indexer.setServerMemory(600_000_000)
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(router)
                               .addServer(indexer)
                               .addServer(historical)
                               .addExtension(ServerManagerForQueryErrorTestModule.class);
  }

  @Override
  protected void beforeAll()
  {
    var ingestionStatus = cluster.callApi().onAnyBroker(
        b -> b.submitSqlTask(
            new ClientSqlQuery(
                "REPLACE INTO t\n"
                + "OVERWRITE ALL\n"
                + "SELECT CURRENT_TIMESTAMP AS __time, 1 AS d\n"
                + "PARTITIONED BY ALL",
                null,
                false,
                false,
                false,
                Map.of(),
                List.of()
            )
        )
    );
    cluster.callApi().waitForTaskToSucceed(ingestionStatus.getTaskId(), overlord);
    try {
      Thread.sleep(1000L);
    }
    catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testSqlParseException()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> b.submitSqlQuery(
                    new ClientSqlQuery(
                        "count(*) FROM t",
                        null,
                        false,
                        false,
                        false,
                        Map.of(),
                        List.of()
                    )
                )
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("400 Bad Request")
    );
  }

  @Test
  public void testSqlValidationException()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> b.submitSqlQuery(
                    new ClientSqlQuery(
                        "SELECT count(*) FROM lol_kek",
                        null,
                        false,
                        false,
                        false,
                        Map.of(),
                        List.of()
                    )
                )
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("400 Bad Request")
    );
  }

  @ParameterizedTest
  @FieldSource("SHOULD_USE_SQL_ENGINE")
  public void testQueryTimeout(boolean shouldUseSqlEngine)
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> queryFuture(b, shouldUseSqlEngine, QUERY_TIMEOUT_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("504")
    );
  }

  @ParameterizedTest
  @FieldSource("SHOULD_USE_SQL_ENGINE")
  public void testQueryCapacityExceeded(boolean shouldUseSqlEngine)
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> queryFuture(
                    b,
                    shouldUseSqlEngine,
                    QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY
                )
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("429")
    );
  }

  @ParameterizedTest
  @FieldSource("SHOULD_USE_SQL_ENGINE")
  public void testQueryUnsupported(boolean shouldUseSqlEngine)
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> queryFuture(
                    b,
                    shouldUseSqlEngine,
                    QUERY_UNSUPPORTED_TEST_CONTEXT_KEY
                )
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("501")
    );
  }

  @ParameterizedTest
  @FieldSource("SHOULD_USE_SQL_ENGINE")
  public void testQueryResourceLimitExceeded(boolean shouldUseSqlEngine)
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> queryFuture(
                    b,
                    shouldUseSqlEngine,
                    RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY
                )
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("400")
    );
  }

  @ParameterizedTest
  @FieldSource("SHOULD_USE_SQL_ENGINE")
  public void testQueryFailure(boolean shouldUseSqlEngine)
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> queryFuture(b, shouldUseSqlEngine, QUERY_FAILURE_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("500")
    );
  }

  private static Map<String, Object> buildTestContext(String key)
  {
    final Map<String, Object> context = new HashMap<>();
    // Disable cache so that each run hits historical.
    context.put(QueryContexts.USE_CACHE_KEY, false);
    context.put(key, true);
    return context;
  }

  /**
   * Set up a query future based on whether to use the SQL engine or native engine.
   *
   * @param shouldUseSqlEngine uses SQL engine if true, native engine otherwise.
   */
  private ListenableFuture<String> queryFuture(BrokerClient b, boolean shouldUseSqlEngine, String contextKey)
  {
    return shouldUseSqlEngine
           ? b.submitSqlQuery(new ClientSqlQuery(
        "SELECT * FROM t LIMIT 1",
        null,
        false,
        false,
        false,
        buildTestContext(contextKey),
        List.of()
    )) : b.submitNativeQuery(new Druids.ScanQueryBuilder()
                                 .dataSource("t")
                                 .eternityInterval()
                                 .limit(1)
                                 .context(buildTestContext(contextKey))
                                 .build()
    );
  }
}
