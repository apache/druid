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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
  // Introduce onAnyRouter(...) and use it; add TLS tests in the follow-up patches
  protected String tableName;

  @Override
  protected void beforeAll()
  {
    tableName = EmbeddedClusterApis.createTestDatasourceName();
    EmbeddedMSQApis msqApi = new EmbeddedMSQApis(cluster, overlord);
    SqlTaskStatus ingestionStatus = msqApi.submitTaskSql(StringUtils.format(
        "REPLACE INTO %s\n"
        + "OVERWRITE ALL\n"
        + "SELECT CURRENT_TIMESTAMP AS __time, 1 AS d PARTITIONED BY ALL",
        tableName
    ));

    cluster.callApi().waitForTaskToSucceed(ingestionStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(tableName, coordinator, broker);
  }

  @Test
  public void testSqlParseException()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.runSql("FROM foo_bar")
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
            () -> cluster.runSql("SELECT * FROM foo_bar")
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("400 Bad Request")
    );
  }

  @Test
  public void testQueryTimeout()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> sqlQueryFuture(b, QUERY_TIMEOUT_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("504")
    );

    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> nativeQueryFuture(b, QUERY_TIMEOUT_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("504")
    );
  }

  @Test
  public void testQueryCapacityExceeded()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> sqlQueryFuture(b, QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("429")
    );

    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> nativeQueryFuture(b, QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("429")
    );
  }

  @Test
  public void testQueryUnsupported()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> sqlQueryFuture(b, QUERY_UNSUPPORTED_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("501")
    );

    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> nativeQueryFuture(b, QUERY_UNSUPPORTED_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("501")
    );
  }

  @Test
  public void testQueryResourceLimitExceeded()
  {
    // SQL
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> sqlQueryFuture(b, RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("400")
    );

    // Native
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> nativeQueryFuture(b, RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("400")
    );
  }

  @Test
  public void testQueryFailure()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> sqlQueryFuture(b, QUERY_FAILURE_TEST_CONTEXT_KEY)
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("500")
    );

    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> nativeQueryFuture(b, QUERY_FAILURE_TEST_CONTEXT_KEY)
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
   * Set up a SQL query future for the test.
   */
  private ListenableFuture<String> sqlQueryFuture(BrokerClient b, String contextKey)
  {
    return b.submitSqlQuery(new ClientSqlQuery(
        StringUtils.format("SELECT * FROM %s LIMIT 1", tableName),
        null,
        false,
        false,
        false,
        buildTestContext(contextKey),
        List.of()
    ));
  }

  /**
   * Set up a native query future for the test.
   */
  private ListenableFuture<String> nativeQueryFuture(BrokerClient b, String contextKey)
  {
    return b.submitNativeQuery(new Druids.ScanQueryBuilder()
                                   .dataSource(tableName)
                                   .eternityInterval()
                                   .limit(1)
                                   .context(buildTestContext(contextKey))
                                   .build()
    );
  }
}
