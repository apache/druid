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

package org.apache.druid.sql;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class SqlQueryPlusTest
{
  @Test
  public void testSetValuesAreFoldedIntoContext()
  {
    final SqlQueryPlus query = SqlQueryPlus.builder("SET lane = 'fast'; SELECT 1")
                                           .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                                           .build();

    Assertions.assertEquals("fast", query.context().get("lane"));
  }

  @Test
  public void testInvalidValuesAreRejected()
  {
    BaseCalciteQueryTest.assertDruidException(
        Assertions.assertThrows(
            DruidException.class,
            () -> SqlQueryPlus.builder("SET maxRowsQueuedForOrdering = 0; SELECT 1")
                              .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                              .build()
        ),
        DruidExceptionMatcher
            .invalidSqlInput()
            .expectMessageContains(
                "Query context parameter [maxRowsQueuedForOrdering] must be within the range [1, 2147483647], but was [0]"
            )
    );

    BaseCalciteQueryTest.assertDruidException(
        Assertions.assertThrows(
            DruidException.class,
            () -> SqlQueryPlus.builder("SELECT 1")
                              .queryContext(Map.of("sqlQueryId", 1))
                              .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                              .build()
        ),
        DruidExceptionMatcher
            .invalidSqlInput()
            .expectMessageContains("Query context parameter [sqlQueryId] should be a string, but got [1]")
    );
  }

  @Test
  public void testUnknownParametersAreRejectedIfConfigured()
  {
    final SqlQueryPlus.Builder builder = SqlQueryPlus.builder("SET myTraceId = 'abc-123'; SELECT 1")
                                                     .auth(CalciteTests.REGULAR_USER_AUTH_RESULT);
    builder.build();

    BaseCalciteQueryTest.assertDruidException(
        Assertions.assertThrows(DruidException.class, () -> builder.rejectUnknownParameters(true).build()),
        DruidExceptionMatcher
            .invalidSqlInput()
            .expectMessageContains("Unknown query context parameter [myTraceId].")
    );
  }

  @Test
  public void testSystemDefaultsAndJdbcContextAreNotValidated()
  {
    // System defaults are validated where they are configured, and the JDBC context when the connection opens.
    final SqlQueryPlus.Builder builder = SqlQueryPlus.builder("SELECT 1")
                                                     .systemDefaultContext(Map.of("myTraceId", "abc-123"))
                                                     .rejectUnknownParameters(true)
                                                     .auth(CalciteTests.REGULAR_USER_AUTH_RESULT);
    Assertions.assertEquals("abc-123", builder.build().context().get("myTraceId"));
    Assertions.assertEquals(
        Map.of("myTraceId", "abc-123"),
        builder.queryContext(Map.of("myTraceId", "abc-123")).buildJdbc().context()
    );
  }

  @Test
  public void testSyntaxError()
  {
    // SqlQueryPlus throws parse errors on build() if the statement is invalid
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> SqlQueryPlus.builder("SELECT COUNT(*) AS cnt, 'foo' AS")
                          .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                          .build()
    );

    BaseCalciteQueryTest.assertDruidException(
        e,
        DruidExceptionMatcher
            .invalidSqlInput()
            .expectMessageContains("Incorrect syntax near the keyword 'AS' at line 1, column 31")
    );
  }

  @Test
  public void testSyntaxErrorJdbc()
  {
    // SqlQueryPlus does not throw parse errors on buildJdbc(), because parsing is deferred
    final SqlQueryPlus sqlQueryPlus =
        SqlQueryPlus.builder("SELECT COUNT(*) AS cnt, 'foo' AS")
                    .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                    .buildJdbc();

    // It does throw exceptions on freshCopy(), though.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        sqlQueryPlus::freshCopy
    );

    BaseCalciteQueryTest.assertDruidException(
        e,
        DruidExceptionMatcher
            .invalidSqlInput()
            .expectMessageContains("Incorrect syntax near the keyword 'AS' at line 1, column 31")
    );
  }

  @Test
  public void testUserProvidedContextOverridesSystemDefault()
  {
    Map<String, Object> systemDefaultContext = Map.of("key", "system-default", "key2", "system-default2");
    Map<String, Object> userProvidedContext = Map.of("key", "user-provided-value");
    final SqlQueryPlus.Builder sqlQueryPlusBuilder =
        SqlQueryPlus.builder("SELECT COUNT(*) AS cnt, 'foo' AS foo")
                    .systemDefaultContext(systemDefaultContext)
                    .queryContext(userProvidedContext)
                    .auth(CalciteTests.REGULAR_USER_AUTH_RESULT);

    Assertions.assertEquals(
        Map.of("key", "user-provided-value", "key2", "system-default2"),
        sqlQueryPlusBuilder.build().context()
    );
    Assertions.assertEquals(
        Map.of("key", "user-provided-value", "key2", "system-default2"),
        sqlQueryPlusBuilder.buildJdbc().context()
    );

    Assertions.assertEquals(
        Map.of("key", "user-provided-value", "key2", "system-default2"),
        sqlQueryPlusBuilder.build().withContext(systemDefaultContext, userProvidedContext).context()
    );
    Assertions.assertEquals(
        Map.of("key", "system-default", "key2", "system-default2"),
        sqlQueryPlusBuilder.build().withContext(systemDefaultContext, Map.of()).context()
    );
    Assertions.assertEquals(
        Map.of("key", "user-provided-value"),
        sqlQueryPlusBuilder.build().withContext(Map.of(), userProvidedContext).context()
    );
  }

  @Test
  public void testNullUserValueFallsBackToSystemDefault()
  {
    final Map<String, Object> systemDefaultContext = Map.of("key", "system-default");
    final Map<String, Object> nullUserContext = new HashMap<>();
    nullUserContext.put("key", null);

    // A null in the request context
    final SqlQueryPlus.Builder builder = SqlQueryPlus.builder("SELECT 1")
                                                     .systemDefaultContext(systemDefaultContext)
                                                     .queryContext(nullUserContext)
                                                     .auth(CalciteTests.REGULAR_USER_AUTH_RESULT);
    Assertions.assertEquals(systemDefaultContext, builder.build().context());
    Assertions.assertEquals(systemDefaultContext, builder.buildJdbc().context());
    Assertions.assertEquals(
        systemDefaultContext,
        builder.build().withContext(systemDefaultContext, nullUserContext).context()
    );

    // A SET statement that unsets a value from the request context
    Assertions.assertEquals(
        systemDefaultContext,
        SqlQueryPlus.builder("SET key = NULL; SELECT 1")
                    .systemDefaultContext(systemDefaultContext)
                    .queryContext(Map.of("key", "user-provided-value"))
                    .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                    .build()
                    .context()
    );
  }
}
