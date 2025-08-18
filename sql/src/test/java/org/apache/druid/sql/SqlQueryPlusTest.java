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
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SqlQueryPlusTest
{
  @Test
  public void testSyntaxError()
  {
    // SqlQueryPlus throws parse errors on build() if the statement is invalid
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> SqlQueryPlus.builder("SELECT COUNT(*) AS cnt, 'foo' AS")
                          .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                          .build()
    );

    MatcherAssert.assertThat(
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
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        sqlQueryPlus::freshCopy
    );

    MatcherAssert.assertThat(
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

    Assert.assertEquals(
        Map.of("key", "user-provided-value", "key2", "system-default2"),
        sqlQueryPlusBuilder.build().context()
    );
    Assert.assertEquals(
        Map.of("key", "user-provided-value", "key2", "system-default2"),
        sqlQueryPlusBuilder.buildJdbc().context()
    );

    Assert.assertEquals(
        Map.of("key", "user-provided-value", "key2", "system-default2"),
        sqlQueryPlusBuilder.build().withContext(systemDefaultContext, userProvidedContext).context()
    );
    Assert.assertEquals(
        Map.of("key", "system-default", "key2", "system-default2"),
        sqlQueryPlusBuilder.build().withContext(systemDefaultContext, Map.of()).context()
    );
    Assert.assertEquals(
        Map.of("key", "user-provided-value"),
        sqlQueryPlusBuilder.build().withContext(Map.of(), userProvidedContext).context()
    );
  }
}
