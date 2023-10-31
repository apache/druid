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

package org.apache.druid.error;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.matchers.DruidMatchers;
import org.apache.druid.query.QueryTimeoutException;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Map;

public class ErrorResponseTest
{
  @Test
  public void testSanity()
  {
    ErrorResponse response = new ErrorResponse(InvalidSqlInput.exception("bad sql!"));

    final Map<String, Object> asMap = response.getAsMap();
    MatcherAssert.assertThat(
        asMap,
        DruidMatchers.mapMatcher(
            "error", "druidException",
            "errorCode", "invalidInput",
            "persona", "USER",
            "category", "INVALID_INPUT",
            "errorMessage", "bad sql!",
            "context", ImmutableMap.of("sourceType", "sql")
        )
    );

    ErrorResponse recomposed = ErrorResponse.fromMap(asMap);

    MatcherAssert.assertThat(
        recomposed.getUnderlyingException(),
        DruidExceptionMatcher.invalidSqlInput().expectMessageIs("bad sql!")
    );
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testQueryExceptionCompat()
  {
    ErrorResponse response = new ErrorResponse(
        DruidException.fromFailure(new QueryExceptionCompat(new QueryTimeoutException()))
    );

    final Map<String, Object> asMap = response.getAsMap();
    MatcherAssert.assertThat(
        asMap,
        DruidMatchers.mapMatcher(
            "error",
            "Query timeout",

            "errorCode",
            "legacyQueryException",

            "persona",
            "OPERATOR",

            "category",
            "TIMEOUT",

            "errorMessage",
            "Query did not complete within configured timeout period. You can increase query timeout or tune the performance of query."
        )
    );
    MatcherAssert.assertThat(
        asMap,
        (Matcher) Matchers.hasEntry(
            Matchers.is("context"),
            Matchers.allOf(
                DruidMatchers.mapMatcher(
                    "errorClass", "org.apache.druid.query.QueryTimeoutException",
                    "legacyErrorCode", "Query timeout"
                ),
                Matchers.hasKey("host")
            )
        )
    );

    ErrorResponse recomposed = ErrorResponse.fromMap(asMap);

    MatcherAssert.assertThat(
        recomposed.getUnderlyingException(),
        new DruidExceptionMatcher(DruidException.Persona.OPERATOR, DruidException.Category.TIMEOUT, "legacyQueryException")
            .expectMessageIs("Query did not complete within configured timeout period. You can increase query timeout or tune the performance of query.")
    );
  }
  @Test
  public void testQueryExceptionCompatWithNullMessage()
  {
    ErrorResponse response = new ErrorResponse(DruidException.fromFailure(new QueryExceptionCompat(new QueryTimeoutException(
        null,
        "hostname"
    ))));
    final Map<String, Object> asMap = response.getAsMap();
    MatcherAssert.assertThat(
        asMap,
        DruidMatchers.mapMatcher(
            "error",
            "Query timeout",

            "errorCode",
            "legacyQueryException",

            "persona",
            "OPERATOR",

            "category",
            "TIMEOUT",

            "errorMessage",
            "null"
        )
    );
  }
}
