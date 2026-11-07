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

package org.apache.druid.query;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BadJsonQueryExceptionTest
{
  @Test
  public void testUsesDeepestCauseMessage()
  {
    final BadJsonQueryException exception = new BadJsonQueryException(
        valueInstantiationException(
            new RuntimeException("intermediate wrapper", new IllegalArgumentException("actionable validation message"))
        )
    );

    Assertions.assertEquals("Invalid native query: actionable validation message", exception.getMessage());
  }

  @Test
  public void testIgnoresEmptyCauseMessage()
  {
    final BadJsonQueryException exception = new BadJsonQueryException(
        valueInstantiationException(
            new RuntimeException("actionable validation message", new IllegalArgumentException(""))
        )
    );

    Assertions.assertEquals("Invalid native query: actionable validation message", exception.getMessage());
  }

  @Test
  public void testUsesFallbackWithoutCause()
  {
    final BadJsonQueryException exception = new BadJsonQueryException(valueInstantiationException(null));

    Assertions.assertEquals(
        "Invalid native query: the request contains invalid or missing fields",
        exception.getMessage()
    );
  }

  @Test
  public void testUnknownQueryType()
  {
    final BadJsonQueryException exception = readBadQuery(
        "{\"queryType\":\"noSuchQueryType\",\"dataSource\":\"ds\",\"intervals\":[\"2000/P1D\"]}"
    );

    Assertions.assertEquals(QueryException.JSON_PARSE_ERROR_CODE, exception.getErrorCode());
    Assertions.assertEquals(InvalidTypeIdException.class.getName(), exception.getErrorClass());
    // Fails at the root, so there is no reference chain to report.
    Assertions.assertTrue(exception.getMessage().startsWith("Invalid native query: "), exception.getMessage());
    Assertions.assertTrue(
        exception.getMessage().contains("Could not resolve type id 'noSuchQueryType'"),
        exception.getMessage()
    );
    Assertions.assertFalse(exception.getMessage().contains("reference chain"), exception.getMessage());
  }

  @Test
  public void testFieldOfWrongType()
  {
    final BadJsonQueryException exception = readBadQuery(
        "{\"queryType\":\"timeseries\",\"dataSource\":\"ds\",\"intervals\":[\"2000/P1D\"],\"aggregations\":\"rows\"}"
    );

    Assertions.assertEquals(QueryException.JSON_PARSE_ERROR_CODE, exception.getErrorCode());
    Assertions.assertEquals(MismatchedInputException.class.getName(), exception.getErrorClass());
    Assertions.assertTrue(exception.getMessage().startsWith("Invalid native query: "), exception.getMessage());
    Assertions.assertTrue(
        exception.getMessage().endsWith(
            "(through reference chain: org.apache.druid.query.timeseries.TimeseriesQuery[\"aggregations\"])"
        ),
        exception.getMessage()
    );
  }

  /**
   * Reads a query that is well-formed JSON but cannot be bound to a {@link Query}, and wraps the resulting
   * {@link JsonMappingException} the same way QueryResource does.
   */
  private static BadJsonQueryException readBadQuery(final String json)
  {
    final JsonMappingException e = Assertions.assertThrows(
        JsonMappingException.class,
        () -> TestHelper.JSON_MAPPER.readValue(json, Query.class)
    );
    return new BadJsonQueryException(e);
  }

  private static ValueInstantiationException valueInstantiationException(Throwable cause)
  {
    return ValueInstantiationException.from(
        null,
        "Jackson wrapper",
        TypeFactory.defaultInstance().constructType(Query.class),
        cause
    );
  }
}
