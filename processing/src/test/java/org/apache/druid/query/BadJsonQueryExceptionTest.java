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

import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.type.TypeFactory;
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
