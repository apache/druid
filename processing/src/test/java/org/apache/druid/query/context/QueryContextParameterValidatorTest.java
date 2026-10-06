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

package org.apache.druid.query.context;

import org.apache.druid.query.BadQueryContextException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryContextParameterValidatorTest
{
  @Test
  void testValidateParameter()
  {
    assertThrows(BadQueryContextException.class, () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", -1));
    assertThrows(BadQueryContextException.class, () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", "not-an-int"));
    assertThrows(BadQueryContextException.class, () -> QueryContextParameterValidator.validate("useResultLevelCache", 1));
    QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", Integer.MAX_VALUE);
    QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", null);
    QueryContextParameterValidator.validate("unmigratedParameter", -1);
  }

  @Test
  void testInvalidValueMessages()
  {
    assertEquals(
        "Query context parameter [maxRowsQueuedForOrdering] should be in integer format, but got [not-an-int]",
        assertThrows(
            BadQueryContextException.class,
            () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", "not-an-int")
        ).getMessage()
    );
    assertEquals(
        "Query context parameter [maxRowsQueuedForOrdering] should be in integer format, but got [12.5]",
        assertThrows(
            BadQueryContextException.class,
            () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", 12.5)
        ).getMessage()
    );
    assertEquals(
        "Query context parameter [maxRowsQueuedForOrdering] must be within the range [1, 2147483647], but was [5000000000]",
        assertThrows(
            BadQueryContextException.class,
            () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", 5_000_000_000L)
        ).getMessage()
    );
    assertEquals(
        "Query context parameter [maxRowsQueuedForOrdering] must be within the range [1, 2147483647], but was [5000000000]",
        assertThrows(
            BadQueryContextException.class,
            () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", "5000000000")
        ).getMessage()
    );
    assertEquals(
        "Query context parameter [useResultLevelCache] should be a boolean, but got [1]",
        assertThrows(
            BadQueryContextException.class,
            () -> QueryContextParameterValidator.validate("useResultLevelCache", 1)
        ).getMessage()
    );
  }

  @Test
  void testValidateRejectsIntegerOverflow()
  {
    // Would wrap to 705032704 with Number#intValue() and pass the range check.
    assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", 5_000_000_000L)
    );
    QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", 10L);
  }

  @Test
  void testValidateParameters()
  {
    assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.validate(
            Map.of("maxRowsQueuedForOrdering", 0, "unmigratedParameter", -1)
        )
    );
    QueryContextParameterValidator.validate(Map.of("maxRowsQueuedForOrdering", 1, "unmigratedParameter", -1));
  }
}
