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

package org.apache.druid.delta.filter;

import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.druid.delta.DeltaAssertions;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class DeltaOrFilterTest
{
  private static final StructType SCHEMA = new StructType()
      .add(new StructField("name", StringType.STRING, true))
      .add(new StructField("age", LongType.LONG, false))
      .add(new StructField("bar", StringType.STRING, true));

  @Test
  public void testOrFilter()
  {
    DeltaOrFilter orFilter = new DeltaOrFilter(
        Arrays.asList(
            new DeltaEqualsFilter("name", "Employee1"),
            new DeltaLessThanOrEqualsFilter("age", "8")
        )
    );

    Predicate predicate = orFilter.getFilterPredicate(SCHEMA);

    Assertions.assertTrue(predicate instanceof Or);
    Assertions.assertEquals(2, predicate.getChildren().size());
  }

  @Test
  public void testOrFilterWithInvalidColumn()
  {
    DeltaOrFilter orFilter = new DeltaOrFilter(
        Arrays.asList(
            new DeltaEqualsFilter("name2", "Employee1"),
            new DeltaLessThanOrEqualsFilter("age", "8")
        )
    );

    DeltaAssertions.assertInvalidInput(
        Assertions.assertThrows(DruidException.class, () -> orFilter.getFilterPredicate(SCHEMA)),
        StringUtils.format("column[name2] doesn't exist in schema[%s]", SCHEMA)
    );
  }

  @Test
  public void testOrFilterWithNoFilterPredicates()
  {
    DeltaAssertions.assertInvalidInput(
        Assertions.assertThrows(
            DruidException.class,
            () -> new DeltaOrFilter(null)
        ),
        "Delta or filter requires 2 filter predicates and must be non-empty. None provided."
    );
  }

  @Test
  public void testOrFilterWithOneFilterPredicate()
  {
    DeltaAssertions.assertInvalidInput(
        Assertions.assertThrows(
            DruidException.class,
            () -> new DeltaOrFilter(
                Collections.singletonList(
                    new DeltaEqualsFilter("name", "Employee1")
                )
            )
        ),
        "Delta or filter requires 2 filter predicates, but provided [1]."
    );
  }
}
