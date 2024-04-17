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

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

public class DeltaBinaryOperatorFilterTest
{
  private static final StructType SCHEMA = new StructType()
      .add(new StructField("name", StringType.STRING, true))
      .add(new StructField("age", LongType.LONG, false))
      .add(new StructField("bar", StringType.STRING, true));

  @Test
  public void testEqualsFilter()
  {
    DeltaBinaryOperatorFilter.DeltaEqualsFilter eqFilter = new DeltaBinaryOperatorFilter.DeltaEqualsFilter(
        "name",
        "Employee1"
    );

    Predicate predicate = eqFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals("=", predicate.getName());
    Assert.assertEquals(2, predicate.getChildren().size());
  }

  @Test
  public void testGreaterThanFilter()
  {
    DeltaBinaryOperatorFilter.DeltaGreaterThanFilter gtFilter = new DeltaBinaryOperatorFilter.DeltaGreaterThanFilter(
        "name",
        "Employee1"
    );

    Predicate predicate = gtFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals(">", predicate.getName());
    Assert.assertEquals(2, predicate.getChildren().size());
  }

  @Test
  public void testGreaterThanOrEqualsFilter()
  {
    DeltaBinaryOperatorFilter.DeltaGreaterThanOrEqualsFilter gteFilter = new DeltaBinaryOperatorFilter.DeltaGreaterThanOrEqualsFilter(
        "name",
        "Employee1"
    );

    Predicate predicate = gteFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals(">=", predicate.getName());
    Assert.assertEquals(2, predicate.getChildren().size());
  }

  @Test
  public void testLessThanFilter()
  {
    DeltaBinaryOperatorFilter.DeltaLessThanFilter ltFilter = new DeltaBinaryOperatorFilter.DeltaLessThanFilter(
        "name",
        "Employee1"
    );

    Predicate predicate = ltFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals("<", predicate.getName());
    Assert.assertEquals(2, predicate.getChildren().size());
  }

  @Test
  public void testLessThanOrEqualsFilter()
  {
    DeltaBinaryOperatorFilter.DeltaLessThanOrEqualsFilter lteFilter = new DeltaBinaryOperatorFilter.DeltaLessThanOrEqualsFilter(
        "name",
        "Employee1"
    );

    Predicate predicate = lteFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals("<=", predicate.getName());
    Assert.assertEquals(2, predicate.getChildren().size());
  }

  @Test
  public void testFilterWithInvalidNumericValue()
  {
    DeltaBinaryOperatorFilter.DeltaLessThanOrEqualsFilter gtFilter = new DeltaBinaryOperatorFilter.DeltaLessThanOrEqualsFilter(
        "age",
        "twentyOne"
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> gtFilter.getFilterPredicate(SCHEMA)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "column[age] has an invalid value[twentyOne]. The value must be a number, as the column's data type is [long]."
        )
    );
  }

  @Test
  public void testFilterWithNullColumn()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new DeltaBinaryOperatorFilter.DeltaEqualsFilter(null, "Employee1")
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "column is required for Delta filters."
        )
    );
  }

  @Test
  public void testFilterWithNullValue()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new DeltaBinaryOperatorFilter.DeltaEqualsFilter("name", null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "value is required for Delta filters."
        )
    );
  }
}
