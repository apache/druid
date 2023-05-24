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

package org.apache.druid.iceberg.filter;

import org.apache.druid.java.util.common.Intervals;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class IcebergOrFilterTest
{
  private final String INTERVAL_COLUMN = "eventTime";
  private final String COLUMN1 = "column1";
  private final String COLUMN2 = "column2";

  private final Expression equalExpression1 = Expressions.equal(COLUMN1, "value1");
  private final Expression equalExpression2 = Expressions.equal(COLUMN2, "value2");
  private final Expression intervalExpression = Expressions.and(
      Expressions.greaterThanOrEqual(
          INTERVAL_COLUMN,
          Literal.of("2022-01-01T00:00:00.000Z")
                 .to(Types.TimestampType.withZone())
                 .value()
      ),
      Expressions.lessThan(
          INTERVAL_COLUMN,
          Literal.of("2022-01-02T00:00:00.000Z")
                 .to(Types.TimestampType.withZone())
                 .value()
      )
  );

  @Test
  public void testFilter()
  {
    IcebergOrFilter orFilter = new IcebergOrFilter(Arrays.asList(
        new IcebergEqualsFilter(COLUMN1, "value1"),
        new IcebergEqualsFilter(COLUMN2, "value2")
    ));
    Expression expectedExpression = Expressions.or(equalExpression1, equalExpression2);
    Assert.assertEquals(expectedExpression.toString(), orFilter.getFilterExpression().toString());
  }

  @Test
  public void testEmptyFilter()
  {
    Assert.assertThrows(IllegalArgumentException.class, () -> new IcebergAndFilter(null));
    Assert.assertThrows(IllegalArgumentException.class, () -> new IcebergAndFilter(Collections.emptyList()));
  }

  @Test
  public void testNestedFilters()
  {
    IcebergOrFilter filterOrOr = new IcebergOrFilter(
        Arrays.asList(
            new IcebergOrFilter(
                Arrays.asList(
                    new IcebergEqualsFilter(COLUMN1, "value1"),
                    new IcebergEqualsFilter(COLUMN2, "value2")
                )),
            new IcebergIntervalFilter(
                INTERVAL_COLUMN,
                Collections.singletonList(Intervals.of(
                    "2022-01-01T00:00:00.000Z/2022-01-02T00:00:00.000Z"))
            )
        ));
    Expression expectedExpressionOrOr = Expressions.or(
        Expressions.or(equalExpression1, equalExpression2),
        intervalExpression
    );

    IcebergOrFilter filterOrAnd = new IcebergOrFilter(
        Arrays.asList(
            new IcebergAndFilter(
                Arrays.asList(
                    new IcebergEqualsFilter(COLUMN1, "value1"),
                    new IcebergEqualsFilter(COLUMN2, "value2")
                )),
            new IcebergIntervalFilter(
                INTERVAL_COLUMN,
                Collections.singletonList(Intervals.of(
                    "2022-01-01T00:00:00.000Z/2022-01-02T00:00:00.000Z"))
            )
        ));
    Expression expectedExpressionOrAnd = Expressions.or(
        Expressions.and(equalExpression1, equalExpression2),
        intervalExpression
    );

    Assert.assertEquals(expectedExpressionOrOr.toString(), filterOrOr.getFilterExpression().toString());
    Assert.assertEquals(expectedExpressionOrAnd.toString(), filterOrAnd.getFilterExpression().toString());
  }
}
