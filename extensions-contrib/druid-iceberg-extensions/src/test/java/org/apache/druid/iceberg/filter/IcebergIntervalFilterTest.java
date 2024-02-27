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
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IcebergIntervalFilterTest
{
  @Test
  public void testFilter()
  {
    String intervalColumn = "eventTime";
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2022-01-01T00:00:00.000Z/2022-01-02T00:00:00.000Z"),
        Intervals.of("2023-01-01T00:00:00.000Z/2023-01-01T00:00:00.000Z")
    );

    IcebergIntervalFilter intervalFilter = new IcebergIntervalFilter(intervalColumn, intervals);
    Expression expectedExpression = Expressions.or(
        Expressions.and(
            Expressions.greaterThanOrEqual(
                intervalColumn,
                Literal.of(intervals.get(0).getStart().toString())
                       .to(Types.TimestampType.withZone())
                       .value()
            ),
            Expressions.lessThan(
                intervalColumn,
                Literal.of(intervals.get(0).getEnd().toString())
                       .to(Types.TimestampType.withZone())
                       .value()
            )
        ),
        Expressions.and(
            Expressions.greaterThanOrEqual(
                intervalColumn,
                Literal.of(intervals.get(1).getStart().toString())
                       .to(Types.TimestampType.withZone())
                       .value()
            ),
            Expressions.lessThan(
                intervalColumn,
                Literal.of(intervals.get(1).getEnd().toString())
                       .to(Types.TimestampType.withZone())
                       .value()
            )
        )
    );
    Assert.assertEquals(expectedExpression.toString(), intervalFilter.getFilterExpression().toString());
  }
}
