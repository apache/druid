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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

public class IcebergIntervalFilter implements IcebergFilter
{
  @JsonProperty
  private final String filterColumn;

  @JsonProperty
  private final List<Interval> intervals;

  @JsonCreator
  public IcebergIntervalFilter(
      @JsonProperty("filterColumn") String filterColumn,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    Preconditions.checkNotNull(filterColumn, "You must specify a filter column on the interval filter");
    Preconditions.checkArgument(intervals != null && intervals.size() > 0, "You must specify intervals on the interval filter");
    this.filterColumn = filterColumn;
    this.intervals = intervals;
  }

  @Override
  public TableScan filter(TableScan tableScan)
  {
    return tableScan.filter(getFilterExpression());
  }

  @Override
  public Expression getFilterExpression()
  {
    List<Expression> expressions = new ArrayList<>();
    for (Interval filterInterval : intervals) {
      //  Converts the input timestamp string into iceberg TimestampType because TimestampType supports microsecond precision.
      // This is to ensure that there are no precision mismatches when doing the comparison.
      Long dateStart = (long) Literal.of(filterInterval.getStart().toString())
                                     .to(Types.TimestampType.withZone())
                                     .value();
      Long dateEnd = (long) Literal.of(filterInterval.getEnd().toString())
                                   .to(Types.TimestampType.withZone())
                                   .value();

      expressions.add(Expressions.and(
          Expressions.greaterThanOrEqual(
              filterColumn,
              dateStart
          ),
          Expressions.lessThan(
              filterColumn,
              dateEnd
          )
      ));
    }
    Expression finalExpr = Expressions.alwaysFalse();
    for (Expression filterExpr : expressions) {
      finalExpr = Expressions.or(finalExpr, filterExpr);
    }
    return finalExpr;
  }
}
