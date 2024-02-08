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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class IcebergRangeFilter implements IcebergFilter
{
  @JsonProperty
  private final String filterColumn;
  @JsonProperty
  private final Boolean lowerOpen;
  @JsonProperty
  private final Boolean upperOpen;
  @JsonProperty
  private final Object lower;
  @JsonProperty
  private final Object upper;

  @JsonCreator
  public IcebergRangeFilter(
      @JsonProperty("filterColumn") String filterColumn,
      @JsonProperty("lower") @Nullable Object lower,
      @JsonProperty("upper") @Nullable Object upper,
      @JsonProperty("lowerOpen") @Nullable Boolean lowerOpen,
      @JsonProperty("upperOpen") @Nullable Boolean upperOpen
  )
  {
    Preconditions.checkNotNull(filterColumn, "You must specify a filter column on the range filter");
    Preconditions.checkArgument(lower != null || upper != null, "Both lower and upper bounds cannot be empty");
    this.filterColumn = filterColumn;
    this.lowerOpen = lowerOpen != null ? lowerOpen : false;
    this.upperOpen = upperOpen != null ? upperOpen : false;
    this.lower = lower;
    this.upper = upper;
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

    if (lower != null) {
      Expression lowerExp = lowerOpen
                            ? Expressions.greaterThan(filterColumn, lower)
                            : Expressions.greaterThanOrEqual(filterColumn, lower);
      expressions.add(lowerExp);
    }
    if (upper != null) {
      Expression upperExp = upperOpen
                            ? Expressions.lessThan(filterColumn, upper)
                            : Expressions.lessThanOrEqual(filterColumn, upper);
      expressions.add(upperExp);
    }
    if (expressions.size() == 2) {
      return Expressions.and(expressions.get(0), expressions.get(1));
    }
    return expressions.get(0);
  }
}
