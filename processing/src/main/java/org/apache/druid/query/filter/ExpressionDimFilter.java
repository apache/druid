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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.RangeSet;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.filter.ExpressionFilter;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public class ExpressionDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private final String expression;
  private final Supplier<Expr> parsed;
  @Nullable
  private final FilterTuning filterTuning;

  @JsonCreator
  public ExpressionDimFilter(
      @JsonProperty("expression") final String expression,
      @JsonProperty("filterTuning") @Nullable final FilterTuning filterTuning,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    this.expression = expression;
    this.filterTuning = filterTuning;
    this.parsed = Suppliers.memoize(() -> Parser.parse(expression, macroTable));
  }

  @VisibleForTesting
  public ExpressionDimFilter(final String expression, ExprMacroTable macroTable)
  {
    this(expression, null, macroTable);
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new ExpressionFilter(parsed, filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(final String dimension)
  {
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return parsed.get().analyzeInputs().getRequiredBindings();
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(DimFilterUtils.EXPRESSION_CACHE_ID)
        .appendString(expression)
        .build();
  }

  @Override
  public String toString()
  {
    return "ExpressionDimFilter{" +
           "expression='" + expression + '\'' +
           ", filterTuning=" + filterTuning +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpressionDimFilter that = (ExpressionDimFilter) o;
    return expression.equals(that.expression) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expression, filterTuning);
  }
}
