/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.RangeSet;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.segment.filter.ExpressionFilter;

import java.util.Objects;

public class ExpressionDimFilter implements DimFilter
{
  private final String expression;
  private final Expr parsed;

  @JsonCreator
  public ExpressionDimFilter(
      @JsonProperty("expression") final String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    this.expression = expression;
    this.parsed = Parser.parse(expression, macroTable);
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new ExpressionFilter(parsed);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(final String dimension)
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(DimFilterUtils.EXPRESSION_CACHE_ID)
        .appendString(expression)
        .build();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExpressionDimFilter that = (ExpressionDimFilter) o;
    return Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expression);
  }

  @Override
  public String toString()
  {
    return "ExpressionDimFilter{" +
           "expression='" + expression + '\'' +
           '}';
  }
}
