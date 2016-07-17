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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.StringUtils;

import java.nio.ByteBuffer;

/**
 */
public class ExpressionFilter implements DimFilter
{
  private final String expression;

  @JsonCreator
  public ExpressionFilter(
      @JsonProperty("expression") String expression
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "expression can not be null");
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] expressionBytes = StringUtils.toUtf8(expression);
    return ByteBuffer.allocate(1 + expressionBytes.length)
                     .put(DimFilterUtils.EXPR_CACHE_ID)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
      {
        throw new IllegalStateException("should not be called");
      }

      @Override
      public ValueMatcher makeMatcher(ValueMatcherFactory factory)
      {
        return factory.makeExpressionMatcher(expression);
      }

      @Override
      public boolean supportsBitmapIndex(BitmapIndexSelector selector)
      {
        return false;
      }

      @Override
      public String toString()
      {
        return ExpressionFilter.this.toString();
      }
    };
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public String toString()
  {
    return "ExpressionFilter{" +
           "expression='" + expression + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return expression.hashCode();
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

    ExpressionFilter that = (ExpressionFilter) o;

    if (!expression.equals(that.expression)) {
      return false;
    }

    return true;
  }
}
