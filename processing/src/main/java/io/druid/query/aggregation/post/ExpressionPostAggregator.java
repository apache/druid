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

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class ExpressionPostAggregator implements PostAggregator
{
  private static final Comparator<Number> DEFAULT_COMPARATOR = new Comparator<Number>()
  {
    @Override
    public int compare(Number o1, Number o2)
    {
      if (o1 instanceof Long && o2 instanceof Long) {
        return Long.compare(o1.longValue(), o2.longValue());
      }
      return Double.compare(o1.doubleValue(), o2.doubleValue());
    }
  };

  private final String name;
  private final String expression;
  private final Comparator comparator;
  private final String ordering;

  private final Expr parsed;
  private final List<String> dependentFields;

  @JsonCreator
  public ExpressionPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression,
      @JsonProperty("ordering") String ordering
  )
  {
    Preconditions.checkArgument(expression != null, "expression cannot be null");

    this.name = name;
    this.expression = expression;
    this.ordering = ordering;
    this.comparator = ordering == null ? DEFAULT_COMPARATOR : Ordering.valueOf(ordering);

    this.parsed = Parser.parse(expression);
    this.dependentFields = Parser.findRequiredBindings(parsed);
  }

  public ExpressionPostAggregator(String name, String fnName)
  {
    this(name, fnName, null);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(dependentFields);
  }

  @Override
  public Comparator getComparator()
  {
    return comparator;
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    return parsed.eval(Parser.withMap(values));
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty("expression")
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty("ordering")
  public String getOrdering()
  {
    return ordering;
  }

  @Override
  public String toString()
  {
    return "ExpressionPostAggregator{" +
           "name='" + name + '\'' +
           ", expression='" + expression + '\'' +
           ", ordering=" + ordering +
           '}';
  }

  public static enum Ordering implements Comparator<Number>
  {
    // ensures the following order: numeric > NaN > Infinite
    numericFirst {
      public int compare(Number lhs, Number rhs)
      {
        if (lhs instanceof Long && rhs instanceof Long) {
          return Long.compare(lhs.longValue(), rhs.longValue());
        }
        double d1 = lhs.doubleValue();
        double d2 = rhs.doubleValue();
        if (isFinite(d1) && !isFinite(d2)) {
          return 1;
        }
        if (!isFinite(d1) && isFinite(d2)) {
          return -1;
        }
        return Double.compare(d1, d2);
      }

      // Double.isFinite only exist in JDK8
      private boolean isFinite(double value)
      {
        return !Double.isInfinite(value) && !Double.isNaN(value);
      }
    }
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

    ExpressionPostAggregator that = (ExpressionPostAggregator) o;

    if (!comparator.equals(that.comparator)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }
    if (!Objects.equals(ordering, that.ordering)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + expression.hashCode();
    result = 31 * result + comparator.hashCode();
    result = 31 * result + (ordering != null ? ordering.hashCode() : 0);
    return result;
  }
}
