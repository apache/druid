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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.druid.java.util.common.guava.Comparators;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ExpressionPostAggregator implements PostAggregator
{
  private static final Comparator<Comparable> DEFAULT_COMPARATOR = Comparator.nullsFirst(
      (Comparable o1, Comparable o2) -> {
        if (o1 instanceof Long && o2 instanceof Long) {
          return Long.compare((long) o1, (long) o2);
        } else if (o1 instanceof Number && o2 instanceof Number) {
          return Double.compare(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
        } else {
          return o1.compareTo(o2);
        }
      }
  );

  private final String name;
  private final String expression;
  private final Comparator<Comparable> comparator;
  private final String ordering;
  private final ExprMacroTable macroTable;
  private final Map<String, Function<Object, Object>> finalizers;

  private final Expr parsed;
  private final Set<String> dependentFields;

  /**
   * Constructor for serialization.
   */
  @JsonCreator
  public ExpressionPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression,
      @JsonProperty("ordering") String ordering,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    this(name, expression, ordering, macroTable, ImmutableMap.of());
  }

  /**
   * Constructor for {@link #decorate(Map)}.
   */
  private ExpressionPostAggregator(
      final String name,
      final String expression,
      @Nullable final String ordering,
      final ExprMacroTable macroTable,
      final Map<String, Function<Object, Object>> finalizers
  )
  {
    Preconditions.checkArgument(expression != null, "expression cannot be null");

    this.name = name;
    this.expression = expression;
    this.ordering = ordering;
    this.comparator = ordering == null ? DEFAULT_COMPARATOR : Ordering.valueOf(ordering);
    this.macroTable = macroTable;
    this.finalizers = finalizers;

    this.parsed = Parser.parse(expression, macroTable);
    this.dependentFields = ImmutableSet.copyOf(Parser.findRequiredBindings(parsed));
  }

  @Override
  public Set<String> getDependentFields()
  {
    return dependentFields;
  }

  @Override
  public Comparator getComparator()
  {
    return comparator;
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    // Maps.transformEntries is lazy, will only finalize values we actually read.
    final Map<String, Object> finalizedValues = Maps.transformEntries(
        values,
        (String k, Object v) -> {
          final Function<Object, Object> finalizer = finalizers.get(k);
          return finalizer != null ? finalizer.apply(v) : v;
        }
    );

    return parsed.eval(Parser.withMap(finalizedValues)).value();
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ExpressionPostAggregator decorate(final Map<String, AggregatorFactory> aggregators)
  {
    return new ExpressionPostAggregator(
        name,
        expression,
        ordering,
        macroTable,
        aggregators.entrySet().stream().collect(
            Collectors.toMap(
                entry -> entry.getKey(),
                entry -> entry.getValue()::finalizeComputation
            )
        )
    );
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

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.EXPRESSION)
        .appendString(expression)
        .appendString(ordering)
        .build();
  }

  public enum Ordering implements Comparator<Comparable>
  {
    // ensures the following order: numeric > NaN > Infinite
    // The name may be referenced via Ordering.valueOf(ordering) in the constructor.
    numericFirst {
      @Override
      public int compare(Comparable lhs, Comparable rhs)
      {
        if (lhs instanceof Long && rhs instanceof Long) {
          return Long.compare(((Number) lhs).longValue(), ((Number) rhs).longValue());
        } else if (lhs instanceof Number && rhs instanceof Number) {
          double d1 = ((Number) lhs).doubleValue();
          double d2 = ((Number) rhs).doubleValue();
          if (Double.isFinite(d1) && !Double.isFinite(d2)) {
            return 1;
          }
          if (!Double.isFinite(d1) && Double.isFinite(d2)) {
            return -1;
          }
          return Double.compare(d1, d2);
        } else {
          return Comparators.<Comparable>naturalNullsFirst().compare(lhs, rhs);
        }
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
