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

package io.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.DimensionExpression;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Grouping
{
  private final List<DimensionExpression> dimensions;
  private final List<Aggregation> aggregations;

  private Grouping(
      final List<DimensionExpression> dimensions,
      final List<Aggregation> aggregations
  )
  {
    this.dimensions = ImmutableList.copyOf(dimensions);
    this.aggregations = ImmutableList.copyOf(aggregations);

    // Verify no collisions.
    final Set<String> seen = Sets.newHashSet();
    for (DimensionExpression dimensionExpression : dimensions) {
      if (!seen.add(dimensionExpression.getOutputName())) {
        throw new ISE("Duplicate field name: %s", dimensionExpression.getOutputName());
      }
    }
    for (Aggregation aggregation : aggregations) {
      for (AggregatorFactory aggregatorFactory : aggregation.getAggregatorFactories()) {
        if (!seen.add(aggregatorFactory.getName())) {
          throw new ISE("Duplicate field name: %s", aggregatorFactory.getName());
        }
      }
      if (aggregation.getPostAggregator() != null && !seen.add(aggregation.getPostAggregator().getName())) {
        throw new ISE("Duplicate field name: %s", aggregation.getPostAggregator().getName());
      }
    }
  }

  public static Grouping create(
      final List<DimensionExpression> dimensions,
      final List<Aggregation> aggregations
  )
  {
    return new Grouping(dimensions, aggregations);
  }

  public List<DimensionExpression> getDimensions()
  {
    return dimensions;
  }

  public List<Aggregation> getAggregations()
  {
    return aggregations;
  }

  public List<DimensionSpec> getDimensionSpecs()
  {
    return dimensions.stream().map(DimensionExpression::toDimensionSpec).collect(Collectors.toList());
  }

  public List<AggregatorFactory> getAggregatorFactories()
  {
    final List<AggregatorFactory> retVal = Lists.newArrayList();
    for (final Aggregation aggregation : aggregations) {
      retVal.addAll(aggregation.getAggregatorFactories());
    }
    return retVal;
  }

  public List<PostAggregator> getPostAggregators()
  {
    final List<PostAggregator> retVal = Lists.newArrayList();
    for (final Aggregation aggregation : aggregations) {
      if (aggregation.getPostAggregator() != null) {
        retVal.add(aggregation.getPostAggregator());
      }
    }
    return retVal;
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

    Grouping grouping = (Grouping) o;

    if (dimensions != null ? !dimensions.equals(grouping.dimensions) : grouping.dimensions != null) {
      return false;
    }
    return aggregations != null ? aggregations.equals(grouping.aggregations) : grouping.aggregations == null;

  }

  @Override
  public int hashCode()
  {
    int result = dimensions != null ? dimensions.hashCode() : 0;
    result = 31 * result + (aggregations != null ? aggregations.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Grouping{" +
           "dimensions=" + dimensions +
           ", aggregations=" + aggregations +
           '}';
  }
}
