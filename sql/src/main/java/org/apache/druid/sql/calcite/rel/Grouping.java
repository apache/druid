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

package org.apache.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.DimensionExpression;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents Druid's concept of a "group by": dimensions, aggregations, post-aggregations, and 'having' filters. This
 * is always something that can be handled by a groupBy query, and in some cases, it may be handleable by a timeseries
 * or topN query type as well.
 *
 * This corresponds to a Calcite Aggregate + optional Filter + optional Project.
 *
 * It does not include sorting, limiting, or post-sorting projections: for this, see the {@link Sorting} class.
 */
public class Grouping
{
  private final List<DimensionExpression> dimensions;
  private final List<Aggregation> aggregations;
  private final DimFilter havingFilter;
  private final RowSignature outputRowSignature;

  private Grouping(
      final List<DimensionExpression> dimensions,
      final List<Aggregation> aggregations,
      final DimFilter havingFilter,
      final RowSignature outputRowSignature
  )
  {
    this.dimensions = ImmutableList.copyOf(dimensions);
    this.aggregations = ImmutableList.copyOf(aggregations);
    this.havingFilter = havingFilter;
    this.outputRowSignature = outputRowSignature;

    // Verify no collisions.
    final Set<String> seen = new HashSet<>();
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

    // Verify that items in the output signature exist.
    for (final String field : outputRowSignature.getRowOrder()) {
      if (!seen.contains(field)) {
        throw new ISE("Missing field in rowOrder: %s", field);
      }
    }
  }

  public static Grouping create(
      final List<DimensionExpression> dimensions,
      final List<Aggregation> aggregations,
      final DimFilter havingFilter,
      final RowSignature outputRowSignature
  )
  {
    return new Grouping(dimensions, aggregations, havingFilter, outputRowSignature);
  }

  public List<DimensionExpression> getDimensions()
  {
    return dimensions;
  }

  public List<Aggregation> getAggregations()
  {
    return aggregations;
  }

  @Nullable
  public DimFilter getHavingFilter()
  {
    return havingFilter;
  }

  public List<DimensionSpec> getDimensionSpecs()
  {
    return dimensions.stream().map(DimensionExpression::toDimensionSpec).collect(Collectors.toList());
  }

  public List<AggregatorFactory> getAggregatorFactories()
  {
    return aggregations.stream()
                       .flatMap(aggregation -> aggregation.getAggregatorFactories().stream())
                       .collect(Collectors.toList());
  }

  public List<PostAggregator> getPostAggregators()
  {
    return aggregations.stream()
                       .map(Aggregation::getPostAggregator)
                       .filter(Objects::nonNull)
                       .collect(Collectors.toList());
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
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
    final Grouping grouping = (Grouping) o;
    return Objects.equals(dimensions, grouping.dimensions) &&
           Objects.equals(aggregations, grouping.aggregations) &&
           Objects.equals(havingFilter, grouping.havingFilter) &&
           Objects.equals(outputRowSignature, grouping.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, aggregations, havingFilter, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "Grouping{" +
           "dimensions=" + dimensions +
           ", aggregations=" + aggregations +
           ", havingFilter=" + havingFilter +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
