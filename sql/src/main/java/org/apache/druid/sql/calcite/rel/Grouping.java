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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.DimensionExpression;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
  private final Subtotals subtotals;
  private final List<Aggregation> aggregations;
  @Nullable
  private final DimFilter havingFilter;
  private final RowSignature outputRowSignature;

  // Denotes whether the original Grouping had more dimensions which were dropped while applying projection to optimize
  // the grouping. Used for returning result which is consistent with most SQL implementations, by correspondingly
  // setting/unsetting the SKIP_EMPTY_BUCKETS flag, if the GroupBy query can be reduced to a timeseries query.
  private final boolean groupingDimensionsDropped;

  private Grouping(
      final List<DimensionExpression> dimensions,
      final Subtotals subtotals,
      final List<Aggregation> aggregations,
      @Nullable final DimFilter havingFilter,
      final RowSignature outputRowSignature
  )
  {
    this(dimensions, subtotals, aggregations, havingFilter, outputRowSignature, false);
  }

  private Grouping(
      final List<DimensionExpression> dimensions,
      final Subtotals subtotals,
      final List<Aggregation> aggregations,
      @Nullable final DimFilter havingFilter,
      final RowSignature outputRowSignature,
      final boolean groupingDimensionsDropped
  )
  {
    this.dimensions = ImmutableList.copyOf(dimensions);
    this.subtotals = Preconditions.checkNotNull(subtotals, "subtotals");
    this.aggregations = ImmutableList.copyOf(aggregations);
    this.havingFilter = havingFilter;
    this.outputRowSignature = Preconditions.checkNotNull(outputRowSignature, "outputRowSignature");
    this.groupingDimensionsDropped = groupingDimensionsDropped;

    // Verify no collisions between dimensions, aggregations, post-aggregations.
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
    for (final String field : outputRowSignature.getColumnNames()) {
      if (!seen.contains(field)) {
        throw new ISE("Missing field in rowOrder: %s", field);
      }
    }
  }

  // This method is private since groupingDimensionsDropped should only be deviated from default in
  // applyProject
  private static Grouping create(
      final List<DimensionExpression> dimensions,
      final Subtotals subtotals,
      final List<Aggregation> aggregations,
      @Nullable final DimFilter havingFilter,
      final RowSignature outputRowSignature,
      final boolean groupingDimensionsDropped
  )
  {
    return new Grouping(
        dimensions,
        subtotals,
        aggregations,
        havingFilter,
        outputRowSignature,
        groupingDimensionsDropped
    );
  }

  public static Grouping create(
      final List<DimensionExpression> dimensions,
      final Subtotals subtotals,
      final List<Aggregation> aggregations,
      @Nullable final DimFilter havingFilter,
      final RowSignature outputRowSignature
  )
  {
    return new Grouping(dimensions, subtotals, aggregations, havingFilter, outputRowSignature);
  }

  public List<DimensionExpression> getDimensions()
  {
    return dimensions;
  }

  public Subtotals getSubtotals()
  {
    return subtotals;
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

  public boolean hasGroupingDimensionsDropped()
  {
    return groupingDimensionsDropped;
  }

  /**
   * Applies a post-grouping projection.
   *
   * @see DruidQuery#computeGrouping which uses this
   */
  public Grouping applyProject(final PlannerContext plannerContext, final Project project)
  {
    final List<DimensionExpression> newDimensions = new ArrayList<>();
    final List<Aggregation> newAggregations = new ArrayList<>(aggregations);
    final Subtotals newSubtotals;

    final Projection postAggregationProjection = Projection.postAggregation(
        project,
        plannerContext,
        outputRowSignature,
        "p"
    );

    postAggregationProjection.getPostAggregators().forEach(
        postAggregator -> newAggregations.add(Aggregation.create(postAggregator))
    );

    // Remove literal dimensions that did not appear in the projection. This is useful for queries
    // like "SELECT COUNT(*) FROM tbl GROUP BY 'dummy'" which some tools can generate, and for which we don't
    // actually want to include a dimension 'dummy'.
    //
    // However, non-literal dimensions (column references) used in any GROUPING SET should not be dropped,
    // even if they are not in the projection. This ensures correct NULL value formatting.
    // See: https://github.com/apache/druid/issues/13204
    final ImmutableBitSet aggregateProjectBits = RelOptUtil.InputFinder.bits(project.getProjects(), null);
    final int[] newDimIndexes = new int[dimensions.size()];
    boolean droppedDimensions = false;

    // Collect all dimension indices referenced in any non-empty subtotal, but only when the subtotals
    // spec has real effect (i.e. multiple grouping sets). A plain GROUP BY with a single group produces
    // subtotals = [[0, 1, ...]] which has no effect and must not prevent literal dimensions from being dropped.
    // See: https://github.com/apache/druid/issues/13204
    final Set<Integer> dimensionsInSubtotals = new HashSet<>();
    if (subtotals.hasEffect(dimensions.stream()
                                      .map(DimensionExpression::toDimensionSpec)
                                      .collect(Collectors.toList()))) {
      for (IntList subtotal : subtotals.getSubtotals()) {
        if (!subtotal.isEmpty()) {
          for (int dimIndex : subtotal) {
            dimensionsInSubtotals.add(dimIndex);
          }
        }
      }
    }

    for (int i = 0; i < dimensions.size(); i++) {
      final DimensionExpression dimension = dimensions.get(i);
      final boolean isLiteral = plannerContext.parseExpression(
          dimension.getDruidExpression().getExpression()
      ).isLiteral();
      final boolean isUsedInSubtotals = dimensionsInSubtotals.contains(i);

      // Drop if it's a literal AND not in projection AND not used in any grouping set.
      // Non-literal dimensions referenced in a GROUPING SET must be preserved so that
      // subtotals which omit them correctly emit null.
      if (isLiteral && !aggregateProjectBits.get(i) && !isUsedInSubtotals) {
        droppedDimensions = true;
        newDimIndexes[i] = -1;
      } else {
        newDimIndexes[i] = newDimensions.size();
        newDimensions.add(dimension);
      }
    }

    // Renumber subtotals, if needed, to account for removed dummy dimensions.
    if (newDimensions.size() != dimensions.size()) {
      final List<IntList> newSubtotalsList = new ArrayList<>();

      for (IntList subtotal : subtotals.getSubtotals()) {
        final IntList newSubtotal = new IntArrayList();
        for (int dimIndex : subtotal) {
          final int newDimIndex = newDimIndexes[dimIndex];
          if (newDimIndex >= 0) {
            newSubtotal.add(newDimIndex);
          }
        }

        newSubtotalsList.add(newSubtotal);
      }

      newSubtotals = new Subtotals(newSubtotalsList);
    } else {
      newSubtotals = subtotals;
    }

    return Grouping.create(
        newDimensions,
        newSubtotals,
        newAggregations,
        havingFilter,
        postAggregationProjection.getOutputRowSignature(),
        droppedDimensions
    );
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
    return dimensions.equals(grouping.dimensions) &&
           subtotals.equals(grouping.subtotals) &&
           aggregations.equals(grouping.aggregations) &&
           Objects.equals(havingFilter, grouping.havingFilter) &&
           outputRowSignature.equals(grouping.outputRowSignature) &&
           groupingDimensionsDropped == grouping.groupingDimensionsDropped;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dimensions,
        subtotals,
        aggregations,
        havingFilter,
        outputRowSignature,
        groupingDimensionsDropped
    );
  }
}
