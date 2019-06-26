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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupByRules
{
  private GroupByRules()
  {
    // No instantiation.
  }

  /**
   * Translate an AggregateCall to Druid equivalents.
   *
   * @return translated aggregation, or null if translation failed.
   */
  public static Aggregation translateAggregateCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final Project project,
      final List<Aggregation> existingAggregations,
      final String name,
      final AggregateCall call,
      final boolean finalizeAggregations
  )
  {
    final DimFilter filter;

    if (call.filterArg >= 0) {
      // AGG(xxx) FILTER(WHERE yyy)
      if (project == null) {
        // We need some kind of projection to support filtered aggregations.
        return null;
      }

      final RexNode expression = project.getChildExps().get(call.filterArg);
      final DimFilter nonOptimizedFilter = Expressions.toFilter(
          plannerContext,
          rowSignature,
          virtualColumnRegistry,
          expression
      );
      if (nonOptimizedFilter == null) {
        return null;
      } else {
        filter = Filtration.create(nonOptimizedFilter)
                           .optimizeFilterOnly(virtualColumnRegistry.getFullRowSignature())
                           .getDimFilter();
      }
    } else {
      filter = null;
    }

    final SqlAggregator sqlAggregator = plannerContext.getOperatorTable()
                                                      .lookupAggregator(call.getAggregation());

    if (sqlAggregator == null) {
      return null;
    }

    // Compute existingAggregations for SqlAggregator impls that want it.
    final List<Aggregation> existingAggregationsWithSameFilter = new ArrayList<>();
    for (Aggregation existingAggregation : existingAggregations) {
      if (filter == null) {
        final boolean doesMatch = existingAggregation.getAggregatorFactories().stream().noneMatch(
            factory -> factory instanceof FilteredAggregatorFactory
        );

        if (doesMatch) {
          existingAggregationsWithSameFilter.add(existingAggregation);
        }
      } else {
        final boolean doesMatch = existingAggregation.getAggregatorFactories().stream().allMatch(
            factory -> factory instanceof FilteredAggregatorFactory &&
                       ((FilteredAggregatorFactory) factory).getFilter().equals(filter)
        );

        if (doesMatch) {
          existingAggregationsWithSameFilter.add(
              Aggregation.create(
                  existingAggregation.getVirtualColumns(),
                  existingAggregation.getAggregatorFactories().stream()
                                     .map(factory -> ((FilteredAggregatorFactory) factory).getAggregator())
                                     .collect(Collectors.toList()),
                  existingAggregation.getPostAggregator()
              )
          );
        }
      }
    }

    final Aggregation retVal = sqlAggregator.toDruidAggregation(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        rexBuilder,
        name,
        call,
        project,
        existingAggregationsWithSameFilter,
        finalizeAggregations
    );

    if (retVal == null) {
      return null;
    } else {
      // Check if this refers to the existingAggregationsWithSameFilter. If so, no need to apply the filter.
      if (isUsingExistingAggregation(retVal, existingAggregationsWithSameFilter)) {
        return retVal;
      } else {
        return retVal.filter(rowSignature, virtualColumnRegistry, filter);
      }
    }
  }

  /**
   * Checks if "aggregation" is exclusively based on existing aggregations from "existingAggregations'.
   */
  private static boolean isUsingExistingAggregation(
      final Aggregation aggregation,
      final List<Aggregation> existingAggregations
  )
  {
    if (!aggregation.getAggregatorFactories().isEmpty()) {
      return false;
    }

    final Set<String> existingAggregationNames = existingAggregations
        .stream()
        .flatMap(xs -> xs.getAggregatorFactories().stream())
        .map(AggregatorFactory::getName)
        .collect(Collectors.toSet());

    return existingAggregationNames.containsAll(aggregation.getPostAggregator().getDependentFields());
  }
}
