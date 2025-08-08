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

package org.apache.druid.segment.projections;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.IsBooleanFilter;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

public class Projections
{
  @Nullable
  public static <T> QueryableProjection<T> findMatchingProjection(
      CursorBuildSpec cursorBuildSpec,
      SortedSet<AggregateProjectionMetadata> projections,
      PhysicalColumnChecker physicalChecker,
      Function<String, T> getRowSelector
  )
  {
    if (cursorBuildSpec.getQueryContext().getBoolean(QueryContexts.NO_PROJECTIONS, false)) {
      return null;
    }
    final String name = cursorBuildSpec.getQueryContext().getString(QueryContexts.USE_PROJECTION);

    if (cursorBuildSpec.isAggregate()) {
      for (AggregateProjectionMetadata spec : projections) {
        if (name != null && !name.equals(spec.getSchema().getName())) {
          continue;
        }
        final ProjectionMatch match = matchAggregateProjection(spec.getSchema(), cursorBuildSpec, physicalChecker);
        if (match != null) {
          if (cursorBuildSpec.getQueryMetrics() != null) {
            cursorBuildSpec.getQueryMetrics().projection(spec.getSchema().getName());
          }
          return new QueryableProjection<>(
              match.getCursorBuildSpec(),
              match.getRemapColumns(),
              getRowSelector.apply(spec.getSchema().getName())
          );
        }
      }
    }
    if (name != null) {
      throw InvalidInput.exception("Projection[%s] specified, but does not satisfy query", name);
    }
    if (cursorBuildSpec.getQueryContext().getBoolean(QueryContexts.FORCE_PROJECTION, false)) {
      throw InvalidInput.exception("Force projections specified, but none satisfy query");
    }
    return null;
  }


  /**
   * Check if this projection "matches" a {@link CursorBuildSpec} for a query to see if we can use a projection
   * instead. For a projection to match, all grouping columns of the build spec must match, virtual columns of the
   * build spec must either be available as a physical column on the projection, or the inputs to the virtual column
   * must be available on the projection, and all aggregators must be compatible with pre-aggregated columns of the
   * projection per {@link AggregatorFactory#substituteCombiningFactory(AggregatorFactory)}. If the projection
   * matches, this method returns a {@link ProjectionMatch} which contains an updated {@link CursorBuildSpec} which has
   * the remaining virtual columns from the original build spec which must still be computed and the 'combining'
   * aggregator factories to process the pre-aggregated data from the projection, as well as a mapping of query column
   * names to projection column names.
   *
   * @param projection            the {@link AggregateProjectionMetadata.Schema} to check for match
   * @param queryCursorBuildSpec  the {@link CursorBuildSpec} that contains the required inputs to build a
   *                              {@link CursorHolder} for a query
   * @param physicalColumnChecker Helper utility which can determine if a physical column required by
   *                              queryCursorBuildSpec is available on the projection OR does not exist on the base
   *                              table either
   * @return a {@link ProjectionMatch} if the {@link CursorBuildSpec} matches the projection, else null
   */
  @Nullable
  public static ProjectionMatch matchAggregateProjection(
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker
  )
  {
    if (!queryCursorBuildSpec.isCompatibleOrdering(projection.getOrderingWithTimeColumnSubstitution())) {
      return null;
    }
    ProjectionMatchBuilder matchBuilder = new ProjectionMatchBuilder();

    // match virtual columns first, which will populate the 'remapColumns' of the match builder
    matchBuilder = matchQueryVirtualColumns(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      return null;
    }

    matchBuilder = matchFilter(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      return null;
    }

    matchBuilder = matchGrouping(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      return null;
    }

    matchBuilder = matchAggregators(projection, queryCursorBuildSpec, matchBuilder);
    if (matchBuilder == null) {
      return null;
    }

    matchBuilder = matchRemainingPhysicalColumns(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      return null;
    }

    return matchBuilder.build(queryCursorBuildSpec);
  }

  @Nullable
  public static ProjectionMatchBuilder matchQueryVirtualColumns(
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    for (VirtualColumn vc : queryCursorBuildSpec.getVirtualColumns().getVirtualColumns()) {
      matchBuilder = matchQueryVirtualColumn(
          vc,
          projection,
          queryCursorBuildSpec,
          physicalColumnChecker,
          matchBuilder
      );
      if (matchBuilder == null) {
        return null;
      }
    }
    return matchBuilder;
  }

  @Nullable
  public static ProjectionMatchBuilder matchFilter(
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    // if the projection has a filter, the query must contain this filter match
    if (projection.getFilter() != null) {
      final Filter queryFilter = queryCursorBuildSpec.getFilter();
      if (queryFilter != null) {
        final Set<String> originalRequired = queryFilter.getRequiredColumns();
        // try to rewrite the query filter into a projection filter, if the rewrite is valid, we can proceed
        final Filter projectionFilter = projection.getFilter().toOptimizedFilter(false);
        final Map<String, String> filterRewrites = new HashMap<>();
        // start with identity
        for (String required : queryFilter.getRequiredColumns()) {
          filterRewrites.put(required, required);
        }
        // overlay projection rewrites
        filterRewrites.putAll(matchBuilder.getRemapColumns());

        final Filter remappedQueryFilter = queryFilter.rewriteRequiredColumns(filterRewrites);

        final Filter rewritten = rewriteFilter(projectionFilter, remappedQueryFilter);
        // if the filter does not contain the projection filter, we cannot match this projection
        if (rewritten == null) {
          return null;
        }
        //noinspection ObjectEquality
        if (rewritten == ProjectionFilterMatch.INSTANCE) {
          // we can remove the whole thing since the query filter exactly matches the projection filter
          matchBuilder.rewriteFilter(null);
          matchBuilder.addMatchedQueryColumns(originalRequired);
        } else {
          // otherwise, we partially rewrote the query filter to eliminate the projection filter since it is baked in
          matchBuilder.rewriteFilter(rewritten);
          matchBuilder.addMatchedQueryColumns(Sets.difference(originalRequired, rewritten.getRequiredColumns()));
        }
      } else {
        // projection has a filter, but the query doesn't, no good
        return null;
      }
    } else {
      // projection doesn't have a filter, retain the original
      matchBuilder.rewriteFilter(queryCursorBuildSpec.getFilter());
    }

    // now that filter has been possibly rewritten, make sure the projection actually has all the required columns.
    if (matchBuilder.getRewriteFilter() != null) {
      for (String queryColumn : matchBuilder.getRewriteFilter().getRequiredColumns()) {
        matchBuilder = matchRequiredColumn(
            queryColumn,
            projection,
            queryCursorBuildSpec,
            physicalColumnChecker,
            matchBuilder
        );
        if (matchBuilder == null) {
          return null;
        }
      }
    }

    return matchBuilder;
  }

  @Nullable
  public static ProjectionMatchBuilder matchGrouping(
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    final List<String> queryGrouping = queryCursorBuildSpec.getGroupingColumns();
    if (queryGrouping != null) {
      for (String queryColumn : queryGrouping) {
        matchBuilder = matchRequiredColumn(
            queryColumn,
            projection,
            queryCursorBuildSpec,
            physicalColumnChecker,
            matchBuilder
        );
        if (matchBuilder == null) {
          return null;
        }
        // a query grouping column must also be defined as a projection grouping column
        if (projection.isInvalidGrouping(queryColumn)) {
          return null;
        }
        // even if remapped
        if (projection.isInvalidGrouping(matchBuilder.getRemapValue(queryColumn))) {
          return null;
        }
      }
    }
    return matchBuilder;
  }

  @Nullable
  public static ProjectionMatchBuilder matchAggregators(
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      ProjectionMatchBuilder matchBuilder
  )
  {
    if (CollectionUtils.isNullOrEmpty(queryCursorBuildSpec.getAggregators())) {
      return matchBuilder;
    }
    boolean allMatch = true;
    for (AggregatorFactory queryAgg : queryCursorBuildSpec.getAggregators()) {
      boolean foundMatch = false;
      for (AggregatorFactory projectionAgg : projection.getAggregators()) {
        final AggregatorFactory combining = queryAgg.substituteCombiningFactory(projectionAgg);
        if (combining != null) {
          matchBuilder.remapColumn(queryAgg.getName(), projectionAgg.getName())
                      .addReferencedPhysicalColumn(projectionAgg.getName())
                      .addPreAggregatedAggregator(combining)
                      .addMatchedQueryColumns(queryAgg.requiredFields());
          foundMatch = true;
          break;
        }
      }
      allMatch = allMatch && foundMatch;
    }
    if (allMatch) {
      return matchBuilder;
    }
    return null;
  }

  @Nullable
  public static ProjectionMatchBuilder matchRemainingPhysicalColumns(
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    // validate physical and virtual columns have all been accounted for
    final Set<String> matchedQueryColumns = matchBuilder.getMatchedQueryColumns();
    if (queryCursorBuildSpec.getPhysicalColumns() != null) {
      for (String queryColumn : queryCursorBuildSpec.getPhysicalColumns()) {
        // a projection always has a __time column, it just might be a constant of the segment interval start if the
        // projection itself did not transform the base table __time column
        if (ColumnHolder.TIME_COLUMN_NAME.equals(queryColumn)) {
          continue;
        }
        if (!matchedQueryColumns.contains(queryColumn)) {
          matchBuilder = matchQueryPhysicalColumn(
              queryColumn,
              projection,
              physicalColumnChecker,
              matchBuilder
          );
          if (matchBuilder == null) {
            return null;
          }
        }
      }
    }
    return matchBuilder;
  }

  /**
   * Ensure that the projection has the specified column required by a {@link CursorBuildSpec} in one form or another.
   * If the column is a {@link VirtualColumn} on the build spec, ensure that the projection has an equivalent virtual
   * column, or has the required inputs to compute the virtual column. If an equivalent virtual column exists, its
   * name will be added to {@link ProjectionMatchBuilder#remapColumn(String, String)} so the query virtual column name\
   * can be mapped to the projection physical column name. If no equivalent virtual column exists, but the inputs are
   * available on the projection to compute it, it will be added to
   * {@link ProjectionMatchBuilder#addReferenceedVirtualColumn(VirtualColumn)}.
   * <p>
   * Finally, if the column is not a virtual column in the query, it is checked with {@link PhysicalColumnChecker}
   * which true if the column is present on the projection OR if the column is NOT present on the base table (meaning
   * missing columns that do not exist anywhere do not disqualify a projection from being used).
   *
   * @param column                Column name to check
   * @param projection            {@link AggregateProjectionMetadata.Schema} to match against
   * @param queryCursorBuildSpec  the {@link CursorBuildSpec} required by the query
   * @param physicalColumnChecker Helper to check if the physical column exists on a projection, or does not exist on
   *                              the base table
   * @param matchBuilder          match state to add mappings of query virtual columns to projection physical columns
   *                              and query virtual columns which still must be computed from projection physical
   *                              columns
   * @return {@link ProjectionMatchBuilder} with updated state per the rules described above, or null if the column
   * cannot be matched
   */
  @Nullable
  public static ProjectionMatchBuilder matchRequiredColumn(
      String column,
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    if (matchBuilder.getMatchedQueryColumns().contains(column)) {
      return matchBuilder;
    }
    final VirtualColumn virtualColumn = queryCursorBuildSpec.getVirtualColumns().getVirtualColumn(column);
    if (virtualColumn != null) {
      return matchQueryVirtualColumn(
          virtualColumn,
          projection,
          queryCursorBuildSpec,
          physicalColumnChecker,
          matchBuilder
      );
    }

    return matchQueryPhysicalColumn(column, projection, physicalColumnChecker, matchBuilder);
  }

  @Nullable
  public static ProjectionMatchBuilder matchQueryVirtualColumn(
      VirtualColumn queryVirtualColumn,
      AggregateProjectionMetadata.Schema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    // check to see if we have an equivalent virtual column defined in the projection, if so we can
    final VirtualColumn projectionEquivalent = projection.getVirtualColumns().findEquivalent(queryVirtualColumn);
    if (projectionEquivalent != null) {
      final String remapColumnName;
      if (Objects.equals(projectionEquivalent.getOutputName(), projection.getTimeColumnName())) {
        remapColumnName = ColumnHolder.TIME_COLUMN_NAME;
      } else {
        remapColumnName = projectionEquivalent.getOutputName();
      }
      if (!queryVirtualColumn.getOutputName().equals(remapColumnName)) {
        matchBuilder.remapColumn(queryVirtualColumn.getOutputName(), remapColumnName);
      }
      return matchBuilder.addMatchedQueryColumn(queryVirtualColumn.getOutputName())
                         .addMatchedQueryColumns(queryVirtualColumn.requiredColumns())
                         .addReferencedPhysicalColumn(remapColumnName);
    }

    matchBuilder.addMatchedQueryColumn(queryVirtualColumn.getOutputName())
                .addReferenceedVirtualColumn(queryVirtualColumn);
    final List<String> requiredInputs = queryVirtualColumn.requiredColumns();
    if (requiredInputs.size() == 1 && ColumnHolder.TIME_COLUMN_NAME.equals(requiredInputs.get(0))) {
      // special handle time granularity. in the future this should be reworked to push this concept into the
      // virtual column and underlying expression itself, but this will do for now
      final Granularity virtualGranularity = Granularities.fromVirtualColumn(queryVirtualColumn);
      if (virtualGranularity != null) {
        if (virtualGranularity.isFinerThan(projection.getEffectiveGranularity())) {
          return null;
        }
        // same granularity, replace virtual column directly by remapping it to the physical column
        if (projection.getEffectiveGranularity().equals(virtualGranularity)) {
          return matchBuilder.remapColumn(queryVirtualColumn.getOutputName(), ColumnHolder.TIME_COLUMN_NAME)
                             .addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
        }
        return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
      } else {
        // anything else with __time requires none granularity
        if (Granularities.NONE.equals(projection.getEffectiveGranularity())) {
          return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
        }
        return null;
      }
    } else {
      for (String required : requiredInputs) {
        matchBuilder = matchRequiredColumn(
            required,
            projection,
            queryCursorBuildSpec,
            physicalColumnChecker,
            matchBuilder
        );
        if (matchBuilder == null) {
          return null;
        }
      }
      return matchBuilder;
    }
  }

  @Nullable
  public static ProjectionMatchBuilder matchQueryPhysicalColumn(
      String column,
      AggregateProjectionMetadata.Schema projection,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    if (physicalColumnChecker.check(projection.getName(), column)) {
      return matchBuilder.addMatchedQueryColumn(column)
                         .addReferencedPhysicalColumn(column);
    }
    return null;
  }

  /**
   * Rewrites a query {@link Filter} if possible, removing the {@link Filter} of a projection. To match a projection
   * filter, the query filter must be equal to the projection filter, or must contain the projection filter as the child
   * of an AND filter. This method returns null
   * indicating that a rewrite is impossible with the implication that the query cannot use the projection because the
   * projection doesn't contain all the rows the query would match if not using the projection.
   */
  @Nullable
  public static Filter rewriteFilter(Filter projectionFilter, Filter queryFilter)
  {
    if (queryFilter.equals(projectionFilter)) {
      return ProjectionFilterMatch.INSTANCE;
    }
    if (queryFilter instanceof IsBooleanFilter && ((IsBooleanFilter) queryFilter).isTrue()) {
      final IsBooleanFilter isTrueFilter = (IsBooleanFilter) queryFilter;
      final Filter rewritten = rewriteFilter(projectionFilter, isTrueFilter.getBaseFilter());
      if (rewritten == null) {
        return null;
      }
      //noinspection ObjectEquality
      if (rewritten == ProjectionFilterMatch.INSTANCE) {
        return ProjectionFilterMatch.INSTANCE;
      }
      return new IsBooleanFilter(rewritten, true);
    }
    if (queryFilter instanceof AndFilter) {
      AndFilter andFilter = (AndFilter) queryFilter;

      // if both and filters, check to see if the query and filter contains all of the clauses of the projection and filter
      if (projectionFilter instanceof AndFilter) {
        AndFilter projectionAndFilter = (AndFilter) projectionFilter;
        Filter rewritten = andFilter;
        // calling rewriteFilter using each child of the projection AND filter as the projection filter will remove
        // the child from the query AND filter if it exists (or return null if it does not exist, since it must exist
        // to be a valid rewrite). The remaining AND filter of will only contain children that were not part of the
        // projection AND filter
        for (Filter filter : projectionAndFilter.getFilters()) {
          rewritten = rewriteFilter(filter, rewritten);
          if (rewritten != null) {
            if (rewritten == ProjectionFilterMatch.INSTANCE) {
              return ProjectionFilterMatch.INSTANCE;
            }
          }
        }
        if (rewritten != null) {
          return rewritten;
        }
        return null;
      }

      // else check to see if any clause of the query AND filter is the projection filter
      List<Filter> newChildren = Lists.newArrayListWithExpectedSize(andFilter.getFilters().size());
      boolean childRewritten = false;
      for (Filter filter : andFilter.getFilters()) {
        Filter rewritten = rewriteFilter(projectionFilter, filter);
        //noinspection ObjectEquality
        if (rewritten == ProjectionFilterMatch.INSTANCE) {
          childRewritten = true;
        } else {
          if (rewritten != null) {
            newChildren.add(rewritten);
            childRewritten = true;
          } else {
            newChildren.add(filter);
          }
        }
      }
      // at least one child must have been rewritten to rewrite the AND
      if (childRewritten) {
        if (newChildren.size() > 1) {
          return new AndFilter(newChildren);
        } else {
          return newChildren.get(0);
        }
      }
      return null;
    }
    return null;
  }

  public static String getProjectionSmooshV9FileName(AggregateProjectionMetadata projectionSpec, String columnName)
  {
    return getProjectionSmooshV9Prefix(projectionSpec) + columnName;
  }

  public static String getProjectionSmooshV9Prefix(AggregateProjectionMetadata projectionSpec)
  {
    return projectionSpec.getSchema().getName() + "/";
  }

  /**
   * Returns true if column is defined in {@link AggregateProjectionSpec#getGroupingColumns()} OR if the column does not
   * exist in the base table. Part of determining if a projection can be used for a given {@link CursorBuildSpec},
   *
   * @see #matchAggregateProjection(AggregateProjectionMetadata.Schema, CursorBuildSpec, PhysicalColumnChecker)
   */
  @FunctionalInterface
  public interface PhysicalColumnChecker
  {
    boolean check(String projectionName, String columnName);
  }

  private Projections()
  {
    // no instantiation
  }
}
