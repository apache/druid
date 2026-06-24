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

import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.NotFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class Projections
{
  private static final Logger log = new Logger(Projections.class);

  private static void logTrace(QueryContext context, String format, Object... args)
  {
    if (context.isDebug()) {
      log.info(format, args);
    } else {
      log.debug(format, args);
    }
  }

  public static final String BASE_TABLE_PROJECTION_NAME = "__base";

  private static final String CLUSTER_GROUP_PREFIX = BASE_TABLE_PROJECTION_NAME + "$";

  private static final ConcurrentHashMap<byte[], Boolean> PERIOD_GRAN_CACHE = new ConcurrentHashMap<>();


  public static String validateProjectionName(@Nullable String name)
  {
    if (name == null || name.isEmpty()) {
      throw InvalidInput.exception("projection name cannot be null or empty");
    }
    if (name.startsWith("__")) {
      throw InvalidInput.exception("projection cannot use reserved name[%s], names cannot start with '__'", name);
    }
    return name;
  }

  @Nullable
  public static <T> QueryableProjection<T> findMatchingProjection(
      CursorBuildSpec cursorBuildSpec,
      SortedSet<AggregateProjectionMetadata> projections,
      Interval dataInterval,
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
        final ProjectionMatch match = matchAggregateProjection(
            spec.getSchema(),
            cursorBuildSpec,
            dataInterval,
            physicalChecker
        );
        if (match != null) {
          if (cursorBuildSpec.getQueryMetrics() != null) {
            cursorBuildSpec.getQueryMetrics().projection(spec.getSchema().getName());
          }
          return new QueryableProjection<>(
              spec.getSchema().getName(),
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
   * @param projection            the {@link AggregateProjectionSchema} to check for match
   * @param queryCursorBuildSpec  the {@link CursorBuildSpec} that contains the required inputs to build a
   *                              {@link CursorHolder} for a query
   * @param physicalColumnChecker Helper utility which can determine if a physical column required by
   *                              queryCursorBuildSpec is available on the projection OR does not exist on the base
   *                              table either
   * @return a {@link ProjectionMatch} if the {@link CursorBuildSpec} matches the projection, else null
   */
  @Nullable
  public static ProjectionMatch matchAggregateProjection(
      AggregateProjectionSchema projection,
      CursorBuildSpec queryCursorBuildSpec,
      Interval dataInterval,
      PhysicalColumnChecker physicalColumnChecker
  )
  {
    if (!queryCursorBuildSpec.isCompatibleOrdering(projection.getOrderingWithTimeColumnSubstitution())) {
      logTrace(
          queryCursorBuildSpec.getQueryContext(),
          "matchAggregateProjection: projection [%s] rejected — incompatible ordering, query wants %s but projection provides %s",
          projection.getName(),
          queryCursorBuildSpec.getPreferredOrdering(),
          projection.getOrderingWithTimeColumnSubstitution()
      );
      return null;
    }
    if (CollectionUtils.isNullOrEmpty(queryCursorBuildSpec.getPhysicalColumns())) {
      logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregateProjection: projection [%s] rejected — no physical columns in query", projection.getName());
      return null;
    }

    if (isUnalignedInterval(projection, queryCursorBuildSpec, dataInterval)) {
      logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregateProjection: projection [%s] rejected — unaligned interval", projection.getName());
      return null;
    }
    ProjectionMatchBuilder matchBuilder = new ProjectionMatchBuilder();

    // match virtual columns first, which will populate the 'remapColumns' of the match builder
    matchBuilder = matchQueryVirtualColumns(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregateProjection: projection [%s] rejected — virtual column mismatch", projection.getName());
      return null;
    }

    matchBuilder = matchFilter(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregateProjection: projection [%s] rejected — filter mismatch", projection.getName());
      return null;
    }

    matchBuilder = matchGrouping(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregateProjection: projection [%s] rejected — grouping mismatch", projection.getName());
      return null;
    }

    matchBuilder = matchAggregators(projection, queryCursorBuildSpec, physicalColumnChecker, matchBuilder);
    if (matchBuilder == null) {
      logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregateProjection: projection [%s] rejected — aggregator mismatch", projection.getName());
      return null;
    }

    return matchBuilder.build(queryCursorBuildSpec);
  }

  @Nullable
  public static ProjectionMatchBuilder matchQueryVirtualColumns(
      AggregateProjectionSchema projection,
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
        logTrace(queryCursorBuildSpec.getQueryContext(), "matchQueryVirtualColumns: projection [%s] rejected — virtual column [%s] could not be matched", projection.getName(), vc.getOutputName());
        return null;
      }
    }
    return matchBuilder;
  }

  @Nullable
  public static ProjectionMatchBuilder matchFilter(
      AggregateProjectionSchema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    // if the projection has a filter, the query must contain this filter match
    if (projection.getFilter() != null) {
      final Filter queryFilter = queryCursorBuildSpec.getFilter();
      if (queryFilter != null) {
        // try to rewrite the query filter into a projection filter, if the rewrite is valid, we can proceed
        final Filter projectionFilter = projection.getFilter().toOptimizedFilter(false);
        final Filter remappedQueryFilter = remapFilterToProjection(matchBuilder, queryFilter);

        final Filter rewritten = ProjectionFilterMatch.rewriteFilter(projectionFilter, remappedQueryFilter);
        // if the filter does not contain the projection filter, we cannot match this projection
        if (rewritten == null) {
          logTrace(queryCursorBuildSpec.getQueryContext(), "matchFilter: projection [%s] rejected — query filter does not contain the projection filter", projection.getName());
          return null;
        }
        //noinspection ObjectEquality
        if (rewritten == ProjectionFilterMatch.INSTANCE) {
          // we can remove the whole thing since the query filter exactly matches the projection filter
          matchBuilder.rewriteFilter(null);
        } else {
          // otherwise, we partially rewrote the query filter to eliminate the projection filter since it is baked in
          matchBuilder.rewriteFilter(rewritten);
        }
      } else {
        // projection has a filter, but the query doesn't, no good
        logTrace(queryCursorBuildSpec.getQueryContext(), "matchFilter: projection [%s] rejected — projection has a filter but query does not", projection.getName());
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
          logTrace(queryCursorBuildSpec.getQueryContext(), "matchFilter: projection [%s] rejected — required filter column [%s] not available on projection", projection.getName(), queryColumn);
          return null;
        }
      }
    }

    return matchBuilder;
  }

  @Nullable
  public static ProjectionMatchBuilder matchGrouping(
      AggregateProjectionSchema projection,
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
          logTrace(queryCursorBuildSpec.getQueryContext(), "matchGrouping: projection [%s] rejected — grouping column [%s] not available on projection", projection.getName(), queryColumn);
          return null;
        }
        // a query grouping column must also be defined as a projection grouping column
        if (projection.isInvalidGrouping(queryColumn)) {
          logTrace(queryCursorBuildSpec.getQueryContext(), "matchGrouping: projection [%s] rejected — column [%s] is not a grouping column on the projection", projection.getName(), queryColumn);
          return null;
        }
        // even if remapped
        if (projection.isInvalidGrouping(matchBuilder.getRemapValue(queryColumn))) {
          logTrace(queryCursorBuildSpec.getQueryContext(), "matchGrouping: projection [%s] rejected — remapped column [%s] is not a grouping column on the projection", projection.getName(), matchBuilder.getRemapValue(queryColumn));
          return null;
        }
      }
    }
    return matchBuilder;
  }

  @Nullable
  public static ProjectionMatchBuilder matchAggregators(
      AggregateProjectionSchema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    if (CollectionUtils.isNullOrEmpty(queryCursorBuildSpec.getAggregators())) {
      return matchBuilder;
    }
    boolean allMatch = true;
    for (AggregatorFactory queryAgg : queryCursorBuildSpec.getAggregators()) {
      AggregatorFactory filterAgg = null;
      if (queryAgg instanceof FilteredAggregatorFactory) {
        filterAgg = ((FilteredAggregatorFactory) queryAgg).getAggregator();
      }
      boolean foundMatch = false;
      for (AggregatorFactory projectionAgg : projection.getAggregators()) {
        final AggregatorFactory combining = queryAgg.substituteCombiningFactory(projectionAgg);
        if (combining != null) {
          matchBuilder.remapColumn(queryAgg.getName(), projectionAgg.getName())
                      .addReferencedPhysicalColumn(projectionAgg.getName())
                      .addPreAggregatedAggregator(combining);
          foundMatch = true;
          break;
        }

        if (filterAgg != null) {
          final AggregatorFactory filteredCombining = filterAgg.substituteCombiningFactory(projectionAgg);
          if (filteredCombining != null) {
            FilteredAggregatorFactory filteredQueryAgg = (FilteredAggregatorFactory) queryAgg;
            final Filter aggFilter = filteredQueryAgg.getFilter().toFilter();
            final Filter remappedAggFilter = remapFilterToProjection(matchBuilder, aggFilter);
            for (String column : aggFilter.getRequiredColumns()) {
              matchBuilder = matchRequiredColumn(
                  column,
                  projection,
                  queryCursorBuildSpec,
                  physicalColumnChecker,
                  matchBuilder
              );
              if (matchBuilder == null) {
                logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregators: projection [%s] rejected — filtered aggregator [%s] requires column [%s] not available on projection", projection.getName(), queryAgg.getName(), column);
                return null;
              }
            }

            final FilteredAggregatorFactory remappedFilteredAgg = new FilteredAggregatorFactory(
                filteredCombining,
                new RewrittenAggDimFilter(filteredQueryAgg.getFilter(), remappedAggFilter)
            );
            matchBuilder.remapColumn(queryAgg.getName(), projectionAgg.getName())
                        .addReferencedPhysicalColumn(projectionAgg.getName())
                        .addPreAggregatedAggregator(remappedFilteredAgg);
            foundMatch = true;
            break;
          }
        }
      }
      allMatch = allMatch && foundMatch;
    }
    if (allMatch) {
      return matchBuilder;
    }
    logTrace(queryCursorBuildSpec.getQueryContext(), "matchAggregators: projection [%s] rejected — one or more query aggregators could not be matched to a projection aggregator", projection.getName());
    return null;
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
   * @param projection            {@link AggregateProjectionSchema} to match against
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
      AggregateProjectionSchema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
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

    return matchQueryPhysicalColumn(column, projection, physicalColumnChecker, matchBuilder, queryCursorBuildSpec.getQueryContext());
  }

  @Nullable
  public static ProjectionMatchBuilder matchQueryVirtualColumn(
      VirtualColumn queryVirtualColumn,
      AggregateProjectionSchema projection,
      CursorBuildSpec queryCursorBuildSpec,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder
  )
  {
    // check to see if we have an equivalent virtual column defined in the projection, if so we can
    final VirtualColumns.Node queryNode =
        queryCursorBuildSpec.getVirtualColumns().getNode(queryVirtualColumn.getOutputName());
    final VirtualColumn projectionEquivalent =
        queryNode != null ? projection.getVirtualColumns().findEquivalent(queryNode) : null;
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
      return matchBuilder.addReferencedPhysicalColumn(remapColumnName);
    }

    matchBuilder.addReferenceedVirtualColumn(queryVirtualColumn);
    final List<String> requiredInputs = queryVirtualColumn.requiredColumns();
    if (requiredInputs.size() == 1 && ColumnHolder.TIME_COLUMN_NAME.equals(requiredInputs.get(0))) {
      // special handle time granularity. in the future this should be reworked to push this concept into the
      // virtual column and underlying expression itself, but this will do for now
      final Granularity virtualGranularity = Granularities.fromVirtualColumn(queryVirtualColumn);
      if (virtualGranularity != null) {
        // same granularity, replace virtual column directly by remapping it to the physical column
        if (projection.getEffectiveGranularity().equals(virtualGranularity)) {
          return matchBuilder.remapColumn(queryVirtualColumn.getOutputName(), ColumnHolder.TIME_COLUMN_NAME)
                             .addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
        } else if (Granularities.ALL.equals(virtualGranularity)
                   || Granularities.NONE.equals(projection.getEffectiveGranularity())) {
          // if virtual gran is ALL or projection gran is NONE, it's guaranteed that projection gran can be mapped to virtual gran
          return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
        } else if (virtualGranularity instanceof PeriodGranularity
                   && projection.getEffectiveGranularity() instanceof PeriodGranularity) {
          PeriodGranularity virtualGran = (PeriodGranularity) virtualGranularity;
          PeriodGranularity projectionGran = (PeriodGranularity) projection.getEffectiveGranularity();
          byte[] combinedKey = new CacheKeyBuilder((byte) 0x0).appendCacheable(projectionGran)
                                                              .appendCacheable(virtualGran)
                                                              .build();
          if (PERIOD_GRAN_CACHE.computeIfAbsent(combinedKey, (unused) -> projectionGran.canBeMappedTo(virtualGran))) {
            return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
          }
        }
        // if it reaches here, can be one of the following cases:
        // 1. virtual gran is NONE, and projection gran is not
        // 2. projection gran is ALL, and virtual gran is not
        // 3. both are period granularities, but projection gran can't be mapped to virtual gran, e.x. PT2H can't be mapped to PT1H
        logTrace(queryCursorBuildSpec.getQueryContext(), "matchQueryVirtualColumn: projection [%s] rejected — virtual column [%s] granularity [%s] is incompatible with projection granularity [%s]", projection.getName(), queryVirtualColumn.getOutputName(), virtualGranularity, projection.getEffectiveGranularity());
        return null;
      } else {
        // we can't decide query granularity for the virtual column with __time, requires none granularity to be safe
        if (Granularities.NONE.equals(projection.getEffectiveGranularity())) {
          return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
        }
        logTrace(queryCursorBuildSpec.getQueryContext(), "matchQueryVirtualColumn: projection [%s] rejected — virtual column [%s] uses __time but projection granularity [%s] is not NONE", projection.getName(), queryVirtualColumn.getOutputName(), projection.getEffectiveGranularity());
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
          logTrace(queryCursorBuildSpec.getQueryContext(), "matchQueryVirtualColumn: projection [%s] rejected — virtual column [%s] requires input [%s] not available on projection", projection.getName(), queryVirtualColumn.getOutputName(), required);
          return null;
        }
      }
      return matchBuilder;
    }
  }

  @Nullable
  public static ProjectionMatchBuilder matchQueryPhysicalColumn(
      String column,
      AggregateProjectionSchema projection,
      PhysicalColumnChecker physicalColumnChecker,
      ProjectionMatchBuilder matchBuilder,
      QueryContext context
  )
  {
    // if we need __time as a physical column, the projection must be grouping on __time directly
    if (ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(projection.getTimeColumnName())) {
        return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
      }
      logTrace(context, "matchQueryPhysicalColumn: projection [%s] rejected — query requires __time as a physical column but projection does not group on __time", projection.getName());
      return null;
    }
    if (physicalColumnChecker.check(projection.getName(), column)) {
      return matchBuilder.addReferencedPhysicalColumn(column);
    }
    logTrace(context, "matchQueryPhysicalColumn: projection [%s] rejected — column [%s] is not available on projection", projection.getName(), column);
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

  public static String getProjectionSegmentInternalFileName(ProjectionSchema schema, String columnName)
  {
    return getProjectionSegmentInternalFilePrefix(schema) + columnName;
  }

  public static String getProjectionSegmentInternalFilePrefix(ProjectionSchema projectionSchema)
  {
    return projectionSchema.getName() + "/";
  }

  /**
   * Check whether {@code type} is an allowed cluster group clustering-column type. Clustering is restricted to the
   * primitive scalar types: {@link ValueType#STRING}, {@link ValueType#LONG}, {@link ValueType#DOUBLE},
   * {@link ValueType#FLOAT}. Complex and array types are rejected.
   */
  public static boolean isAllowedClusteringType(@Nullable ColumnType type)
  {
    return type != null && type.anyOf(ValueType.STRING, ValueType.LONG, ValueType.DOUBLE, ValueType.FLOAT);
  }

  /**
   * Segment internal file prefix + column for a cluster group's per-group column data:
   * {@code __base$<id0>_<id1>...<idK>/<column>}
   */
  public static String getClusterGroupSegmentInternalFileName(List<Integer> clusteringValueIds, String column)
  {
    return getClusterGroupSegmentInternalFilePrefix(clusteringValueIds) + column;
  }

  public static String getClusterGroupSegmentInternalFilePrefix(List<Integer> clusteringValueIds)
  {
    return getClusterGroupBundleName(clusteringValueIds) + '/';
  }

  /**
   * Bundle name for a cluster group's containers in the V10 file, matching the tag {@code IndexMergerV10} applies at
   * write time: {@code __base$<id0>_<id1>...<idK>}. This is the cluster group's canonical identity; the per-group file
   * prefix ({@link #getClusterGroupSegmentInternalFilePrefix}) is just this name plus a trailing {@code '/'} separator.
   */
  public static String getClusterGroupBundleName(List<Integer> clusteringValueIds)
  {
    if (clusteringValueIds == null || clusteringValueIds.isEmpty()) {
      throw DruidException.defensive("clusteringValueIds must not be null or empty");
    }
    final StringBuilder sb = new StringBuilder(CLUSTER_GROUP_PREFIX);
    for (int i = 0; i < clusteringValueIds.size(); i++) {
      if (i > 0) {
        sb.append('_');
      }
      sb.append(clusteringValueIds.get(i));
    }
    return sb.toString();
  }

  /**
   * Build the per-query {@link ClusterGroupQueryPlan} for {@code groups} against a {@link CursorBuildSpec}. Walks the
   * filter tree once per group via {@link #walkClusterGroupFilter}, folding clustering-column leaves to
   * {@link TrueFilter} / {@link FalseFilter} against each group's constant clustering tuple and propagating those
   * constants through AND / OR / NOT. Non-clustering filters remain in place so the per-group cursor evaluates them
   * as expected. Query-VC-equivalent-to-clustering-VC resolution happens per-leaf via {@link #resolveClusteringIndex}.
   * <p/>
   * Output shape per group encodes the truth value: top-level {@link FalseFilter} = provably FALSE (group is
   * pruned from {@link ClusterGroupQueryPlan#survivingGroups()}), top-level {@link TrueFilter} = provably TRUE
   * (no residual filter needed at the cursor), anything else = UNKNOWN (residual filter passed to the per-group
   * cursor). The walker's result is stashed on the plan so {@link ClusterGroupQueryPlan#rewriteFor} hands it back
   * directly without re-walking.
   */
  public static ClusterGroupQueryPlan planClusterGroupQuery(
      List<TableClusterGroupSpec> groups,
      CursorBuildSpec cursorBuildSpec
  )
  {
    final Filter queryFilter = cursorBuildSpec.getFilter();
    final VirtualColumns queryVcs = cursorBuildSpec.getVirtualColumns();
    if (groups.isEmpty() || queryFilter == null) {
      // No filter (or no groups): every group survives, per-group rewrite is a no-op (null filter).
      return new ClusterGroupQueryPlan(groups, group -> null);
    }

    // Every spec in the list shares one summary by construction (set once in the schema constructor), so
    // clusteringColumns + groupVcs are loop-invariant, only the per-group clustering tuple changes.
    final ClusteredValueGroupsBaseTableSchema summary = groups.getFirst().getSummary();
    final RowSignature clusteringColumns = summary.getClusteringColumns();
    final VirtualColumns groupVcs = summary.getVirtualColumns();

    // Single walk per group: produces the rewritten filter, and a top-level FalseFilter means the group prunes.
    // Cache the rewrite for every group (including pruned ones, where it's FalseFilter) so rewriteFor doesn't
    // re-walk for either the cursor factory or callers that want to inspect a pruned group's outcome directly.
    final List<TableClusterGroupSpec> kept = new ArrayList<>(groups.size());
    final IdentityHashMap<TableClusterGroupSpec, Filter> rewriteCache = new IdentityHashMap<>();
    for (TableClusterGroupSpec group : groups) {
      final Filter rewritten = walkClusterGroupFilter(
          queryFilter,
          clusteringColumns,
          group.lookupClusteringValues(),
          queryVcs,
          groupVcs
      );
      rewriteCache.put(group, rewritten);
      if (!(rewritten instanceof FalseFilter)) {
        kept.add(group);
      }
    }
    return new ClusterGroupQueryPlan(kept, rewriteCache::get);
  }

  /**
   * Walk the filter tree against {@code group}'s constant clustering tuple and return a rewritten filter where each
   * leaf whose column resolves to a clustering column (physical or virtual) is folded to {@link TrueFilter} /
   * {@link FalseFilter}, with those constants propagated through AND / OR / NOT. All other filters remain unchanged.
   * <p/>
   * Output shape encodes the truth value implicitly: a top-level {@link FalseFilter} means the filter is provably
   * FALSE against this group's clustering tuple (the planner uses this to decide which groups to prune); a top-level
   * {@link TrueFilter} means it's provably TRUE (no residual filter needed at the cursor); anything else means
   * UNKNOWN and the rewritten filter exists to push down to the per-group cursor.
   */
  private static Filter walkClusterGroupFilter(
      Filter filter,
      RowSignature clusteringColumns,
      Object[] clusteringValues,
      VirtualColumns queryVcs,
      VirtualColumns groupVcs
  )
  {
    if (filter instanceof AndFilter andFilter) {
      final List<Filter> kept = new ArrayList<>(andFilter.getFilters().size());
      for (Filter sub : andFilter.getFilters()) {
        final Filter rewritten =
            walkClusterGroupFilter(sub, clusteringColumns, clusteringValues, queryVcs, groupVcs);
        if (rewritten instanceof FalseFilter) {
          return FalseFilter.instance();   // AND short-circuits on FALSE
        }
        if (rewritten instanceof TrueFilter) {
          continue;   // drop TRUE children
        }
        kept.add(rewritten);
      }
      if (kept.isEmpty()) {
        return TrueFilter.instance();
      }
      if (kept.size() == 1) {
        return kept.get(0);
      }
      return new AndFilter(kept);
    }

    if (filter instanceof OrFilter orFilter) {
      final List<Filter> kept = new ArrayList<>(orFilter.getFilters().size());
      for (Filter sub : orFilter.getFilters()) {
        final Filter rewritten =
            walkClusterGroupFilter(sub, clusteringColumns, clusteringValues, queryVcs, groupVcs);
        if (rewritten instanceof TrueFilter) {
          return TrueFilter.instance();   // OR short-circuits on TRUE
        }
        if (rewritten instanceof FalseFilter) {
          continue;   // drop FALSE children
        }
        kept.add(rewritten);
      }
      if (kept.isEmpty()) {
        return FalseFilter.instance();
      }
      if (kept.size() == 1) {
        return kept.get(0);
      }
      return new OrFilter(kept);
    }

    if (filter instanceof NotFilter notFilter) {
      final Filter inner = walkClusterGroupFilter(
          notFilter.getBaseFilter(),
          clusteringColumns,
          clusteringValues,
          queryVcs,
          groupVcs
      );
      if (inner instanceof TrueFilter) {
        return FalseFilter.instance();
      }
      if (inner instanceof FalseFilter) {
        return TrueFilter.instance();
      }
      return new NotFilter(inner);
    }

    if (filter instanceof NullFilter isNull) {
      final int idx = resolveClusteringIndex(isNull.getColumn(), clusteringColumns, queryVcs, groupVcs);
      if (idx < 0) {
        return filter;
      }
      return clusteringValues[idx] == null ? TrueFilter.instance() : FalseFilter.instance();
    }

    if (filter instanceof EqualityFilter eq) {
      final int idx = resolveClusteringIndex(eq.getColumn(), clusteringColumns, queryVcs, groupVcs);
      if (idx < 0) {
        return filter;
      }
      // EqualityFilter doesn't match nulls by design; the constructor also rejects null match values.
      if (clusteringValues[idx] == null) {
        return FalseFilter.instance();
      }
      return Objects.equals(clusteringValues[idx], eq.getMatchValue())
             ? TrueFilter.instance()
             : FalseFilter.instance();
    }

    if (filter instanceof TypedInFilter in) {
      final int idx = resolveClusteringIndex(in.getColumn(), clusteringColumns, queryVcs, groupVcs);
      if (idx < 0) {
        return filter;
      }
      // TypedInFilter matches nulls if present in the values list. Iterate explicitly — immutable List impls
      // (List.of, ImmutableList) NPE on contains(null).
      final Object val = clusteringValues[idx];
      for (Object v : in.getSortedValues()) {
        if (Objects.equals(v, val)) {
          return TrueFilter.instance();
        }
      }
      return FalseFilter.instance();
    }

    // Anything else: not a recognized clustering-column leaf shape, leave as-is. The cursor will evaluate it per-row
    // like any other non-clustering filter.
    return filter;
  }

  /**
   * Resolve a filter leaf's column name to a clustering-column index for this group's clustering tuple. Query-VC
   * lookup wins first because query VC names are allowed to shadow physical/clustering column names: if a query VC
   * by that name exists, the only path to a clustering column is via
   * {@link VirtualColumns#findEquivalent(VirtualColumns.Node)} against the group's clustering VCs. Otherwise, fall
   * through to a direct name lookup against the clustering signature. Returns {@code -1} when the leaf doesn't
   * reference a clustering column — including the operator-VC-shadows-without-equivalence case, which is
   * intentionally treated as "leave the leaf unchanged" so the cursor evaluates it through the query VCs as it
   * would any other filter.
   */
  private static int resolveClusteringIndex(
      String column,
      RowSignature clusteringColumns,
      VirtualColumns queryVcs,
      VirtualColumns groupVcs
  )
  {
    final VirtualColumns.Node queryNode = queryVcs.getNode(column);
    if (queryNode != null) {
      final VirtualColumn equivalent = groupVcs.findEquivalent(queryNode);
      if (equivalent == null) {
        return -1;
      }
      return clusteringColumns.indexOf(equivalent.getOutputName());
    }
    return clusteringColumns.indexOf(column);
  }

  /**
   * Check that the query {@link CursorBuildSpec} either contains the entire data interval, or that the query interval
   * is aligned with {@link AggregateProjectionSchema#getEffectiveGranularity()}
   */
  private static boolean isUnalignedInterval(
      AggregateProjectionSchema projection,
      CursorBuildSpec queryCursorBuildSpec,
      Interval dataInterval
  )
  {
    final Interval queryInterval = queryCursorBuildSpec.getInterval();
    if (!queryInterval.contains(dataInterval)) {
      final Granularity granularity = projection.getEffectiveGranularity();
      final DateTime start = queryInterval.getStart();
      final DateTime end = queryInterval.getEnd();
      // the interval filter must align with the projection granularity for a match to be valid
      return !start.equals(granularity.bucketStart(start)) || !end.equals(granularity.bucketStart(end));
    }
    return false;
  }

  private static Filter remapFilterToProjection(ProjectionMatchBuilder matchBuilder, Filter aggFilter)
  {
    final Map<String, String> filterRewrites = new HashMap<>();
    // start with identity
    for (String required : aggFilter.getRequiredColumns()) {
      filterRewrites.put(required, required);
    }
    // overlay projection rewrites
    filterRewrites.putAll(matchBuilder.getRemapColumns());

    final Filter remappedAggFilter = aggFilter.rewriteRequiredColumns(filterRewrites);
    return remappedAggFilter;
  }

  /**
   * Returns true if column is defined in {@link AggregateProjectionSpec#getGroupingColumns()} OR if the column does not
   * exist in the base table. Part of determining if a projection can be used for a given {@link CursorBuildSpec},
   *
   * @see #matchAggregateProjection(AggregateProjectionSchema, CursorBuildSpec, Interval, PhysicalColumnChecker)
   */
  @FunctionalInterface
  public interface PhysicalColumnChecker
  {
    boolean check(String projectionName, String columnName);
  }

  private static final class RewrittenAggDimFilter implements DimFilter
  {
    private final DimFilter originalFilter;
    private final Filter rewrittenFilter;

    private RewrittenAggDimFilter(DimFilter originalFilter, Filter rewrittenFilter)
    {
      this.originalFilter = originalFilter;
      this.rewrittenFilter = rewrittenFilter;
    }

    @Override
    public DimFilter optimize(boolean mayIncludeUnknown)
    {
      return this;
    }

    @Override
    public Filter toOptimizedFilter(boolean mayIncludeUnknown)
    {
      return rewrittenFilter;
    }

    @Override
    public Filter toFilter()
    {
      return rewrittenFilter;
    }

    @Nullable
    @Override
    public RangeSet<String> getDimensionRangeSet(String dimension)
    {
      return null;
    }

    @Override
    public Set<String> getRequiredColumns()
    {
      return rewrittenFilter.getRequiredColumns();
    }

    @Nullable
    @Override
    public byte[] getCacheKey()
    {
      return originalFilter.getCacheKey();
    }
  }

  private Projections()
  {
    // no instantiation
  }
}
