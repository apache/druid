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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.AsyncCursorHolder;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinFilterSplit;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class HashJoinSegmentCursorFactory implements CursorFactory
{
  private final CursorFactory baseCursorFactory;
  @Nullable
  private final Filter baseFilter;
  private final List<JoinableClause> clauses;
  private final JoinFilterPreAnalysis joinFilterPreAnalysis;

  public HashJoinSegmentCursorFactory(
      CursorFactory baseCursorFactory,
      @Nullable Filter baseFilter,
      List<JoinableClause> clauses,
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    this.baseCursorFactory = baseCursorFactory;
    this.baseFilter = baseFilter;
    this.clauses = clauses;
    this.joinFilterPreAnalysis = joinFilterPreAnalysis;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final Filter combinedFilter = baseFilterAnd(spec.getFilter());
    final Set<String> physicalColumns = computeBasePhysicalColumns(spec, combinedFilter);

    if (clauses.isEmpty()) {
      // if there are no clauses, we can just use the base cursor directly if we apply the combined filter
      return baseCursorFactory.makeCursorHolder(noClausesBaseSpec(spec, combinedFilter, physicalColumns));
    }

    // else we need to wipe out the grouping, aggregations, and ordering
    final Closer joinablesCloser = Closer.create();
    final JoinCursorPlan plan = planJoinCursor(spec, combinedFilter, physicalColumns);
    final CursorHolder baseCursorHolder = joinablesCloser.register(
        baseCursorFactory.makeCursorHolder(plan.baseBuildSpec)
    );
    return joinCursorHolder(plan, joinablesCloser, baseCursorHolder);
  }

  @Override
  public AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
  {
    final Filter combinedFilter = baseFilterAnd(spec.getFilter());
    final Set<String> physicalColumns = computeBasePhysicalColumns(spec, combinedFilter);

    if (clauses.isEmpty()) {
      return baseCursorFactory.makeCursorHolderAsync(noClausesBaseSpec(spec, combinedFilter, physicalColumns));
    }

    final Closer joinablesCloser = Closer.create();
    // Join filter analysis + base-spec computation are CPU-only; do them synchronously to learn the base spec.
    final JoinCursorPlan plan = planJoinCursor(spec, combinedFilter, physicalColumns);

    // Build the left/base holder asynchronously (a partial base segment downloads its required columns here); the
    // join's build-side joinables are already-resident in-memory tables, so the base is the only async piece. Once it's
    // ready, wrap it with the join cursors. Closing the returned holder before it's ready cancels the base load.
    final AsyncCursorHolder baseAsync = baseCursorFactory.makeCursorHolderAsync(plan.baseBuildSpec);
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(baseAsync::close);
    baseAsync.addReadyCallback(() -> {
      final CursorHolder joinHolder;
      try {
        // release() transfers ownership of the base holder to us (and surfaces a base-load failure as its cause); the
        // join holder now owns closing it via joinablesCloser. The wrap below can't throw, so the catch only fires on
        // a base failure or a cancel race (baseAsync already closed), in both cases joinablesCloser is still empty.
        final CursorHolder baseHolder = baseAsync.release();
        joinablesCloser.register(baseHolder);
        joinHolder = joinCursorHolder(plan, joinablesCloser, baseHolder);
      }
      catch (Throwable t) {
        asyncHolder.setException(t);
        return;
      }
      if (!asyncHolder.set(joinHolder)) {
        // awaiter closed the wrapper while we were producing the holder; close it so the base holder doesn't leak
        joinHolder.close();
      }
    });
    return asyncHolder;
  }

  /**
   * Physical columns to pass to the base cursor: a copy of the spec's physical columns plus any columns required by
   * the combined filter (null when the spec didn't declare physical columns, meaning "all"). Shared by the no-clauses
   * and join paths.
   */
  @Nullable
  private static Set<String> computeBasePhysicalColumns(CursorBuildSpec spec, @Nullable Filter combinedFilter)
  {
    final Set<String> physicalColumns = spec.getPhysicalColumns() != null
                                        ? new HashSet<>(spec.getPhysicalColumns())
                                        : null;
    if (physicalColumns != null && combinedFilter != null) {
      for (String column : combinedFilter.getRequiredColumns()) {
        if (!spec.getVirtualColumns().exists(column)) {
          physicalColumns.add(column);
        }
      }
    }
    return physicalColumns;
  }

  /**
   * Base cursor spec for the no-clauses case: the original spec with the combined filter and base physical columns.
   */
  private static CursorBuildSpec noClausesBaseSpec(
      CursorBuildSpec spec,
      @Nullable Filter combinedFilter,
      @Nullable Set<String> physicalColumns
  )
  {
    return CursorBuildSpec.builder(spec)
                          .setFilter(combinedFilter)
                          .setPhysicalColumns(physicalColumns)
                          .build();
  }

  /**
   * Run the (CPU-only) join filter pre-analysis and compute the base-table cursor build spec. Returns the analysis
   * results needed at cursor-construction time ({@link JoinCursorPlan}). {@code physicalColumns} is mutated in place
   * (it accumulates base-filter, pre-join virtual-column, and clause-condition columns, minus the join prefixes).
   */
  private JoinCursorPlan planJoinCursor(
      CursorBuildSpec spec,
      @Nullable Filter combinedFilter,
      @Nullable Set<String> physicalColumns
  )
  {
    // Filter pre-analysis key implied by the call to "makeCursorHolder". We need to sanity-check that it matches
    // the actual pre-analysis that was done. Note: we could now infer a rewrite config from the "makeCursorHolder"
    // call (it requires access to the query context which we now have access to) but this code hasn't been updated to
    // sanity-check it, so currently we are still skipping it by re-using the one present in the cached key.
    final JoinFilterPreAnalysisKey keyIn =
        new JoinFilterPreAnalysisKey(
            joinFilterPreAnalysis.getKey().getRewriteConfig(),
            clauses,
            spec.getVirtualColumns(),
            combinedFilter
        );

    final JoinFilterPreAnalysisKey keyCached = joinFilterPreAnalysis.getKey();
    final JoinFilterPreAnalysis actualPreAnalysis;
    if (keyIn.equals(keyCached)) {
      // Common case: key used during filter pre-analysis (keyCached) matches key implied by makeCursorHolder call
      // (keyIn).
      actualPreAnalysis = joinFilterPreAnalysis;
    } else {
      // Less common case: key differs. Re-analyze the filter. This case can happen when an unnest datasource is
      // layered on top of a join datasource.
      actualPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(keyIn);
    }

    final JoinFilterSplit joinFilterSplit = JoinFilterAnalyzer.splitFilter(actualPreAnalysis, baseFilter);

    // start with a full scan clipped to interval
    final CursorBuildSpec.CursorBuildSpecBuilder cursorBuildSpecBuilder =
        CursorBuildSpec.builder()
                       .setInterval(spec.getInterval())
                       .setQueryContext(spec.getQueryContext())
                       .setQueryMetrics(spec.getQueryMetrics());

    // retain time ordering if preferred
    Order timeOrder = Cursors.getTimeOrdering(spec.getPreferredOrdering());
    if (timeOrder == Order.DESCENDING) {
      cursorBuildSpecBuilder.setPreferredOrdering(Cursors.descendingTimeOrder());
    } else if (timeOrder == Order.ASCENDING) {
      cursorBuildSpecBuilder.setPreferredOrdering(Cursors.ascendingTimeOrder());
    }

    // add pushdown filters if present
    if (joinFilterSplit.getBaseTableFilter().isPresent()) {
      cursorBuildSpecBuilder.setFilter(joinFilterSplit.getBaseTableFilter().get());
    }
    final VirtualColumns preJoinVirtualColumns = VirtualColumns.fromIterable(
        Iterables.concat(
            Sets.difference(
                ImmutableSet.copyOf(spec.getVirtualColumns().getVirtualColumns()),
                joinFilterPreAnalysis.getPostJoinVirtualColumns()
            ),
            joinFilterSplit.getPushDownVirtualColumns()
        )
    );
    cursorBuildSpecBuilder.setVirtualColumns(preJoinVirtualColumns);

    // add all base table physical columns if they were originally set
    if (physicalColumns != null) {
      if (joinFilterSplit.getBaseTableFilter().isPresent()) {
        for (String column : joinFilterSplit.getBaseTableFilter().get().getRequiredColumns()) {
          if (!spec.getVirtualColumns().exists(column) && !preJoinVirtualColumns.exists(column)) {
            physicalColumns.add(column);
          }
        }
      }
      for (VirtualColumn virtualColumn : preJoinVirtualColumns.getVirtualColumns()) {
        for (String column : virtualColumn.requiredColumns()) {
          if (!spec.getVirtualColumns().exists(column) && !preJoinVirtualColumns.exists(column)) {
            physicalColumns.add(column);
          }
        }
      }
      final Set<String> prefixes = new HashSet<>();
      for (JoinableClause clause : clauses) {
        prefixes.add(clause.getPrefix());
        physicalColumns.addAll(clause.getCondition().getRequiredColumns());
      }
      for (String prefix : prefixes) {
        physicalColumns.removeIf(x -> JoinPrefixUtils.isPrefixedBy(x, prefix));
      }
      cursorBuildSpecBuilder.setPhysicalColumns(physicalColumns);
    }

    return new JoinCursorPlan(actualPreAnalysis, joinFilterSplit, cursorBuildSpecBuilder.build());
  }

  /**
   * Build the join {@link CursorHolder} on top of a base-table holder. {@code baseCursorHolder} must already be
   * registered with {@code joinablesCloser} (the sync path registers it when built; the async path registers the
   * released holder); closing the returned holder closes {@code joinablesCloser} (the base holder + per-cursor join
   * matchers created in {@link CursorHolder#asCursor}).
   */
  private CursorHolder joinCursorHolder(
      JoinCursorPlan plan,
      Closer joinablesCloser,
      CursorHolder baseCursorHolder
  )
  {
    return new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        final Cursor baseCursor = baseCursorHolder.asCursor();

        if (baseCursor == null) {
          return null;
        }

        Cursor retVal = baseCursor;

        for (JoinableClause clause : clauses) {
          retVal = HashJoinEngine.makeJoinCursor(retVal, clause, joinablesCloser);
        }

        return PostJoinCursor.wrap(
            retVal,
            VirtualColumns.fromIterable(plan.actualPreAnalysis.getPostJoinVirtualColumns()),
            plan.joinFilterSplit.getJoinTableFilter().orElse(null)
        );
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        return computeOrdering(baseCursorHolder.getOrdering());
      }

      @Override
      public void close()
      {
        CloseableUtils.closeAndWrapExceptions(joinablesCloser);
      }
    };
  }

  @Override
  public RowSignature getRowSignature()
  {
    // Use a Set since we may encounter duplicates, if a field from a Joinable shadows one of the base fields.
    final RowSignature baseSignature = baseCursorFactory.getRowSignature();

    final LinkedHashSet<String> columns = new LinkedHashSet<>(baseSignature.getColumnNames());
    for (final JoinableClause clause : clauses) {
      columns.addAll(clause.getAvailableColumnsPrefixed());
    }

    final RowSignature.Builder builder = RowSignature.builder();
    // Check clauses in reverse, since "makeCursorHolder" creates the cursor in such a way that the last clause
    // gets first dibs to claim a column.
    LinkedHashSet<JoinableClause> reverseClauses = new LinkedHashSet<>(Lists.reverse(clauses));
    for (final String column : columns) {
      final Optional<JoinableClause> maybeClause = reverseClauses.stream()
                                                                 .filter(c -> c.includesColumn(column))
                                                                 .findFirst();
      if (maybeClause.isPresent()) {
        final JoinableClause clause = maybeClause.get();
        builder.add(
            column,
            ColumnType.fromCapabilities(clause.getJoinable().getColumnCapabilities(clause.unprefix(column)))
        );
      } else {
        builder.add(column, baseSignature.getColumnType(column).get());
      }
    }

    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final Optional<JoinableClause> maybeClause = Lists.reverse(clauses)
                                                      .stream()
                                                      .filter(x -> x.includesColumn(column))
                                                      .findFirst();

    if (maybeClause.isPresent()) {
      final JoinableClause clause = maybeClause.get();
      return clause.getJoinable().getColumnCapabilities(clause.unprefix(column));
    } else {
      return baseCursorFactory.getColumnCapabilities(column);
    }
  }

  @Nullable
  private Filter baseFilterAnd(@Nullable final Filter other)
  {
    return Filters.maybeAnd(Arrays.asList(baseFilter, other)).orElse(null);
  }

  /**
   * Computes ordering of a join {@link CursorHolder} based on the ordering of an underlying {@link CursorHolder}.
   */
  private List<OrderBy> computeOrdering(final List<OrderBy> baseOrdering)
  {
    // Sorted the same way as the base segment, unless a joined-in column shadows one of the base columns.
    int limit = 0;
    for (; limit < baseOrdering.size(); limit++) {
      if (!baseCursorFactory.getRowSignature().contains(baseOrdering.get(limit).getColumnName())) {
        break;
      }
    }

    return limit == baseOrdering.size() ? baseOrdering : baseOrdering.subList(0, limit);
  }

  /**
   * Outputs of {@link #planJoinCursor}: the (possibly re-computed) join filter pre-analysis and split needed when the
   * join cursor is actually constructed, plus the base-table cursor build spec used to open the left-side holder.
   */
  private static final class JoinCursorPlan
  {
    /**
     * Typically the same as {@link HashJoinSegmentCursorFactory#joinFilterPreAnalysis}, but may differ when an unnest
     * datasource is layered on top of a join datasource.
     */
    private final JoinFilterPreAnalysis actualPreAnalysis;

    /**
     * Result of {@link JoinFilterAnalyzer#splitFilter} on {@link #actualPreAnalysis} and
     * {@link HashJoinSegmentCursorFactory#baseFilter}.
     */
    private final JoinFilterSplit joinFilterSplit;

    /**
     * Build spec for the left-side {@link HashJoinSegmentCursorFactory#baseCursorFactory} holder.
     */
    private final CursorBuildSpec baseBuildSpec;

    private JoinCursorPlan(
        JoinFilterPreAnalysis actualPreAnalysis,
        JoinFilterSplit joinFilterSplit,
        CursorBuildSpec baseBuildSpec
    )
    {
      this.actualPreAnalysis = actualPreAnalysis;
      this.joinFilterSplit = joinFilterSplit;
      this.baseBuildSpec = baseBuildSpec;
    }
  }
}
