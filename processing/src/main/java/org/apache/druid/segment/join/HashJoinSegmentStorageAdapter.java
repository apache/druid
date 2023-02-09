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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinFilterSplit;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class HashJoinSegmentStorageAdapter implements StorageAdapter
{
  private final StorageAdapter baseAdapter;

  @Nullable
  private final Filter baseFilter;
  private final List<JoinableClause> clauses;
  private final JoinFilterPreAnalysis joinFilterPreAnalysis;

  /**
   * @param baseAdapter           A StorageAdapter for the left-hand side base segment
   * @param clauses               The right-hand side clauses. The caller is responsible for ensuring that there are no
   *                              duplicate prefixes or prefixes that shadow each other across the clauses
   * @param joinFilterPreAnalysis Pre-analysis for the query we expect to run on this storage adapter
   */
  HashJoinSegmentStorageAdapter(
      final StorageAdapter baseAdapter,
      final List<JoinableClause> clauses,
      final JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    this(baseAdapter, null, clauses, joinFilterPreAnalysis);
  }

  /**
   * @param baseAdapter           A StorageAdapter for the left-hand side base segment
   * @param baseFilter            A filter for the left-hand side base segment
   * @param clauses               The right-hand side clauses. The caller is responsible for ensuring that there are no
   *                              duplicate prefixes or prefixes that shadow each other across the clauses
   * @param joinFilterPreAnalysis Pre-analysis for the query we expect to run on this storage adapter
   */
  HashJoinSegmentStorageAdapter(
      final StorageAdapter baseAdapter,
      @Nullable final Filter baseFilter,
      final List<JoinableClause> clauses,
      final JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    this.baseAdapter = baseAdapter;
    this.baseFilter = baseFilter;
    this.clauses = clauses;
    this.joinFilterPreAnalysis = joinFilterPreAnalysis;
  }

  @Override
  public Interval getInterval()
  {
    return baseAdapter.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    // Use a Set since we may encounter duplicates, if a field from a Joinable shadows one of the base fields.
    final LinkedHashSet<String> availableDimensions = new LinkedHashSet<>();

    baseAdapter.getAvailableDimensions().forEach(availableDimensions::add);

    for (JoinableClause clause : clauses) {
      availableDimensions.addAll(clause.getAvailableColumnsPrefixed());
    }

    return new ListIndexed<>(Lists.newArrayList(availableDimensions));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return baseAdapter.getAvailableMetrics();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    final Optional<JoinableClause> maybeClause = getClauseForColumn(column);

    if (maybeClause.isPresent()) {
      final JoinableClause clause = maybeClause.get();
      return clause.getJoinable().getCardinality(clause.unprefix(column));
    } else {
      return baseAdapter.getDimensionCardinality(column);
    }
  }

  @Override
  public DateTime getMinTime()
  {
    return baseAdapter.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return baseAdapter.getMaxTime();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    if (isBaseColumn(column)) {
      return baseAdapter.getMinValue(column);
    } else {
      return null;
    }
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    if (isBaseColumn(column)) {
      return baseAdapter.getMaxValue(column);
    } else {
      return null;
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final Optional<JoinableClause> maybeClause = getClauseForColumn(column);

    if (maybeClause.isPresent()) {
      final JoinableClause clause = maybeClause.get();
      return clause.getJoinable().getColumnCapabilities(clause.unprefix(column));
    } else {
      return baseAdapter.getColumnCapabilities(column);
    }
  }

  @Override
  public int getNumRows()
  {
    // Cannot determine the number of rows ahead of time for a join segment (rows may be added or removed based
    // on the join condition). At the time of this writing, this method is only used by the 'segmentMetadata' query,
    // which isn't meant to support join segments anyway.
    throw new UnsupportedOperationException("Cannot retrieve number of rows from join segment");
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return baseAdapter.getMaxIngestedEventTime();
  }

  @Override
  public Metadata getMetadata()
  {
    // Cannot get meaningful Metadata for this segment, since it isn't real. At the time of this writing, this method
    // is only used by the 'segmentMetadata' query, which isn't meant to support join segments anyway.
    throw new UnsupportedOperationException("Cannot retrieve metadata from join segment");
  }

  @Override
  public boolean hasBuiltInFilters()
  {
    // if the baseFilter is not null, then rows from underlying storage adapter can be potentially filtered.
    // otherwise, a filtering inner join can also filter rows.
    return baseFilter != null || clauses.stream().anyMatch(
        clause -> clause.getJoinType() == JoinType.INNER && !clause.getCondition().isAlwaysTrue()
    );
  }

  @Override
  public boolean canVectorize(@Nullable Filter filter, VirtualColumns virtualColumns, boolean descending)
  {
    // HashJoinEngine isn't vectorized yet.
    // However, we can still vectorize if there are no clauses, since that means all we need to do is apply
    // a base filter. That's easy enough!
    return clauses.isEmpty() && baseAdapter.canVectorize(baseFilterAnd(filter), virtualColumns, descending);
  }

  @Nullable
  @Override
  public VectorCursor makeVectorCursor(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      boolean descending,
      int vectorSize,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    if (!canVectorize(filter, virtualColumns, descending)) {
      throw new ISE("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor'.");
    }

    // Should have been checked by canVectorize.
    assert clauses.isEmpty();

    return baseAdapter.makeVectorCursor(
        baseFilterAnd(filter),
        interval,
        virtualColumns,
        descending,
        vectorSize,
        queryMetrics
    );
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable final Filter filter,
      @Nonnull final Interval interval,
      @Nonnull final VirtualColumns virtualColumns,
      @Nonnull final Granularity gran,
      final boolean descending,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    final Filter combinedFilter = baseFilterAnd(filter);

    if (clauses.isEmpty()) {
      return baseAdapter.makeCursors(
          combinedFilter,
          interval,
          virtualColumns,
          gran,
          descending,
          queryMetrics
      );
    }

    // Filter pre-analysis key implied by the call to "makeCursors". We need to sanity-check that it matches
    // the actual pre-analysis that was done. Note: we can't infer a rewrite config from the "makeCursors" call (it
    // requires access to the query context) so we'll need to skip sanity-checking it, by re-using the one present
    // in the cached key.)
    final JoinFilterPreAnalysisKey keyIn =
        new JoinFilterPreAnalysisKey(
            joinFilterPreAnalysis.getKey().getRewriteConfig(),
            clauses,
            virtualColumns,
            combinedFilter
        );

    final JoinFilterPreAnalysisKey keyCached = joinFilterPreAnalysis.getKey();

    if (!keyIn.equals(keyCached)) {
      // It is a bug if this happens. The implied key and the cached key should always match.
      throw new ISE("Pre-analysis mismatch, cannot execute query");
    }

    final List<VirtualColumn> preJoinVirtualColumns = new ArrayList<>();
    final List<VirtualColumn> postJoinVirtualColumns = new ArrayList<>();

    determineBaseColumnsWithPreAndPostJoinVirtualColumns(
        virtualColumns,
        preJoinVirtualColumns,
        postJoinVirtualColumns
    );

    // We merge the filter on base table specified by the user and filter on the base table that is pushed from
    // the join
    JoinFilterSplit joinFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis, baseFilter);
    preJoinVirtualColumns.addAll(joinFilterSplit.getPushDownVirtualColumns());


    final Sequence<Cursor> baseCursorSequence = baseAdapter.makeCursors(
        joinFilterSplit.getBaseTableFilter().isPresent() ? joinFilterSplit.getBaseTableFilter().get() : null,
        interval,
        VirtualColumns.create(preJoinVirtualColumns),
        gran,
        descending,
        queryMetrics
    );

    Closer joinablesCloser = Closer.create();
    return Sequences.<Cursor, Cursor>map(
        baseCursorSequence,
        cursor -> {
          assert cursor != null;
          Cursor retVal = cursor;

          for (JoinableClause clause : clauses) {
            retVal = HashJoinEngine.makeJoinCursor(retVal, clause, descending, joinablesCloser);
          }

          return PostJoinCursor.wrap(
              retVal,
              VirtualColumns.create(postJoinVirtualColumns),
              joinFilterSplit.getJoinTableFilter().orElse(null)
          );
        }
    ).withBaggage(joinablesCloser);
  }

  /**
   * Returns whether "column" will be selected from "baseAdapter". This is true if it is not shadowed by any joinables
   * (i.e. if it does not start with any of their prefixes).
   */
  public boolean isBaseColumn(final String column)
  {
    return !getClauseForColumn(column).isPresent();
  }

  /**
   * Return a String set containing the name of columns that belong to the base table (including any pre-join virtual
   * columns as well).
   *
   * Additionally, if the preJoinVirtualColumns and/or postJoinVirtualColumns arguments are provided, this method
   * will add each VirtualColumn in the provided virtualColumns to either preJoinVirtualColumns or
   * postJoinVirtualColumns based on whether the virtual column is pre-join or post-join.
   *
   * @param virtualColumns         List of virtual columns from the query
   * @param preJoinVirtualColumns  If provided, virtual columns determined to be pre-join will be added to this list
   * @param postJoinVirtualColumns If provided, virtual columns determined to be post-join will be added to this list
   *
   * @return The set of base column names, including any pre-join virtual columns.
   */
  public Set<String> determineBaseColumnsWithPreAndPostJoinVirtualColumns(
      VirtualColumns virtualColumns,
      @Nullable List<VirtualColumn> preJoinVirtualColumns,
      @Nullable List<VirtualColumn> postJoinVirtualColumns
  )
  {
    final Set<String> baseColumns = new HashSet<>(baseAdapter.getRowSignature().getColumnNames());

    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      // Virtual columns cannot depend on each other, so we don't need to check transitive dependencies.
      if (baseColumns.containsAll(virtualColumn.requiredColumns())) {
        // Since pre-join virtual columns can be computed using only base columns, we include them in the
        // base column set.
        baseColumns.add(virtualColumn.getOutputName());
        if (preJoinVirtualColumns != null) {
          preJoinVirtualColumns.add(virtualColumn);
        }
      } else {
        if (postJoinVirtualColumns != null) {
          postJoinVirtualColumns.add(virtualColumn);
        }
      }
    }

    return baseColumns;
  }

  /**
   * Returns the JoinableClause corresponding to a particular column, based on the clauses' prefixes.
   *
   * @param column column name
   *
   * @return the clause, or absent if the column does not correspond to any clause
   */
  private Optional<JoinableClause> getClauseForColumn(final String column)
  {
    // Check clauses in reverse, since "makeCursors" creates the cursor in such a way that the last clause
    // gets first dibs to claim a column.
    return Lists.reverse(clauses)
                .stream()
                .filter(clause -> clause.includesColumn(column))
                .findFirst();
  }

  @Nullable
  private Filter baseFilterAnd(@Nullable final Filter other)
  {
    return Filters.maybeAnd(Arrays.asList(baseFilter, other)).orElse(null);
  }
}
