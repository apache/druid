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
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorMaker;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.StorageAdapter;
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
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

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
  public CursorMaker asCursorMaker(CursorBuildSpec spec)
  {
    final CursorBuildSpec.CursorBuildSpecBuilder cursorBuildSpecBuilder =
        CursorBuildSpec.builder()
                       .setInterval(spec.getInterval())
                       .setGranularity(spec.getGranularity())
                       .isDescending(spec.isDescending())
                       .setQueryMetrics(spec.getQueryMetrics());

    final Filter combinedFilter = baseFilterAnd(spec.getFilter());


    if (clauses.isEmpty()) {
      // if there are no clauses, we can just use the base cursor directly if we apply the combined filter
      final CursorBuildSpec newSpec = cursorBuildSpecBuilder.setFilter(combinedFilter)
                                                            .setVirtualColumns(spec.getVirtualColumns())
                                                            .build();
      return baseAdapter.asCursorMaker(newSpec);
    }

    return new CursorMaker()
    {
      final Closer joinablesCloser = Closer.create();

      @Override
      public Cursor makeCursor()
      {
        // Filter pre-analysis key implied by the call to "makeCursors". We need to sanity-check that it matches
        // the actual pre-analysis that was done. Note: we can't infer a rewrite config from the "makeCursors" call (it
        // requires access to the query context) so we'll need to skip sanity-checking it, by re-using the one present
        // in the cached key.)
        final JoinFilterPreAnalysisKey keyIn =
            new JoinFilterPreAnalysisKey(
                joinFilterPreAnalysis.getKey().getRewriteConfig(),
                clauses,
                spec.getVirtualColumns(),
                combinedFilter
            );

        final JoinFilterPreAnalysisKey keyCached = joinFilterPreAnalysis.getKey();
        final JoinFilterPreAnalysis preAnalysis;
        if (keyIn.equals(keyCached)) {
          // Common case: key used during filter pre-analysis (keyCached) matches key implied by makeCursors call (keyIn).
          preAnalysis = joinFilterPreAnalysis;
        } else {
          // Less common case: key differs. Re-analyze the filter. This case can happen when an unnest datasource is
          // layered on top of a join datasource.
          preAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(keyIn);
        }

        final JoinFilterSplit joinFilterSplit = JoinFilterAnalyzer.splitFilter(
            preAnalysis,
            baseFilter
        );


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

        final Cursor baseCursor = joinablesCloser.register(baseAdapter.asCursorMaker(cursorBuildSpecBuilder.build()))
                                                 .makeCursor();

        if (baseCursor == null) {
          return null;
        }

        Cursor retVal = baseCursor;

        for (JoinableClause clause : clauses) {
          retVal = HashJoinEngine.makeJoinCursor(retVal, clause, spec.isDescending(), joinablesCloser);
        }

        return PostJoinCursor.wrap(
            retVal,
            VirtualColumns.fromIterable(preAnalysis.getPostJoinVirtualColumns()),
            joinFilterSplit.getJoinTableFilter().orElse(null)
        );
      }

      @Override
      public void close()
      {
        CloseableUtils.closeAndWrapExceptions(joinablesCloser);
      }

      @Override
      public boolean canVectorize()
      {
        return CursorMaker.super.canVectorize();
      }

      @Nullable
      @Override
      public VectorCursor makeVectorCursor()
      {
        return CursorMaker.super.makeVectorCursor();
      }
    };
  }

  @Override
  public boolean isFromTombstone()
  {
    return baseAdapter.isFromTombstone();
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
