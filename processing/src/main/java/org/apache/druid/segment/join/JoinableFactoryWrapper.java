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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinableClauses;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A wrapper class over {@link JoinableFactory} for working with {@link Joinable} related classes.
 */
public class JoinableFactoryWrapper
{

  private static final byte JOIN_OPERATION = 0x1;
  private static final Logger log = new Logger(JoinableFactoryWrapper.class);

  private final JoinableFactory joinableFactory;

  public JoinableFactoryWrapper(final JoinableFactory joinableFactory)
  {
    this.joinableFactory = Preconditions.checkNotNull(joinableFactory, "joinableFactory");
  }

  /**
   * Creates a Function that maps base segments to {@link HashJoinSegment} if needed (i.e. if the number of join
   * clauses is > 0). If mapping is not needed, this method will return {@link Function#identity()}.
   *
   * @param baseFilter         Filter to apply before the join takes place
   * @param clauses            Pre-joinable clauses
   * @param cpuTimeAccumulator An accumulator that we will add CPU nanos to; this is part of the function to encourage
   *                           callers to remember to track metrics on CPU time required for creation of Joinables
   * @param query              The query that will be run on the mapped segments. Usually this should be
   *                           {@code analysis.getBaseQuery().orElse(query)}, where "analysis" is a
   *                           {@link DataSourceAnalysis} and "query" is the original
   *                           query from the end user.
   */
  public Function<SegmentReference, SegmentReference> createSegmentMapFn(
      @Nullable final Filter baseFilter,
      final List<PreJoinableClause> clauses,
      final AtomicLong cpuTimeAccumulator,
      final Query<?> query
  )
  {
    // compute column correlations here and RHS correlated values
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () -> {
          if (clauses.isEmpty()) {
            return Function.identity();
          } else {
            final JoinableClauses joinableClauses = JoinableClauses.createClauses(clauses, joinableFactory);
            final JoinFilterRewriteConfig filterRewriteConfig = JoinFilterRewriteConfig.forQuery(query);

            // Pick off any join clauses that can be converted into filters.
            final Set<String> requiredColumns = query.getRequiredColumns();
            final Filter baseFilterToUse;
            final List<JoinableClause> clausesToUse;

            if (requiredColumns != null && filterRewriteConfig.isEnableRewriteJoinToFilter()) {
              final Pair<List<Filter>, List<JoinableClause>> conversionResult = convertJoinsToFilters(
                  joinableClauses.getJoinableClauses(),
                  requiredColumns,
                  Ints.checkedCast(Math.min(filterRewriteConfig.getFilterRewriteMaxSize(), Integer.MAX_VALUE))
              );

              baseFilterToUse =
                  Filters.maybeAnd(
                      Lists.newArrayList(
                          Iterables.concat(
                              Collections.singleton(baseFilter),
                              conversionResult.lhs
                          )
                      )
                  ).orElse(null);
              clausesToUse = conversionResult.rhs;
            } else {
              baseFilterToUse = baseFilter;
              clausesToUse = joinableClauses.getJoinableClauses();
            }

            // Analyze remaining join clauses to see if filters on them can be pushed down.
            final JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
                new JoinFilterPreAnalysisKey(
                    filterRewriteConfig,
                    clausesToUse,
                    query.getVirtualColumns(),
                    Filters.maybeAnd(Arrays.asList(baseFilterToUse, Filters.toFilter(query.getFilter())))
                           .orElse(null)
                )
            );

            return baseSegment ->
                new HashJoinSegment(baseSegment, baseFilterToUse, clausesToUse, joinFilterPreAnalysis);
          }
        }
    );
  }

  /**
   * Compute a cache key prefix for a join data source. This includes the data sources that participate in the RHS of a
   * join as well as any query specific constructs associated with join data source such as base table filter. This key prefix
   * can be used in segment level cache or result level cache. The function can return following wrapped in an
   * Optional
   * - Non-empty byte array - If there is join datasource involved and caching is possible. The result includes
   * join condition expression, join type and cache key returned by joinable factory for each {@link PreJoinableClause}
   * - NULL - There is a join but caching is not possible. It may happen if one of the participating datasource
   * in the JOIN is not cacheable.
   *
   * @param dataSourceAnalysis for the join datasource
   *
   * @return the optional cache key to be used as part of query cache key
   *
   * @throws {@link IAE} if this operation is called on a non-join data source
   */
  public Optional<byte[]> computeJoinDataSourceCacheKey(
      final DataSourceAnalysis dataSourceAnalysis
  )
  {
    final List<PreJoinableClause> clauses = dataSourceAnalysis.getPreJoinableClauses();
    if (clauses.isEmpty()) {
      throw new IAE("No join clauses to build the cache key for data source [%s]", dataSourceAnalysis.getDataSource());
    }

    final CacheKeyBuilder keyBuilder;
    keyBuilder = new CacheKeyBuilder(JOIN_OPERATION);
    if (dataSourceAnalysis.getJoinBaseTableFilter().isPresent()) {
      keyBuilder.appendCacheable(dataSourceAnalysis.getJoinBaseTableFilter().get());
    }
    for (PreJoinableClause clause : clauses) {
      Optional<byte[]> bytes = joinableFactory.computeJoinCacheKey(clause.getDataSource(), clause.getCondition());
      if (!bytes.isPresent()) {
        // Encountered a data source which didn't support cache yet
        log.debug("skipping caching for join since [%s] does not support caching", clause.getDataSource());
        return Optional.empty();
      }
      keyBuilder.appendByteArray(bytes.get());
      keyBuilder.appendString(clause.getCondition().getOriginalExpression());
      keyBuilder.appendString(clause.getPrefix());
      keyBuilder.appendString(clause.getJoinType().name());
    }
    return Optional.of(keyBuilder.build());
  }


  /**
   * Converts any join clauses to filters that can be converted, and returns the rest as-is.
   *
   * See {@link #convertJoinToFilter} for details on the logic.
   */
  @VisibleForTesting
  static Pair<List<Filter>, List<JoinableClause>> convertJoinsToFilters(
      final List<JoinableClause> clauses,
      final Set<String> requiredColumns,
      final int maxNumFilterValues
  )
  {
    final List<Filter> filterList = new ArrayList<>();
    final List<JoinableClause> clausesToUse = new ArrayList<>();

    // Join clauses may depend on other, earlier join clauses.
    // We track that using a Multiset, because we'll need to remove required columns one by one as we convert clauses,
    // and multiple clauses may refer to the same column.
    final Multiset<String> columnsRequiredByJoinClauses = HashMultiset.create();

    for (JoinableClause clause : clauses) {
      for (String column : clause.getCondition().getRequiredColumns()) {
        columnsRequiredByJoinClauses.add(column, 1);
      }
    }

    // Walk through the list of clauses, picking off any from the start of the list that can be converted to filters.
    boolean atStart = true;
    for (JoinableClause clause : clauses) {
      if (atStart) {
        // Remove this clause from columnsRequiredByJoinClauses. It's ok if it relies on itself.
        for (String column : clause.getCondition().getRequiredColumns()) {
          columnsRequiredByJoinClauses.remove(column, 1);
        }

        final Optional<Filter> filter =
            convertJoinToFilter(
                clause,
                Sets.union(requiredColumns, columnsRequiredByJoinClauses.elementSet()),
                maxNumFilterValues
            );

        if (filter.isPresent()) {
          filterList.add(filter.get());
        } else {
          clausesToUse.add(clause);
          atStart = false;
        }
      } else {
        clausesToUse.add(clause);
      }
    }

    // Sanity check. If this exception is ever thrown, it's a bug.
    if (filterList.size() + clausesToUse.size() != clauses.size()) {
      throw new ISE("Lost a join clause during planning");
    }

    return Pair.of(filterList, clausesToUse);
  }

  /**
   * Converts a join clause into an "in" filter if possible.
   *
   * The requirements are:
   *
   * - it must be an INNER equi-join
   * - the right-hand columns referenced by the condition must not have any duplicate values
   * - no columns from the right-hand side can appear in "requiredColumns"
   */
  @VisibleForTesting
  static Optional<Filter> convertJoinToFilter(
      final JoinableClause clause,
      final Set<String> requiredColumns,
      final int maxNumFilterValues
  )
  {
    if (clause.getJoinType() == JoinType.INNER
        && requiredColumns.stream().noneMatch(clause::includesColumn)
        && clause.getCondition().getNonEquiConditions().isEmpty()
        && clause.getCondition().getEquiConditions().size() > 0) {
      final List<Filter> filters = new ArrayList<>();
      int numValues = maxNumFilterValues;

      for (final Equality condition : clause.getCondition().getEquiConditions()) {
        final String leftColumn = condition.getLeftExpr().getBindingIfIdentifier();

        if (leftColumn == null) {
          return Optional.empty();
        }

        final Optional<Set<String>> columnValuesForFilter =
            clause.getJoinable().getNonNullColumnValuesIfAllUnique(condition.getRightColumn(), numValues);

        if (columnValuesForFilter.isPresent()) {
          numValues -= columnValuesForFilter.get().size();
          filters.add(Filters.toFilter(new InDimFilter(leftColumn, columnValuesForFilter.get())));
        } else {
          return Optional.empty();
        }
      }

      return Optional.of(Filters.and(filters));
    }

    return Optional.empty();
  }
}
