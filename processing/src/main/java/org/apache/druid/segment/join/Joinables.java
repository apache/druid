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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinableClauses;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A wrapper class over {@link JoinableFactory} for working with {@link Joinable} related classes.
 */
public class Joinables
{
  private static final Comparator<String> DESCENDING_LENGTH_STRING_COMPARATOR = (s1, s2) ->
      Integer.compare(s2.length(), s1.length());

  private static final byte JOIN_OPERATION = 0x1;
  private static final Logger log = new Logger(Joinables.class);

  private final JoinableFactory joinableFactory;

  public Joinables(final JoinableFactory joinableFactory)
  {
    this.joinableFactory = Preconditions.checkNotNull(joinableFactory, "joinableFactory");
  }

  public JoinableFactory getJoinableFactory()
  {
    return this.joinableFactory;
  }

  /**
   * Creates a Function that maps base segments to {@link HashJoinSegment} if needed (i.e. if the number of join
   * clauses is > 0). If mapping is not needed, this method will return {@link Function#identity()}.
   *
   * @param clauses            Pre-joinable clauses
   * @param cpuTimeAccumulator An accumulator that we will add CPU nanos to; this is part of the function to encourage
   *                           callers to remember to track metrics on CPU time required for creation of Joinables
   * @param query              The query that will be run on the mapped segments. Usually this should be
   *                           {@code analysis.getBaseQuery().orElse(query)}, where "analysis" is a
   *                           {@link DataSourceAnalysis} and "query" is the original
   *                           query from the end user.
   */
  public Function<SegmentReference, SegmentReference> createSegmentMapFn(
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
            final JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
                new JoinFilterPreAnalysisKey(
                    JoinFilterRewriteConfig.forQuery(query),
                    joinableClauses.getJoinableClauses(),
                    query.getVirtualColumns(),
                    Filters.toFilter(query.getFilter())
                )
            );

            return baseSegment ->
                new HashJoinSegment(
                    baseSegment,
                    joinableClauses.getJoinableClauses(),
                    joinFilterPreAnalysis
                );
          }
        }
    );
  }

  /**
   * Compute a cache key prefix for data sources that participate in the RHS of a join. This key prefix
   * can be used in segment level cache or result level cache. The function can return following wrapped in an
   * Optional
   *  - Empty byte array - If there is no join datasource involved
   *  - Non-empty byte array - If there is join datasource involved and caching is possible. The result includes
   *  join condition expression, join type and cache key returned by joinable factory for each {@link PreJoinableClause}
   *  - NULL - There is a join but caching is not possible. It may happen if one of the participating datasource
   *  in the JOIN is not cacheable.
   *
   * @param dataSourceAnalysis
   * @return
   */
  public Optional<byte[]> computeJoinDataSourceCacheKey(
      final DataSourceAnalysis dataSourceAnalysis
  )
  {

    final List<PreJoinableClause> clauses = dataSourceAnalysis.getPreJoinableClauses();
    if (clauses.isEmpty()) {
      return Optional.of(StringUtils.EMPTY_BYTES);
    }

    final CacheKeyBuilder keyBuilder;
    keyBuilder = new CacheKeyBuilder(JOIN_OPERATION);
    for (PreJoinableClause clause : clauses) {
      if (!clause.getCondition().canHashJoin()) {
        log.debug("skipping caching for join since [%s] does not support hash-join", clause.getCondition());
        return Optional.empty();
      }
      Optional<byte[]> bytes = joinableFactory.computeJoinCacheKey(clause.getDataSource());
      if (!bytes.isPresent()) {
        // Encountered a data source which didn't support cache yet
        log.debug("skipping caching for join since [%s] does not support caching", clause.getDataSource());
        return Optional.empty();
      }
      keyBuilder.appendByteArray(bytes.get());
      keyBuilder.appendString(clause.getPrefix());    //TODO - prefix shouldn't be required IMO
      keyBuilder.appendString(clause.getCondition().getOriginalExpression());
      keyBuilder.appendString(clause.getJoinType().name());
    }
    return Optional.ofNullable(keyBuilder.build());
  }

  /**
   * Checks that "prefix" is a valid prefix for a join clause (see {@link JoinableClause#getPrefix()}) and, if so,
   * returns it. Otherwise, throws an exception.
   */
  public static String validatePrefix(@Nullable final String prefix)
  {
    if (prefix == null || prefix.isEmpty()) {
      throw new IAE("Join clause cannot have null or empty prefix");
    } else if (isPrefixedBy(ColumnHolder.TIME_COLUMN_NAME, prefix) || ColumnHolder.TIME_COLUMN_NAME.equals(prefix)) {
      throw new IAE(
          "Join clause cannot have prefix[%s], since it would shadow %s",
          prefix,
          ColumnHolder.TIME_COLUMN_NAME
      );
    } else {
      return prefix;
    }
  }

  public static boolean isPrefixedBy(final String columnName, final String prefix)
  {
    return columnName.length() > prefix.length() && columnName.startsWith(prefix);
  }

  /**
   * Check if any prefixes in the provided list duplicate or shadow each other.
   *
   * @param prefixes A mutable list containing the prefixes to check. This list will be sorted by descending
   *                 string length.
   */
  public static void checkPrefixesForDuplicatesAndShadowing(
      final List<String> prefixes
  )
  {
    // this is a naive approach that assumes we'll typically handle only a small number of prefixes
    prefixes.sort(DESCENDING_LENGTH_STRING_COMPARATOR);
    for (int i = 0; i < prefixes.size(); i++) {
      String prefix = prefixes.get(i);
      for (int k = i + 1; k < prefixes.size(); k++) {
        String otherPrefix = prefixes.get(k);
        if (prefix.equals(otherPrefix)) {
          throw new IAE("Detected duplicate prefix in join clauses: [%s]", prefix);
        }
        if (isPrefixedBy(prefix, otherPrefix)) {
          throw new IAE("Detected conflicting prefixes in join clauses: [%s, %s]", prefix, otherPrefix);
        }
      }
    }
  }
}
