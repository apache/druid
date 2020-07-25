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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.Query;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Utility methods for working with {@link Joinable} related classes.
 */
public class Joinables
{
  private static final Comparator<String> DESCENDING_LENGTH_STRING_COMPARATOR = (s1, s2) ->
      Integer.compare(s2.length(), s1.length());

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
   * Creates a Function that maps base segments to {@link HashJoinSegment} if needed (i.e. if the number of join
   * clauses is > 0). If mapping is not needed, this method will return {@link Function#identity()}.
   *
   * @param clauses            Pre-joinable clauses
   * @param joinableFactory    Factory for joinables
   * @param cpuTimeAccumulator An accumulator that we will add CPU nanos to; this is part of the function to encourage
   *                           callers to remember to track metrics on CPU time required for creation of Joinables
   * @param query              The query that will be run on the mapped segments. Usually this should be
   *                           {@code analysis.getBaseQuery().orElse(query)}, where "analysis" is a
   *                           {@link org.apache.druid.query.planning.DataSourceAnalysis} and "query" is the original
   *                           query from the end user.
   */
  public static Function<SegmentReference, SegmentReference> createSegmentMapFn(
      final List<PreJoinableClause> clauses,
      final JoinableFactory joinableFactory,
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
