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

package org.apache.druid.segment.join.filter.rewrite;

import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinableClauses;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A JoinFilterPreAnalysisGroup holds all of the JoinFilterPreAnalysis objects for a given query and
 * also stores the per-query parameters that control the filter rewrite operations (from the query context).
 *
 * The analyses map is keyed by (Filter, JoinableClause list, VirtualColumns): each Filter in the map belongs to a
 * separate level of query (e.g. outer query, subquery level 1, etc.)
 *
 * If there is only a single Filter, then this class does not use the analyses map, instead of using a single reference
 * for efficiency reasons.
 */
public class JoinFilterPreAnalysisGroup
{
  private final JoinFilterRewriteConfig joinFilterRewriteConfig;
  private final Map<JoinFilterPreAnalysisGroupKey, JoinFilterPreAnalysis> analyses;
  private final boolean isSingleLevelMode;

  /**
   * Hashing and comparing filters can be expensive for large filters, so if we're only dealing with
   * a single level of join query, then we can be more efficient by using a single reference instead of a map.
   */
  private JoinFilterPreAnalysis preAnalysisForSingleLevelMode;

  public JoinFilterPreAnalysisGroup(
      JoinFilterRewriteConfig joinFilterRewriteConfig,
      boolean isSingleLevelMode
  )
  {
    this.joinFilterRewriteConfig = joinFilterRewriteConfig;
    this.analyses = new HashMap<>();
    this.isSingleLevelMode = isSingleLevelMode;
  }

  public boolean isSingleLevelMode()
  {
    return isSingleLevelMode;
  }

  public JoinFilterPreAnalysis computeJoinFilterPreAnalysisIfAbsent(
      Filter filter,
      List<JoinableClause> clauses,
      VirtualColumns virtualColumns
  )
  {
    if (isSingleLevelMode) {
      preAnalysisForSingleLevelMode = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
          JoinableClauses.fromList(clauses),
          virtualColumns,
          filter,
          joinFilterRewriteConfig
      );
      return preAnalysisForSingleLevelMode;
    }

    JoinFilterPreAnalysisGroupKey key = new JoinFilterPreAnalysisGroupKey(filter, clauses, virtualColumns);
    return analyses.computeIfAbsent(
        key,
        (groupKey) -> {
          return JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
              JoinableClauses.fromList(clauses),
              virtualColumns,
              filter,
              joinFilterRewriteConfig
          );
        }
    );
  }

  public JoinFilterPreAnalysis getAnalysis(
      Filter filter,
      List<JoinableClause> clauses,
      VirtualColumns virtualColumns
  )
  {
    JoinFilterPreAnalysisGroupKey key = new JoinFilterPreAnalysisGroupKey(filter, clauses, virtualColumns);
    return analyses.get(key);
  }

  public JoinFilterPreAnalysis getPreAnalysisForSingleLevelMode()
  {
    return preAnalysisForSingleLevelMode;
  }

  public static class JoinFilterPreAnalysisGroupKey
  {
    private final Filter filter;
    private final List<JoinableClause> clauses;
    private final VirtualColumns virtualColumns;

    public JoinFilterPreAnalysisGroupKey(
        Filter filter,
        List<JoinableClause> clauses,
        VirtualColumns virtualColumns
    )
    {
      this.filter = filter;
      this.clauses = clauses;
      this.virtualColumns = virtualColumns;
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
      JoinFilterPreAnalysisGroupKey that = (JoinFilterPreAnalysisGroupKey) o;
      return Objects.equals(filter, that.filter) &&
             Objects.equals(clauses, that.clauses) &&
             Objects.equals(virtualColumns, that.virtualColumns);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(filter, clauses, virtualColumns);
    }
  }
}
