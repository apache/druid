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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A JoinFilterPreAnalysisGroup holds all of the JoinFilterPreAnalysis objects for a given query and
 * also stores the per-query parameters that control the filter rewrite operations (from the query context).
 *
 * The analyses map is keyed by (Filter, JoinableClause list, VirtualColumns): each Filter in the map belongs to a
 * separate level of query (e.g. outer query, subquery level 1, etc.)
 *
 * A concurrent hash map is used since the per-level Filter processing occurs across multiple threads.
 */
public class JoinFilterPreAnalysisGroup
{
  private final JoinFilterRewriteConfig joinFilterRewriteConfig;
  private final ConcurrentHashMap<JoinFilterPreAnalysisGroupKey, JoinFilterPreAnalysis> analyses;

  /**
   * This is an undocumented option provided as a transition tool:
   *
   * The join filter rewrites originally performed the pre-analysis phase prior to any per-segment processing,
   * analyzing only the filter in the top-level of the query.
   *
   * This did not work for nested queries (see https://github.com/apache/druid/pull/9978), so the rewrite pre-analysis
   * was moved into the cursor creation of the {@link org.apache.druid.segment.join.HashJoinSegmentStorageAdapter}.
   * This design requires synchronization across multiple segment processing threads; the old rewrite mode
   * is kept temporarily available in case issues arise with the new mode, and the user does not run queries with the
   * affected nested shape.
   */
  private JoinFilterPreAnalysis preAnalysisForOldRewriteMode;

  public JoinFilterPreAnalysisGroup(
      JoinFilterRewriteConfig joinFilterRewriteConfig
  )
  {
    this.joinFilterRewriteConfig = joinFilterRewriteConfig;
    this.analyses = new ConcurrentHashMap<>();
  }

  public JoinFilterRewriteConfig getJoinFilterRewriteConfig()
  {
    return joinFilterRewriteConfig;
  }

  public JoinFilterPreAnalysis computeJoinFilterPreAnalysisIfAbsent(
      Filter filter,
      List<JoinableClause> clauses,
      VirtualColumns virtualColumns
  )
  {
    // Some filters have potentially expensive hash codes that are lazily computed and cached.
    // We call hashCode() here in a synchronized block before we attempt to use the Filter in the analyses map,
    // to ensure that the hashCode is only computed once per Filter since the Filter interface is not thread-safe.
    synchronized (analyses) {
      if (filter != null) {
        filter.hashCode();
      }
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

  public void performAnalysisForOldRewriteMode(
      Filter filter,
      List<JoinableClause> clauses,
      VirtualColumns virtualColumns
  )
  {
    preAnalysisForOldRewriteMode = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        JoinableClauses.fromList(clauses),
        virtualColumns,
        filter,
        joinFilterRewriteConfig
    );
  }

  public JoinFilterPreAnalysis getPreAnalysisForOldRewriteMode()
  {
    return preAnalysisForOldRewriteMode;
  }

  private static class JoinFilterPreAnalysisGroupKey
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

    public Filter getFilter()
    {
      return filter;
    }

    public List<JoinableClause> getClauses()
    {
      return clauses;
    }

    public VirtualColumns getVirtualColumns()
    {
      return virtualColumns;
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
