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

package org.apache.druid.query.planning;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;

/**
 * Walks a query tree to decide whether every physical leaf datasource has an
 * ancestor query with a bounded {@link QuerySegmentSpec}, for the purpose of
 * enforcing {@code druid.sql.planner.requireTimeCondition}.
 *
 * <p>Unlike {@link ExecutionVertex#getEffectiveQuerySegmentSpec()}, which is
 * pruned at the execution-vertex boundary (see
 * {@link Query#mayCollapseQueryDataSource()}), this walker descends into every
 * {@link QueryDataSource} and every left input of a {@link JoinDataSource}.
 * {@code requireTimeCondition} asks a validation question ("is there a
 * {@code __time} filter anywhere that bounds the base scan?") that is not
 * bounded by the execution vertex, and {@code ExecutionVertex}'s segment spec
 * is load-bearing for segment selection (see {@code CachingClusteredClient},
 * {@code BaseQuery#getQuerySegmentWalker}, and {@code BrokerQueryResource}) —
 * so this validation is resolved outside {@code ExecutionVertex}.
 *
 * <p>Right-hand inputs of joins are intentionally not descended: a
 * {@code __time} filter there does not bound the base table (see
 * {@code CalciteQueryTest#testRequireTimeConditionSemiJoinNegative}).
 */
public final class RequireTimeConditionAnalyzer
{
  private RequireTimeConditionAnalyzer()
  {
  }

  /**
   * @return true if every physical leaf datasource in {@code query} is
   *         descended from an ancestor query with a bounded interval. Global
   *         datasources (lookups, inline) always satisfy the check.
   */
  public static boolean hasTimeFilterOnAllLegs(Query<?> query)
  {
    TimeFilterExplorer explorer = new TimeFilterExplorer(query);
    return explorer.allLegsBounded;
  }

  private static class TimeFilterExplorer extends ExecutionVertexShuttle
  {
    boolean allLegsBounded = true;

    TimeFilterExplorer(Query<?> query)
    {
      traverse(query);
    }

    @Override
    protected boolean mayTraverseQuery(Query<?> query)
    {
      return true;
    }

    @Override
    protected boolean mayTraverseDataSource(EVNode node)
    {
      return true;
    }

    @Override
    protected Query<?> visitQuery(Query<?> query)
    {
      return query;
    }

    @Override
    protected DataSource visit(DataSource dataSource, boolean leaf)
    {
      if (!isPhysicalLeaf(dataSource) || dataSource.isGlobal()) {
        return dataSource;
      }
      boolean crossedJoinLegBoundary = false;
      boolean collapsedChain = true;
      int queriesSeen = 0;
      for (int i = parents.size() - 1; i >= 0; i--) {
        EVNode ancestor = parents.get(i);
        if (!ancestor.isQuery() && ancestor.index != null && ancestor.index != 0 && i > 0) {
          EVNode parentOfAncestor = parents.get(i - 1);
          if (!parentOfAncestor.isQuery() && parentOfAncestor.dataSource instanceof JoinDataSource) {
            crossedJoinLegBoundary = true;
          }
        }
        if (ancestor.isQuery()) {
          Query<?> q = ancestor.getQuery();
          if (queriesSeen > 0) {
            // To trust q's bound, every Query between the leaf and q must have been
            // absorbed into q (or further up). Each hop needs the outer Query to
            // mayCollapseQueryDataSource() its inner QueryDataSource; otherwise the
            // inner query runs independently and the outer's bound does not reach the leaf.
            collapsedChain &= q.mayCollapseQueryDataSource();
          }
          if (!crossedJoinLegBoundary && collapsedChain && isBounded(q)) {
            return dataSource;
          }
          queriesSeen++;
        }
      }
      allLegsBounded = false;
      return dataSource;
    }

    private static boolean isPhysicalLeaf(DataSource dataSource)
    {
      return !(dataSource instanceof QueryDataSource) && dataSource.getChildren().isEmpty();
    }

    private static boolean isBounded(Query<?> query)
    {
      if (!(query instanceof BaseQuery)) {
        return false;
      }
      QuerySegmentSpec spec = ((BaseQuery<?>) query).getQuerySegmentSpec();
      return spec != null && !Intervals.ONLY_ETERNITY.equals(spec.getIntervals());
    }
  }
}
