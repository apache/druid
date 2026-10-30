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
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;

import javax.annotation.Nullable;

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
 * <p>Right-hand inputs of joins are descended but treated specially: the
 * outer query's bound does not propagate across a right-leg boundary at
 * runtime (the outer interval only applies to its primary/left input), so a
 * right-leg physical leaf whose own innermost query ancestor is unbounded is
 * rejected with {@link Position#JOIN_RIGHT}. A right-leg
 * {@link org.apache.druid.query.GlobalTableDataSource}, which is materialized
 * via {@code JoinableFactory} as an {@code IndexedTable} rather than scanned
 * as segments, is exempt. See
 * {@code CalciteQueryTest#testRequireTimeConditionSemiJoinNegative}.
 *
 * <p>On rejection the analyzer records the first offending physical leaf's
 * datasource name and its structural position so callers can build a specific
 * error message.
 */
public final class RequireTimeConditionAnalyzer
{
  public enum Position
  {
    TOP_LEVEL,
    NESTED_SUBQUERY,
    JOIN_RIGHT
  }

  public static final class Result
  {
    private final boolean satisfied;
    @Nullable
    private final String leafName;
    @Nullable
    private final Position position;

    private Result(boolean satisfied, @Nullable String leafName, @Nullable Position position)
    {
      this.satisfied = satisfied;
      this.leafName = leafName;
      this.position = position;
    }

    public boolean isSatisfied()
    {
      return satisfied;
    }

    @Nullable
    public String getLeafName()
    {
      return leafName;
    }

    @Nullable
    public Position getPosition()
    {
      return position;
    }
  }

  private RequireTimeConditionAnalyzer()
  {
  }

  /**
   * @return a {@link Result} describing whether every physical leaf datasource
   *         in {@code query} is descended from an ancestor query with a bounded
   *         interval. If not, the result also carries the first offending
   *         leaf's name and its structural position. Global datasources
   *         (lookups, inline) always satisfy the check.
   */
  public static Result analyze(Query<?> query)
  {
    final TimeFilterExplorer explorer = new TimeFilterExplorer(query);
    if (explorer.allLegsBounded) {
      return new Result(true, null, null);
    }
    return new Result(false, explorer.firstOffenderName, explorer.firstOffenderPosition);
  }

  /**
   * Boolean-only convenience over {@link #analyze(Query)} for callers that do
   * not need the offending-leaf details.
   */
  public static boolean hasTimeFilterOnAllLegs(Query<?> query)
  {
    return analyze(query).isSatisfied();
  }

  private static class TimeFilterExplorer extends ExecutionVertexShuttle
  {
    boolean allLegsBounded = true;
    @Nullable
    String firstOffenderName;
    @Nullable
    Position firstOffenderPosition;

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
      if (!isPhysicalLeaf(dataSource)) {
        return dataSource;
      }
      // Lookups, inline, and frame-based inline datasources have no segments to scan,
      // so they always satisfy the guard. GlobalTableDataSource is still segment-backed
      // when queried directly or on a join's left leg (see GlobalTableDataSource javadoc:
      // "when queried directly, or on the left hand side of a join, they will be treated
      // as any normal table datasource"), so it is NOT exempted here.
      if (!(dataSource instanceof TableDataSource) && dataSource.isGlobal()) {
        return dataSource;
      }
      // Only the innermost Query ancestor's segment spec reaches a physical leaf at
      // runtime: an outer non-collapsible QueryDataSource executes its inner query with
      // the inner's original intervals, and even for collapsible pairs (e.g. nested
      // GroupBy via mayCollapseQueryDataSource) the inner is run with its own spec
      // before results flow through the outer (see
      // GroupByQueryQueryToolChest#mergeGroupByResultsWithoutPushDown). An outer bound
      // therefore does not propagate to the physical scan. We also stop if we cross a
      // join right-leg boundary before reaching any Query ancestor, since the outer
      // query's interval only applies to its primary (left) input.
      for (int i = parents.size() - 1; i >= 0; i--) {
        final EVNode ancestor = parents.get(i);
        if (ancestor.isQuery()) {
          if (isBounded(ancestor.getQuery())) {
            return dataSource;
          }
          recordRejection(dataSource, queryAncestorPosition());
          allLegsBounded = false;
          return dataSource;
        }
        if (ancestor.index != null && ancestor.index != 0 && i > 0) {
          final EVNode parentOfAncestor = parents.get(i - 1);
          if (!parentOfAncestor.isQuery() && parentOfAncestor.dataSource instanceof JoinDataSource) {
            // Right-leg broadcast join: a GlobalTableDataSource is materialized as an
            // IndexedTable via JoinableFactory rather than scanned as segments, so the
            // right-side join exemption still applies here even though it is a TableDataSource.
            if (dataSource.isGlobal()) {
              return dataSource;
            }
            recordRejection(dataSource, Position.JOIN_RIGHT);
            allLegsBounded = false;
            return dataSource;
          }
        }
      }
      recordRejection(dataSource, queryAncestorPosition());
      allLegsBounded = false;
      return dataSource;
    }

    private void recordRejection(DataSource dataSource, Position position)
    {
      if (firstOffenderName != null) {
        return;
      }
      firstOffenderName = describeLeaf(dataSource);
      firstOffenderPosition = position;
    }

    private Position queryAncestorPosition()
    {
      int queryCount = 0;
      for (EVNode a : parents) {
        if (a.isQuery()) {
          queryCount++;
        }
      }
      return queryCount > 1 ? Position.NESTED_SUBQUERY : Position.TOP_LEVEL;
    }

    private static String describeLeaf(DataSource dataSource)
    {
      if (dataSource instanceof TableDataSource) {
        return ((TableDataSource) dataSource).getName();
      }
      return dataSource.toString();
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
      final QuerySegmentSpec spec = ((BaseQuery<?>) query).getQuerySegmentSpec();
      return spec != null && !Intervals.ONLY_ETERNITY.equals(spec.getIntervals());
    }
  }
}
