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

package org.apache.druid.query.groupby.strategy;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.segment.StorageAdapter;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.function.BinaryOperator;

public interface GroupByStrategy
{
  /**
   * Initializes resources required to run {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} for a
   * particular query. That method is also the primary caller of this method.
   *
   * Used by {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)}.
   *
   * @param query a groupBy query to be processed
   *
   * @return broker resource
   */
  GroupByQueryResource prepareResource(GroupByQuery query);

  /**
   * Indicates if results from this query are cacheable or not.
   *
   * Used by {@link GroupByQueryQueryToolChest#getCacheStrategy(GroupByQuery)}.
   *
   * @param willMergeRunners indicates that {@link QueryRunnerFactory#mergeRunners(ExecutorService, Iterable)} will be
   *                         called on the cached by-segment results. Can be used to distinguish if we are running on
   *                         a broker or data node.
   *
   * @return true if this strategy is cacheable, otherwise false.
   */
  boolean isCacheable(boolean willMergeRunners);

  /**
   * Indicates if this query should undergo "mergeResults" or not. Checked by
   * {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)}.
   */
  boolean doMergeResults(GroupByQuery query);

  /**
   * Runs a provided {@link QueryRunner} on a provided {@link GroupByQuery}, which is assumed to return rows that are
   * properly sorted (by timestamp and dimensions) but not necessarily fully merged (that is, there may be adjacent
   * rows with the same timestamp and dimensions) and without PostAggregators computed. This method will fully merge
   * the rows, apply PostAggregators, and return the resulting {@link Sequence}.
   *
   * The query will be modified before passing it down to the base runner. For example, "having" clauses will be
   * removed and various context parameters will be adjusted.
   *
   * Despite the similar name, this method is much reduced in scope compared to
   * {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)}. That method does delegate to this one at some points,
   * but has a truckload of other responsibility, including computing outer query results (if there are subqueries),
   * computing subtotals (like GROUPING SETS), and computing the havingSpec and limitSpec.
   *
   * @param baseRunner      base query runner
   * @param query           the groupBy query to run inside the base query runner
   * @param responseContext the response context to pass to the base query runner
   *
   * @return merged result sequence
   */
  Sequence<ResultRow> mergeResults(
      QueryRunner<ResultRow> baseRunner,
      GroupByQuery query,
      ResponseContext responseContext
  );

  /**
   * See {@link org.apache.druid.query.QueryToolChest#createMergeFn(Query)} for details, allows
   * {@link GroupByQueryQueryToolChest} to delegate implementation to the strategy
   */
  @Nullable
  default BinaryOperator<ResultRow> createMergeFn(Query<ResultRow> query)
  {
    return null;
  }

  /**
   * See {@link org.apache.druid.query.QueryToolChest#createResultComparator(Query)}, allows
   * {@link GroupByQueryQueryToolChest} to delegate implementation to the strategy
   */
  @Nullable
  default Comparator<ResultRow> createResultComparator(Query<ResultRow> queryParam)
  {
    throw new UOE("%s doesn't provide a result comparator", this.getClass().getName());
  }

  /**
   * Apply the {@link GroupByQuery} "postProcessingFn", which is responsible for HavingSpec and LimitSpec.
   *
   * @param results sequence of results
   * @param query   the groupBy query
   *
   * @return post-processed results, with HavingSpec and LimitSpec applied
   */
  Sequence<ResultRow> applyPostProcessing(Sequence<ResultRow> results, GroupByQuery query);

  /**
   * Called by {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} when it needs to process a subquery.
   *
   * @param subquery           inner query
   * @param query              outer query
   * @param resource           resources returned by {@link #prepareResource(GroupByQuery)}
   * @param subqueryResult     result rows from the subquery
   * @param wasQueryPushedDown true if the outer query was pushed down (so we only need to merge the outer query's
   *                           results, not run it from scratch like a normal outer query)
   *
   * @return results of the outer query
   */
  Sequence<ResultRow> processSubqueryResult(
      GroupByQuery subquery,
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<ResultRow> subqueryResult,
      boolean wasQueryPushedDown
  );

  /**
   * Called by {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} when it needs to generate subtotals.
   *
   * @param query       query that has a "subtotalsSpec"
   * @param resource    resources returned by {@link #prepareResource(GroupByQuery)}
   * @param queryResult result rows from the main query
   *
   * @return results for each list of subtotals in the query, concatenated together
   */
  Sequence<ResultRow> processSubtotalsSpec(
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<ResultRow> queryResult
  );

  /**
   * Merge a variety of single-segment query runners into a combined runner. Used by
   * {@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory#mergeRunners(ExecutorService, Iterable)}. In
   * that sense, it is intended to go along with {@link #process(GroupByQuery, StorageAdapter)} (the runners created
   * by that method will be fed into this method).
   *
   * This method is only called on data servers, like Historicals (not the Broker).
   *
   * @param exec         executor service used for parallel execution of the query runners
   * @param queryRunners collection of query runners to merge
   *
   * @return merged query runner
   */
  QueryRunner<ResultRow> mergeRunners(ListeningExecutorService exec, Iterable<QueryRunner<ResultRow>> queryRunners);

  /**
   * Process a groupBy query on a single {@link StorageAdapter}. This is used by
   * {@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory#createRunner} to create per-segment
   * QueryRunners.
   *
   * This method is only called on data servers, like Historicals (not the Broker).
   *
   * @param query          the groupBy query
   * @param storageAdapter storage adatper for the segment in question
   *
   * @return result sequence for the storage adapter
   */
  Sequence<ResultRow> process(GroupByQuery query, StorageAdapter storageAdapter);

  /**
   * Returns whether this strategy supports pushing down outer queries. This is used by
   * {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} when it decides whether or not to push down an
   * outer query from the Broker to data servers, like Historicals.
   *
   * Can be removed when the "v1" groupBy strategy is removed. ("v1" returns false, and "v2" returns true.)
   */
  boolean supportsNestedQueryPushDown();
}
