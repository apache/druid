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
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.segment.StorageAdapter;

import java.util.concurrent.ExecutorService;

public interface GroupByStrategy
{
  /**
   * Initializes resources required for a broker to process the given query.
   *
   * @param query a groupBy query to be processed
   * @return broker resource
   */
  GroupByQueryResource prepareResource(GroupByQuery query, boolean willMergeRunners);

  /**
   * Indicates this strategy is cacheable or not.
   * The {@code willMergeRunners} parameter can be used for distinguishing the caller is a broker or a data node.
   *
   * @param willMergeRunners indicates that {@link QueryRunnerFactory#mergeRunners(ExecutorService, Iterable)} will be
   *                         called on the cached by-segment results
   * @return true if this strategy is cacheable, otherwise false.
   */
  boolean isCacheable(boolean willMergeRunners);

  /**
   * Indicates if this query should undergo "mergeResults" or not.
   */
  boolean doMergeResults(GroupByQuery query);

  /**
   * Decorate a runner with an interval chunking decorator.
   */
  QueryRunner<Row> createIntervalChunkingRunner(
      IntervalChunkingQueryRunnerDecorator decorator,
      QueryRunner<Row> runner,
      GroupByQueryQueryToolChest toolChest
  );

  Sequence<Row> mergeResults(QueryRunner<Row> baseRunner, GroupByQuery query, ResponseContext responseContext);

  Sequence<Row> applyPostProcessing(Sequence<Row> results, GroupByQuery query);

  Sequence<Row> processSubqueryResult(
      GroupByQuery subquery,
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<Row> subqueryResult,
      boolean wasQueryPushedDown
  );

  Sequence<Row> processSubtotalsSpec(GroupByQuery query, GroupByQueryResource resource, Sequence<Row> queryResult);

  QueryRunner<Row> mergeRunners(ListeningExecutorService exec, Iterable<QueryRunner<Row>> queryRunners);

  Sequence<Row> process(GroupByQuery query, StorageAdapter storageAdapter);

  boolean supportsNestedQueryPushDown();
}
