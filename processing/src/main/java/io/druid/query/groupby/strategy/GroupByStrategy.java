/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby.strategy;

import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.resource.GroupByQueryResource;
import io.druid.segment.StorageAdapter;

import java.util.Map;
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

  Sequence<Row> mergeResults(
      QueryRunner<Row> baseRunner,
      GroupByQuery query,
      Map<String, Object> responseContext
  );

  Sequence<Row> processSubqueryResult(
      GroupByQuery subquery,
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<Row> subqueryResult
  );

  QueryRunner<Row> mergeRunners(
      ListeningExecutorService exec,
      Iterable<QueryRunner<Row>> queryRunners
  );

  Sequence<Row> process(
      GroupByQuery query,
      StorageAdapter storageAdapter
  );
}
