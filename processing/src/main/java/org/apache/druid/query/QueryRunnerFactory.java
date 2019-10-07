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

package org.apache.druid.query;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.Segment;

import java.util.concurrent.ExecutorService;

/**
 * An interface that defines the nitty gritty implementation details of a Query on a Segment
 */
@ExtensionPoint
public interface QueryRunnerFactory<T, QueryType extends Query<T>>
{
  /**
   * Given a specific segment, this method will create a {@link QueryRunner}.
   *
   * The {@link QueryRunner}, when asked, will generate a {@link org.apache.druid.java.util.common.guava.Sequence} of
   * results based on the given segment.  This is the meat of the {@link Query} processing and is where the results are
   * actually generated.  Everything else is just merging and reduction logic.
   *
   * @param   segment The segment to process
   * @return  A {@link QueryRunner} that, when asked, will generate a
   *          {@link org.apache.druid.java.util.common.guava.Sequence} of results based on the given segment
   */
  QueryRunner<T> createRunner(Segment segment);

  /**
   * Runners generated with createRunner() and combined into an Iterable in (time,shardId) order are passed
   * along to this method with an {@link ExecutorService}.  The method should then return a {@link QueryRunner} that,
   * when asked, will use the {@link ExecutorService} to run the base QueryRunners in some fashion.
   *
   * The vast majority of the time, this should be implemented with {@link ChainedExecutionQueryRunner}:
   *
   *     return new ChainedExecutionQueryRunner<>(queryExecutor, toolChest.getOrdering(), queryWatcher, queryRunners);
   *
   * Which will allow for parallel execution up to the maximum number of processing threads allowed.
   *
   * @param queryExecutor   {@link ExecutorService} to be used for parallel processing
   * @param queryRunners    Individual {@link QueryRunner} objects that produce some results
   * @return                a {@link QueryRunner} that, when asked, will use the {@link ExecutorService} to run the base
   *                        {@link QueryRunner} collection.
   */
  QueryRunner<T> mergeRunners(ExecutorService queryExecutor, Iterable<QueryRunner<T>> queryRunners);

  /**
   * Provides access to the {@link QueryToolChest} for this specific {@link Query} type.
   *
   * @return an instance of the toolchest for this specific query type.
   */
  QueryToolChest<T, QueryType> getToolchest();
}
