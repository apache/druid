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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.guice.annotations.ExtensionPoint;

/**
 * This class implements the logic of how units of query execution run concurrently. It is used in {@link QueryRunnerFactory#mergeRunners(QueryProcessingPool, Iterable)}.
 * In a most straightforward implementation, each unit will be submitted to an {@link PrioritizedExecutorService}. Extensions,
 * however, can implement their own logic for picking which unit to pick first for execution.
 * <p>
 * This interface extends {@link ListeningExecutorService} as well. It has a separate
 * method to submit query execution tasks so that implementations can differentiate those tasks from any regular async
 * tasks. One example is {@link org.apache.druid.query.groupby.strategy.GroupByStrategyV2#mergeRunners(QueryProcessingPool, Iterable)}
 * where different kind of tasks are submitted to same processing pool.
 * <p>
 * Query execution task also includes a reference to {@link QueryRunner} so that any state required to decide the priority
 * of a unit can be carried forward with the corresponding {@link QueryRunner}.
 */
@ExtensionPoint
public interface QueryProcessingPool extends ListeningExecutorService
{
  /**
   * Submits the query execution unit task for asynchronous execution.
   *
   * @param task - Task to be submitted.
   * @param <T>  - Task result type
   * @param <V>  - Query runner sequence type
   * @return - Future object for tracking the task completion.
   */
  <T, V> ListenableFuture<T> submitRunnerTask(PrioritizedQueryRunnerCallable<T, V> task);
}
