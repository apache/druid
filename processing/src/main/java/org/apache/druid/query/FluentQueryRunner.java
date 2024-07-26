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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.context.ResponseContext;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class FluentQueryRunner<T> implements QueryRunner<T>
{
  @SuppressWarnings("unchecked")
  public static <K, J extends Query<K>> FluentQueryRunner<K> create(
      QueryRunner<K> runner,
      QueryToolChest<K, J> toolchest
  )
  {
    return new FluentQueryRunner<>(runner, (QueryToolChest<K, Query<K>>) toolchest);
  }

  private final QueryToolChest<T, Query<T>> toolChest;
  private final QueryRunner<T> baseRunner;

  public FluentQueryRunner(QueryRunner<T> runner, QueryToolChest<T, Query<T>> toolChest)
  {
    this.baseRunner = runner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    return baseRunner.run(queryPlus, responseContext);
  }

  public FluentQueryRunner<T> from(QueryRunner<T> runner)
  {
    return new FluentQueryRunner<T>(runner, toolChest);
  }

  public FluentQueryRunner<T> applyPostMergeDecoration()
  {
    return from(new FinalizeResultsQueryRunner<>(toolChest.postMergeQueryDecoration(baseRunner), toolChest));
  }

  public FluentQueryRunner<T> applyPreMergeDecoration()
  {
    return from(new UnionQueryRunner<>(toolChest.preMergeQueryDecoration(baseRunner)));
  }

  public FluentQueryRunner<T> emitCPUTimeMetric(ServiceEmitter emitter)
  {
    return emitCPUTimeMetric(emitter, new AtomicLong(0L));
  }

  public FluentQueryRunner<T> emitCPUTimeMetric(ServiceEmitter emitter, AtomicLong accumulator)
  {
    return from(
        CPUTimeMetricQueryRunner.safeBuild(
            baseRunner,
            toolChest,
            emitter,
            accumulator,
            true
        )
    );
  }

  public FluentQueryRunner<T> postProcess(PostProcessingOperator<T> postProcessing)
  {
    return from(postProcessing != null ? postProcessing.postProcess(baseRunner) : baseRunner);
  }

  /**
   * Delegates to {@link QueryToolChest#mergeResults(QueryRunner, boolean)}.
   * 
   * @see QueryToolChest#mergeResults(QueryRunner, boolean)
   */
  public FluentQueryRunner<T> mergeResults(boolean willMergeRunner)
  {
    return from(toolChest.mergeResults(baseRunner, willMergeRunner));
  }

  public FluentQueryRunner<T> map(final Function<QueryRunner<T>, QueryRunner<T>> mapFn)
  {
    return from(mapFn.apply(baseRunner));
  }

  /**
   * Sets the toString of the QueryRunner.  This is used because QueryRunner objects are often used as parameters for
   * tests and the toString() value of the QueryRunners are used for the name of the test.
   *
   * This method doesn't return a FluentQueryRunner because it breaks the fluency.
   *
   * @param toStringValue the value that the resulting QueryRunner should return from its toString method.
   * @return a QueryRunner that will return toStringValue from its toString method
   */
  public QueryRunner<T> setToString(String toStringValue)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
      {
        return FluentQueryRunner.this.run(queryPlus, responseContext);
      }

      @Override
      public String toString()
      {
        return toStringValue;
      }
    };
  }
}
